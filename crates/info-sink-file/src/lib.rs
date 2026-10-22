//! File INFO sink plugin: writes every INFO payload as one JSONL line into
//! NEW files that are written once, sequentially, and closed.
//!
//! Built for stage volumes that reject appends, random writes and renames:
//!
//! * a file is opened with `create_new` (an existing name is an error, never
//!   overwritten or appended to), written front to back, and closed when it is
//!   sealed — no temp file, no rename;
//! * files are named `<prefix><seq06>.jsonl` (`seq` from 1, zero-padded to six
//!   digits) in `dir`. At create the directory is scanned and numbering
//!   continues after the highest existing `<prefix>NNNNNN.jsonl`, so a
//!   restarted process never collides with its previous files. After file
//!   999999 the number simply grows to 7 digits (`<prefix>1000000.jsonl`),
//!   where LEXICAL order no longer matches sequence order: readers must sort
//!   by the parsed number (as [`list_files`] does), never by name;
//! * a file is sealed after `rotate_records` lines, when a write finds it
//!   older than `rotate_secs`, on `sz_is_flush` (the consumer's timer) and on
//!   `sz_is_close` (final seal). A file is only created by a write, so a seal
//!   with nothing written leaves nothing behind;
//! * each line is EXACTLY the INFO JSON the engine returned; a payload holding
//!   a newline would break JSONL and is rejected (fatal) rather than altered.
//!
//! Visibility: a file exists under its FINAL name from its first line on —
//! there is no rename at seal — so a directory listing taken while the
//! consumer runs can show a file that is still being written. A reader must
//! read `dir` only after the consuming job has exited successfully (exit 0,
//! or 3 for a redo drain cap); the files of a failed run are discarded by the
//! caller. A reader that must pick files up while the job runs sets
//! `"seal_marker": true`: after each successful seal the plugin creates an
//! empty `<file>.sealed` next to the file, and only files with a marker are
//! complete. (Markers are written after the seal, so a crash can leave a
//! sealed file without one; never the reverse.)
//!
//! Durability (what a successful call guarantees):
//!
//! * `write` returning Ok means the line was handed to the OS in ONE
//!   `write_all` (no in-process buffer): a SIGKILL / OOM kill after that loses
//!   nothing the consumer has acknowledged. (A host crash before the seal can
//!   still lose it; the seal's fsync covers that.)
//! * a seal is `fsync` (tolerating `EINVAL` / `ENOTSUP` / `EOPNOTSUPP` /
//!   `ENODEV` / unsupported, which volumes without fsync return) followed by an explicit `close(2)` whose error
//!   IS checked (NFS-like volumes report deferred write errors there).
//! * any write, create, fsync or close error marks the sink FAILED: the open
//!   file is dropped WITHOUT a seal (it may end in a partial line) and every
//!   later write, flush and close returns an error. A failed run's files must
//!   be discarded by the caller (or kept only up to the consumer's printed
//!   resume point, see the consumer README).
//!
//! Config (JSON object; unknown keys are rejected):
//! `{"dir":"/work/info","prefix":"info-c0-","rotate_records":10000,"rotate_secs":30,"seal_marker":false}`
//! — `dir` (must exist) and `prefix` are required; `rotate_records` and
//! `rotate_secs` default to [`DEFAULT_ROTATE_RECORDS`] / [`DEFAULT_ROTATE_SECS`]
//! and must be >= 1; `seal_marker` defaults to `false`.
//!
//! Thread-safe: one mutex serializes writers (a blocked writer is the
//! consumer's backpressure). The INFO kind is not recorded in the file.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::IntoRawFd;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sz_info_sink::{InfoKind, InfoSink, export_info_sink};

/// Lines per file when the config does not set `rotate_records`.
pub const DEFAULT_ROTATE_RECORDS: u64 = 10_000;
/// Maximum file age (seconds) when the config does not set `rotate_secs`.
pub const DEFAULT_ROTATE_SECS: u64 = 30;
/// Extension of every output file.
const EXTENSION: &str = ".jsonl";
/// Suffix of the empty marker written after a seal (`seal_marker`).
pub const SEAL_MARKER_SUFFIX: &str = ".sealed";

fn default_rotate_records() -> u64 {
    DEFAULT_ROTATE_RECORDS
}

fn default_rotate_secs() -> u64 {
    DEFAULT_ROTATE_SECS
}

/// The plugin config (see the module docs).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    pub dir: PathBuf,
    pub prefix: String,
    #[serde(default = "default_rotate_records")]
    pub rotate_records: u64,
    #[serde(default = "default_rotate_secs")]
    pub rotate_secs: u64,
    /// Write an empty `<file>.sealed` after each successful seal.
    #[serde(default)]
    pub seal_marker: bool,
}

impl FileSinkConfig {
    /// Parses and validates the config string.
    pub fn parse(config: &str) -> Result<Self, String> {
        let cfg: Self = serde_json::from_str(config)
            .map_err(|e| format!("info-sink-file config is invalid: {e}"))?;
        if cfg.prefix.is_empty() || cfg.prefix.contains(['/', '\\']) {
            return Err(format!(
                "info-sink-file prefix must be a non-empty file-name prefix, got {:?}",
                cfg.prefix
            ));
        }
        if cfg.rotate_records == 0 || cfg.rotate_secs == 0 {
            return Err("info-sink-file rotate_records and rotate_secs must be >= 1".to_string());
        }
        if !cfg.dir.is_dir() {
            return Err(format!(
                "info-sink-file dir {:?} does not exist or is not a directory",
                cfg.dir
            ));
        }
        Ok(cfg)
    }

    /// `<prefix><seq06>.jsonl`.
    pub fn file_name(&self, seq: u64) -> String {
        format!("{}{seq:06}{EXTENSION}", self.prefix)
    }

    /// The sequence number of `name` if it is one of this sink's files.
    fn seq_of(&self, name: &str) -> Option<u64> {
        let digits = name.strip_prefix(&self.prefix)?.strip_suffix(EXTENSION)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    }

    /// First sequence number to use: one past the highest existing file.
    fn first_seq(&self) -> Result<u64, String> {
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| format!("cannot list info-sink-file dir {:?}: {e}", self.dir))?;
        let mut max = 0;
        for entry in entries {
            let entry = entry.map_err(|e| format!("cannot list {:?}: {e}", self.dir))?;
            if let Some(seq) = entry.file_name().to_str().and_then(|n| self.seq_of(n)) {
                max = max.max(seq);
            }
        }
        Ok(max + 1)
    }
}

/// The file currently being written (unbuffered: see the module docs).
struct OpenFile {
    file: File,
    path: PathBuf,
    records: u64,
    opened: Instant,
}

impl OpenFile {
    /// Appends `info` + `\n` with ONE `write_all` (no in-process buffering).
    fn append(&mut self, info: &str) -> Result<(), String> {
        let mut line = Vec::with_capacity(info.len() + 1);
        line.extend_from_slice(info.as_bytes());
        line.push(b'\n');
        self.file
            .write_all(&line)
            .map_err(|e| format!("cannot write {:?}: {e}", self.path))?;
        self.records += 1;
        Ok(())
    }

    /// fsync (where supported), an error-checked close and, with `marker`,
    /// the empty `<file>.sealed` marker.
    fn seal(self, marker: bool) -> Result<(), String> {
        let Self { file, path, .. } = self;
        let synced = match file.sync_all() {
            Err(e) if !fsync_unsupported(&e) => Err(format!("cannot fsync {path:?}: {e}")),
            _ => Ok(()),
        };
        // Closed explicitly (and checked) even after an fsync error; the fsync
        // error is the one reported.
        let closed = close_checked(file).map_err(|e| format!("cannot close {path:?}: {e}"));
        synced.and(closed)?;
        if marker {
            write_marker(&path)?;
        }
        Ok(())
    }
}

/// Creates the empty `<path>.sealed` marker (new file, checked close).
fn write_marker(path: &std::path::Path) -> Result<(), String> {
    let mut name = path.as_os_str().to_owned();
    name.push(SEAL_MARKER_SUFFIX);
    let marker = PathBuf::from(name);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .and_then(close_checked)
        .map_err(|e| format!("cannot create seal marker {marker:?}: {e}"))
}

/// `true` for the fsync errors a volume returns when it simply does not
/// support fsync (the seal then relies on close alone): EINVAL (Linux, e.g.
/// special files), ENOTSUP / EOPNOTSUPP, and ENODEV ("Operation not supported
/// by device", macOS `F_FULLFSYNC` on devices and volumes without it).
fn fsync_unsupported(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::Unsupported
        || e.raw_os_error().is_some_and(|c| {
            [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENODEV].contains(&c)
        })
}

/// close(2) with its error reported (dropping a `File` ignores it).
fn close_checked(file: File) -> std::io::Result<()> {
    let fd = file.into_raw_fd();
    // SAFETY: `fd` is owned (taken from the File) and closed exactly once.
    match unsafe { libc::close(fd) } {
        0 => Ok(()),
        _ => Err(std::io::Error::last_os_error()),
    }
}

struct State {
    next_seq: u64,
    current: Option<OpenFile>,
    closed: bool,
    /// Set by the first I/O error; the sink refuses everything afterwards.
    failed: Option<String>,
}

impl State {
    /// Errors when the sink can no longer be used.
    fn usable(&self, closed_is_error: bool) -> Result<(), String> {
        match (&self.failed, self.closed && closed_is_error) {
            (Some(e), _) => Err(format!("info-sink-file FAILED earlier: {e}")),
            (None, true) => Err("info-sink-file is closed".to_string()),
            (None, false) => Ok(()),
        }
    }

    /// Records the first I/O error and discards the open file WITHOUT a seal.
    fn fail(&mut self, e: String) -> String {
        self.current = None;
        self.failed.get_or_insert_with(|| e.clone());
        e
    }

    /// Seals the open file, if any; a seal error fails the sink.
    fn seal(&mut self, marker: bool) -> Result<(), String> {
        match self.current.take().map_or(Ok(()), |f| f.seal(marker)) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.fail(e)),
        }
    }
}

/// The file sink (see the module docs).
pub struct FileSink {
    cfg: FileSinkConfig,
    rotate_age: Duration,
    state: Mutex<State>,
}

impl FileSink {
    /// Plugin constructor: parses the config and finds the first free number.
    pub fn new(config: &str) -> Result<Self, String> {
        let cfg = FileSinkConfig::parse(config)?;
        let next_seq = cfg.first_seq()?;
        Ok(Self {
            rotate_age: Duration::from_secs(cfg.rotate_secs),
            cfg,
            state: Mutex::new(State {
                next_seq,
                current: None,
                closed: false,
                failed: None,
            }),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, String> {
        self.state
            .lock()
            .map_err(|_| "info-sink-file state poisoned by an earlier panic".to_string())
    }

    fn open_next(&self, state: &mut State) -> Result<OpenFile, String> {
        let path = self.cfg.dir.join(self.cfg.file_name(state.next_seq));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| format!("cannot create {path:?}: {e}"))?;
        state.next_seq += 1;
        Ok(OpenFile {
            file,
            path,
            records: 0,
            opened: Instant::now(),
        })
    }

    /// The write path under the lock: age rotation, lazy create, one append,
    /// count rotation.
    fn write_locked(&self, state: &mut State, info: &str) -> Result<(), String> {
        if state
            .current
            .as_ref()
            .is_some_and(|f| f.opened.elapsed() >= self.rotate_age)
        {
            state.seal(self.cfg.seal_marker)?;
        }
        let mut file = match state.current.take() {
            Some(f) => f,
            None => self.open_next(state)?,
        };
        file.append(info)?;
        let full = file.records >= self.cfg.rotate_records;
        state.current = Some(file);
        if full {
            state.seal(self.cfg.seal_marker)?;
        }
        Ok(())
    }
}

/// Every file of this sink in `dir`, in sequence order.
pub fn list_files(cfg: &FileSinkConfig) -> Result<Vec<PathBuf>, String> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(&cfg.dir)
        .map_err(|e| format!("cannot list {:?}: {e}", cfg.dir))?
        .filter_map(Result::ok)
        .filter_map(|e| {
            let seq = e.file_name().to_str().and_then(|n| cfg.seq_of(n))?;
            Some((seq, e.path()))
        })
        .collect();
    found.sort();
    Ok(found.into_iter().map(|(_, p)| p).collect())
}

impl InfoSink for FileSink {
    fn write(&self, _kind: InfoKind, info: &str) -> Result<(), String> {
        if info.contains(['\n', '\r']) {
            return Err(
                "INFO payload contains a line break; refusing to split a JSONL line".into(),
            );
        }
        let mut state = self.lock()?;
        state.usable(true)?;
        self.write_locked(&mut state, info)
            .map_err(|e| state.fail(e))
    }

    fn flush(&self) -> Result<(), String> {
        let mut state = self.lock()?;
        state.usable(true)?;
        state.seal(self.cfg.seal_marker)
    }

    /// Final seal; idempotent once it succeeded, an error forever after a
    /// failure.
    fn close(&self) -> Result<(), String> {
        let mut state = self.lock()?;
        state.usable(false)?;
        state.closed = true;
        state.seal(self.cfg.seal_marker)
    }
}

export_info_sink!(FileSink, FileSink::new);

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    /// A fresh, empty directory under the system temp dir.
    fn temp_dir(tag: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("sz_info_sink_file_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("mkdir");
        d
    }

    fn config(dir: &Path, rotate_records: u64, rotate_secs: u64) -> String {
        serde_json::json!({
            "dir": dir,
            "prefix": "info-c0-",
            "rotate_records": rotate_records,
            "rotate_secs": rotate_secs,
        })
        .to_string()
    }

    fn marker_config(dir: &Path, rotate_records: u64) -> String {
        serde_json::json!({
            "dir": dir,
            "prefix": "info-c0-",
            "rotate_records": rotate_records,
            "rotate_secs": 3600,
            "seal_marker": true,
        })
        .to_string()
    }

    /// Every file name in `dir`, sorted.
    fn dir_names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    /// M4: `seal_marker` adds `<file>.sealed` after each seal (never for the
    /// file still open), is off by default, and markers neither count as
    /// sink files nor disturb the numbering on restart.
    #[test]
    fn seal_marker_marks_each_sealed_file_only() {
        let dir = temp_dir("marker");
        let sink = FileSink::new(&marker_config(&dir, 2)).unwrap();
        for i in 0..3 {
            sink.write(InfoKind::Add, &format!(r#"{{"I":{i}}}"#))
                .unwrap();
        }
        assert_eq!(
            dir_names(&dir),
            [
                "info-c0-000001.jsonl",
                "info-c0-000001.jsonl.sealed",
                "info-c0-000002.jsonl"
            ],
            "the open file has no marker yet"
        );
        sink.flush().unwrap();
        assert!(dir.join("info-c0-000002.jsonl.sealed").exists());
        assert_eq!(
            std::fs::metadata(dir.join("info-c0-000002.jsonl.sealed"))
                .unwrap()
                .len(),
            0
        );
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&marker_config(&dir, 2)).unwrap();
        assert!(cfg.seal_marker);
        assert_eq!(list_files(&cfg).unwrap().len(), 2, "markers are not files");
        // Restart: numbering continues after the highest data file.
        let sink = FileSink::new(&marker_config(&dir, 2)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        sink.close().unwrap();
        assert!(dir.join("info-c0-000003.jsonl.sealed").exists());
        // Default off.
        let plain = temp_dir("nomarker");
        let sink = FileSink::new(&config(&plain, 1, 3600)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        sink.close().unwrap();
        assert_eq!(dir_names(&plain), ["info-c0-000001.jsonl"]);
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(plain);
    }

    /// A marker that cannot be created fails the sink.
    #[test]
    fn a_marker_name_taken_by_a_directory_fails_the_seal() {
        let dir = temp_dir("marker_taken");
        let sink = FileSink::new(&marker_config(&dir, 1000)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        std::fs::create_dir(dir.join("info-c0-000001.jsonl.sealed")).unwrap();
        let e = sink.flush().unwrap_err();
        assert!(e.contains("cannot create seal marker"), "{e}");
        assert!(sink.close().unwrap_err().contains("FAILED"));
        let _ = std::fs::remove_dir_all(dir);
    }

    fn read_lines(paths: &[PathBuf]) -> Vec<Vec<String>> {
        paths
            .iter()
            .map(|p| {
                std::fs::read_to_string(p)
                    .unwrap()
                    .lines()
                    .map(str::to_string)
                    .collect()
            })
            .collect()
    }

    fn names(paths: &[PathBuf]) -> Vec<String> {
        paths
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn config_requires_dir_and_prefix_and_validates_values() {
        let dir = temp_dir("cfg");
        let cfg = FileSinkConfig::parse(&format!(r#"{{"dir":{:?},"prefix":"p-"}}"#, dir)).unwrap();
        assert_eq!(
            (cfg.rotate_records, cfg.rotate_secs),
            (DEFAULT_ROTATE_RECORDS, DEFAULT_ROTATE_SECS)
        );
        assert_eq!(cfg.file_name(7), "p-000007.jsonl");
        for bad in [
            r#"{"prefix":"p-"}"#.to_string(),
            format!(r#"{{"dir":{dir:?}}}"#),
            format!(r#"{{"dir":{dir:?},"prefix":""}}"#),
            format!(r#"{{"dir":{dir:?},"prefix":"a/b"}}"#),
            format!(r#"{{"dir":{dir:?},"prefix":"p","rotate_records":0}}"#),
            format!(r#"{{"dir":{dir:?},"prefix":"p","rotate_secs":0}}"#),
            format!(r#"{{"dir":{dir:?},"prefix":"p","typo":1}}"#),
            r#"{"dir":"/nonexistent-sz-info-dir","prefix":"p"}"#.to_string(),
            "not json".to_string(),
        ] {
            assert!(FileSink::new(&bad).is_err(), "accepted {bad}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// L13: past 999999 the name grows to 7 digits; numbering and listing
    /// follow the parsed number, not the lexical order.
    #[test]
    fn names_grow_past_six_digits_and_order_by_number() {
        let dir = temp_dir("seq7");
        std::fs::write(dir.join("info-c0-999999.jsonl"), "{}\n").unwrap();
        let sink = FileSink::new(&config(&dir, 1, 3600)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&config(&dir, 1, 3600)).unwrap();
        assert_eq!(cfg.file_name(1_000_000), "info-c0-1000000.jsonl");
        assert_eq!(
            names(&list_files(&cfg).unwrap()),
            ["info-c0-999999.jsonl", "info-c0-1000000.jsonl"],
            "sequence order, although '1' sorts before '9' lexically"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rotates_by_record_count_and_seals_on_close() {
        let dir = temp_dir("count");
        let sink = FileSink::new(&config(&dir, 2, 3600)).unwrap();
        for i in 0..5 {
            sink.write(InfoKind::Add, &format!(r#"{{"I":{i}}}"#))
                .unwrap();
        }
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&config(&dir, 2, 3600)).unwrap();
        let files = list_files(&cfg).unwrap();
        assert_eq!(
            names(&files),
            [
                "info-c0-000001.jsonl",
                "info-c0-000002.jsonl",
                "info-c0-000003.jsonl"
            ]
        );
        assert_eq!(
            read_lines(&files),
            vec![
                vec![r#"{"I":0}"#, r#"{"I":1}"#],
                vec![r#"{"I":2}"#, r#"{"I":3}"#],
                vec![r#"{"I":4}"#]
            ]
        );
        // Writes and flushes after close are errors; close is idempotent.
        assert!(sink.write(InfoKind::Redo, "{}").is_err());
        assert!(sink.flush().is_err());
        assert!(sink.close().is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rotates_by_age_at_the_next_write() {
        let dir = temp_dir("age");
        let sink = FileSink::new(&config(&dir, 1000, 1)).unwrap();
        sink.write(InfoKind::Add, "{\"A\":1}").unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        sink.write(InfoKind::Redo, "{\"A\":2}").unwrap();
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&config(&dir, 1000, 1)).unwrap();
        assert_eq!(
            read_lines(&list_files(&cfg).unwrap()),
            vec![vec!["{\"A\":1}"], vec!["{\"A\":2}"]]
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn flush_seals_and_never_leaves_empty_files() {
        let dir = temp_dir("flush");
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        sink.flush().unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        sink.flush().unwrap();
        sink.flush().unwrap();
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&config(&dir, 1000, 3600)).unwrap();
        let files = list_files(&cfg).unwrap();
        assert_eq!(names(&files), ["info-c0-000001.jsonl"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn restart_continues_numbering_and_never_touches_existing_files() {
        let dir = temp_dir("restart");
        std::fs::write(dir.join("info-c0-000004.jsonl"), "old\n").unwrap();
        std::fs::write(dir.join("info-c0-x.jsonl"), "ignored\n").unwrap();
        std::fs::write(dir.join("info-c1-000009.jsonl"), "other prefix\n").unwrap();
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        sink.close().unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("info-c0-000004.jsonl")).unwrap(),
            "old\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("info-c0-000005.jsonl")).unwrap(),
            "{}\n"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_name_taken_after_create_is_an_error_not_an_overwrite() {
        let dir = temp_dir("collide");
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        std::fs::write(dir.join("info-c0-000001.jsonl"), "someone else\n").unwrap();
        let e = sink.write(InfoKind::Add, "{}").unwrap_err();
        assert!(e.contains("cannot create"), "{e}");
        assert_eq!(
            std::fs::read_to_string(dir.join("info-c0-000001.jsonl")).unwrap(),
            "someone else\n"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn line_breaks_in_a_payload_are_rejected() {
        let dir = temp_dir("newline");
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        assert!(sink.write(InfoKind::Add, "{\n}").is_err());
        assert!(sink.write(InfoKind::Add, "{\r}").is_err());
        sink.close().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn concurrent_writers_lose_and_split_nothing() {
        let dir = temp_dir("threads");
        let sink = Arc::new(FileSink::new(&config(&dir, 37, 3600)).unwrap());
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let sink = sink.clone();
                std::thread::spawn(move || {
                    for i in 0..250 {
                        sink.write(InfoKind::Add, &format!(r#"{{"T":{t},"I":{i}}}"#))
                            .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        sink.close().unwrap();
        let cfg = FileSinkConfig::parse(&config(&dir, 37, 3600)).unwrap();
        let files = read_lines(&list_files(&cfg).unwrap());
        assert!(files.iter().all(|f| f.len() <= 37 && !f.is_empty()));
        let mut all: Vec<(u64, u64)> = files
            .iter()
            .flatten()
            .map(|l| {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                (v["T"].as_u64().unwrap(), v["I"].as_u64().unwrap())
            })
            .collect();
        all.sort_unstable();
        let want: Vec<(u64, u64)> = (0..8).flat_map(|t| (0..250).map(move |i| (t, i))).collect();
        assert_eq!(all, want);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// H1: once `write` returns Ok the line is in the kernel (no in-process
    /// buffer that a SIGKILL could lose) — readable through another handle
    /// before any flush or seal.
    #[test]
    fn a_written_line_is_handed_to_the_os_before_write_returns() {
        let dir = temp_dir("unbuffered");
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        sink.write(InfoKind::Add, r#"{"A":1}"#).unwrap();
        sink.write(InfoKind::Redo, r#"{"B":2}"#).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("info-c0-000001.jsonl")).unwrap(),
            "{\"A\":1}\n{\"B\":2}\n",
            "every acknowledged line must already be in the file"
        );
        sink.close().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Env var that turns a re-executed test binary into the child of
    /// [`a_failed_write_marks_the_sink_failed_and_never_seals`].
    const FSIZE_CHILD: &str = "SZ_INFO_SINK_FILE_FSIZE_CHILD";
    const CHILD_OK: &str = "FSIZE_CHILD_OK";

    /// The child half: a REAL write failure (RLIMIT_FSIZE -> EFBIG, SIGXFSZ
    /// ignored) part-way through a line. Runs only in a re-executed process
    /// because the limit is process-wide.
    fn fsize_child(dir: &Path) {
        let mut original = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: plain libc calls with valid arguments.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut original), 0);
            let lim = libc::rlimit {
                rlim_cur: 64,
                ..original
            };
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &lim), 0);
        }
        let sink = FileSink::new(&marker_config(dir, 1000)).unwrap();
        // 20-byte lines: three fit under the 64-byte limit, the fourth is cut.
        let line = r#"{"PAD":"xxxxxxxxx"}"#;
        assert_eq!(line.len() + 1, 20);
        for _ in 0..3 {
            sink.write(InfoKind::Add, line).unwrap();
        }
        let e = sink.write(InfoKind::Add, line).unwrap_err();
        assert!(e.contains("cannot write"), "{e}");
        for e in [
            sink.write(InfoKind::Add, "{}").unwrap_err(),
            sink.flush().unwrap_err(),
            sink.close().unwrap_err(),
            sink.close().unwrap_err(),
        ] {
            assert!(e.contains("FAILED"), "{e}");
        }
        // Lift the limit again (the soft limit may rise back to the hard
        // one) so files written at exit — e.g. a coverage profile — are not
        // cut at 64 bytes.
        // SAFETY: plain libc call with a valid argument.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &original) }, 0);
        println!("{CHILD_OK}");
    }

    /// M6: a write error marks the sink FAILED: every later write, flush and
    /// close fails, and the file holding the cut line is discarded unsealed
    /// (no `.sealed` marker even with `seal_marker` on).
    #[test]
    fn a_failed_write_marks_the_sink_failed_and_never_seals() {
        let dir = temp_dir("fsize");
        if std::env::var_os(FSIZE_CHILD).is_some() {
            return fsize_child(Path::new(&std::env::var(FSIZE_CHILD).unwrap()));
        }
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::a_failed_write_marks_the_sink_failed_and_never_seals",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(FSIZE_CHILD, &dir)
            .output()
            .expect("re-exec test binary");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success() && stdout.contains(CHILD_OK),
            "child failed: {:?}\n{stdout}\n{stderr}",
            out.status
        );
        let written = std::fs::read(dir.join("info-c0-000001.jsonl")).unwrap();
        assert_eq!(written.len(), 64, "three lines + the cut fourth");
        assert!(
            !dir.join("info-c0-000001.jsonl.sealed").exists(),
            "a failed file is never marked sealed"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// M5: the next file's name is taken by a directory -> the create fails
    /// and the sink is FAILED from then on.
    #[test]
    fn a_path_replaced_by_a_directory_fails_the_sink() {
        let dir = temp_dir("dirpath");
        let sink = FileSink::new(&config(&dir, 1, 3600)).unwrap();
        sink.write(InfoKind::Add, "{}").unwrap();
        std::fs::create_dir(dir.join("info-c0-000002.jsonl")).unwrap();
        let e = sink.write(InfoKind::Add, "{}").unwrap_err();
        assert!(e.contains("cannot create"), "{e}");
        let e = sink.flush().unwrap_err();
        assert!(e.contains("FAILED"), "{e}");
        assert!(sink.close().is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// An `OpenFile` around an arbitrary descriptor (seal-path tests).
    fn open_file(file: File, path: &str) -> OpenFile {
        OpenFile {
            file,
            path: PathBuf::from(path),
            records: 0,
            opened: Instant::now(),
        }
    }

    /// A descriptor number no process can have open (far above any
    /// RLIMIT_NOFILE), so fsync / close on it fail for real with EBADF without
    /// touching a descriptor another test thread owns.
    fn bad_fd_file() -> File {
        use std::os::fd::FromRawFd;
        // SAFETY: the number is never a live descriptor; the File is consumed
        // by the code under test, whose close(2) reports EBADF.
        unsafe { File::from_raw_fd(i32::MAX - 1) }
    }

    /// fsync errors other than "unsupported" fail the seal, and a close(2)
    /// error is reported (both via a real EBADF).
    #[test]
    fn seal_reports_fsync_and_close_errors() {
        let e = open_file(bad_fd_file(), "/bad").seal(false).unwrap_err();
        assert!(e.contains("cannot fsync"), "{e}");
        let e = close_checked(bad_fd_file()).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EBADF));
    }

    /// A descriptor whose fsync is "unsupported" (a character device: ENODEV
    /// on macOS, EINVAL on Linux) still seals: the error is tolerated and the
    /// close is checked.
    #[test]
    fn seal_tolerates_unsupported_fsync() {
        let null = OpenOptions::new().write(true).open("/dev/null").unwrap();
        let null_sync = null.try_clone().unwrap().sync_all();
        open_file(null, "/dev/null")
            .seal(false)
            .unwrap_or_else(|e| {
                panic!("/dev/null seal failed (its fsync said {null_sync:?}): {e}")
            });
        assert!(fsync_unsupported(&std::io::Error::from(
            std::io::ErrorKind::Unsupported
        )));
        for code in [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENODEV] {
            assert!(fsync_unsupported(&std::io::Error::from_raw_os_error(code)));
        }
        assert!(!fsync_unsupported(&std::io::Error::from_raw_os_error(
            libc::EIO
        )));
    }

    /// M5: a write to a full device (Linux `/dev/full`, ENOSPC) fails the sink.
    #[test]
    fn a_full_device_fails_the_write() {
        let full = Path::new("/dev/full");
        if !full.exists() {
            eprintln!("SKIP: /dev/full does not exist on this platform (Linux only)");
            return;
        }
        let file = OpenOptions::new().write(true).open(full).unwrap();
        let mut f = open_file(file, "/dev/full");
        let e = f.append("{}").unwrap_err();
        assert!(e.contains("cannot write"), "{e}");
    }

    /// H1 cost measurement (not a pass/fail gate): 100k INFO-sized lines
    /// through the unbuffered sink vs the same bytes through an 8 KiB
    /// `BufWriter` on the same local volume. Run with
    /// `cargo test --release -p sz-info-sink-file -- --ignored --nocapture bench_`.
    #[test]
    #[ignore = "benchmark; run explicitly"]
    fn bench_unbuffered_vs_buffered_100k_lines() {
        const LINES: usize = 100_000;
        let info = format!(
            r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"{}","AFFECTED_ENTITIES":[{{"ENTITY_ID":1}}]}}"#,
            "R".repeat(100)
        );
        let dir = temp_dir("bench");
        let sink = FileSink::new(&config(&dir, LINES as u64 + 1, 3600)).unwrap();
        let t0 = Instant::now();
        for _ in 0..LINES {
            sink.write(InfoKind::Add, &info).unwrap();
        }
        let unbuffered = t0.elapsed();
        sink.close().unwrap();

        let path = dir.join("buffered.jsonl");
        let t0 = Instant::now();
        let mut w = std::io::BufWriter::new(File::create(&path).unwrap());
        for _ in 0..LINES {
            w.write_all(info.as_bytes()).unwrap();
            w.write_all(b"\n").unwrap();
        }
        w.into_inner().unwrap().sync_all().unwrap();
        let buffered = t0.elapsed();
        let per_line = |d: Duration| d.as_secs_f64() * 1e6 / LINES as f64;
        eprintln!(
            "BENCH {LINES} lines x {} B: unbuffered {:.0} ms ({:.2} us/line, {:.0} lines/s), \
             buffered {:.0} ms ({:.2} us/line)",
            info.len() + 1,
            unbuffered.as_secs_f64() * 1e3,
            per_line(unbuffered),
            LINES as f64 / unbuffered.as_secs_f64(),
            buffered.as_secs_f64() * 1e3,
            per_line(buffered),
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn unwritable_dir_fails_on_first_write() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir("readonly");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let sink = FileSink::new(&config(&dir, 1000, 3600)).unwrap();
        let r = sink.write(InfoKind::Add, "{}");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        match r {
            Err(e) => assert!(e.contains("cannot create"), "{e}"),
            // Only root bypasses directory permissions.
            Ok(()) => eprintln!("SKIP: running as root, directory permissions not enforced"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
