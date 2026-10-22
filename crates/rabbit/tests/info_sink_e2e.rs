//! INFO sink + redo drain cap end-to-end tests: the real driver binary in file
//! mode against a real Senzing engine on a THROWAWAY SQLite repository (no
//! broker, no mocks, well under the 500-record unlicensed cap).
//!
//! The repository is created once per test binary run: the SQLite template
//! `<RESOURCEPATH>/templates/G2C.db.template` is copied into a fresh temp
//! directory, the default config (with data source `TEST`) is registered, and
//! the `PIPELINE` section is taken from `SENZING_ENGINE_CONFIGURATION_JSON`
//! (its `SQL` section is replaced). Without that variable every test prints
//! SKIP (FAILS under `IT_REQUIRE_INFRA=1`). Test data: `tests/fixtures/info_sink.yaml`.
//!
//! Response fields asserted on (`AFFECTED_ENTITIES[].ENTITY_ID` of the
//! WITH_INFO document, `RESOLVED_ENTITY.ENTITY_ID` of get_entity) were checked
//! against the Senzing MCP `get_sdk_reference` response_schemas (source:
//! https://senzing.com/docs/release/4/4_0_breaking_changes/4_0_breaking_changes_sdk/).
//!
//! ```bash
//! cargo test -p sz_rabbit_combined_consumer --test info_sink_e2e -- --nocapture --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;
use sz_rust_sdk::prelude::*;

const INSTANCE: &str = "sz_info_sink_e2e";

#[derive(serde::Deserialize)]
struct Fixture {
    data_source: String,
    redo_names: Vec<String>,
    record_template: String,
    sink: SinkCase,
    drain_cap: DrainCapCase,
    drained_cap: DrainedCapCase,
    sigterm: SigtermCase,
    slow_load: SlowLoadCase,
    queue: QueueCase,
    faults: Faults,
    redo_stats: RedoStatsCase,
    fetch_failure_redo_sleep_secs: u64,
    redo_process_failure_table: String,
    final_probe: DrainedCapCase,
    markers: Markers,
    exit_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct RedoStatsCase {
    interval: u64,
    long_record_secs: u64,
}

/// Configs of the fault-fixture INFO sink plugin.
#[derive(serde::Deserialize)]
struct Faults {
    fail_write: String,
    fail_write_and_close: String,
    fail_flush: String,
    block_flush: String,
    block_close: String,
    slow_write: String,
}

#[derive(serde::Deserialize)]
struct SinkCase {
    prefix: String,
    rotate_records: u64,
    rotate_secs: u64,
    flush_secs: u64,
    seal_marker: bool,
}

#[derive(serde::Deserialize)]
struct DrainCapCase {
    max_secs: u64,
    transform_sleep_ms: u64,
    threads: usize,
    redo_percent: u8,
}

#[derive(serde::Deserialize)]
struct DrainedCapCase {
    records: usize,
    max_secs: u64,
    redo_sleep_secs: u64,
}

#[derive(serde::Deserialize)]
struct SigtermCase {
    redo_sleep_secs: u64,
    draining_marker: String,
}

#[derive(serde::Deserialize)]
struct SlowLoadCase {
    transform_sleep_ms: u64,
    acked_before_kill: usize,
    files_before_failure: usize,
}

#[derive(serde::Deserialize)]
struct QueueCase {
    name_prefix: String,
}

#[derive(serde::Deserialize)]
struct Markers {
    redo_idle: String,
    sink_closed: String,
    cap_expired: String,
    remaining_prefix: String,
    write_failed: String,
    final_total: String,
    resume_prefix: String,
    input_exhausted: String,
    fetch_failed: String,
    engine_init_failed: String,
    close_stuck: String,
    flush_stuck: String,
    flush_failed: String,
    remaining_unknown: String,
    redo_stats: String,
    redo_fatal: String,
    reprobe: String,
}

fn fixture() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/info_sink.yaml");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        serde_norway::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
    })
}

fn require_infra() -> bool {
    std::env::var("IT_REQUIRE_INFRA").is_ok_and(|v| !v.is_empty() && v != "0")
}

/// The throwaway repository shared by this binary's tests.
struct Repo {
    /// Engine config JSON pointing at the throwaway SQLite file.
    engine_config: String,
    env: Arc<SzEnvironmentCore>,
    work: PathBuf,
}

/// Creates the repository once; `None` (after a SKIP note) without an engine
/// config to borrow the PIPELINE paths from.
fn repo() -> Option<&'static Repo> {
    static R: OnceLock<Option<Repo>> = OnceLock::new();
    R.get_or_init(|| {
        let Some(base) = std::env::var("SENZING_ENGINE_CONFIGURATION_JSON")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            assert!(
                !require_infra(),
                "IT_REQUIRE_INFRA set but SENZING_ENGINE_CONFIGURATION_JSON is not"
            );
            eprintln!("SKIP info_sink_e2e: SENZING_ENGINE_CONFIGURATION_JSON not set");
            return None;
        };
        Some(create_repo(&base))
    })
    .as_ref()
}

/// The empty SQLite repository to copy. macOS packages ship
/// `templates/G2C.db.template`; the Linux packages ship only the schema DDL,
/// so CI builds the file from it and points `SZ_E2E_SQLITE_TEMPLATE` here.
fn sqlite_template(resources: &str) -> PathBuf {
    std::env::var_os("SZ_E2E_SQLITE_TEMPLATE")
        .filter(|p| !p.is_empty())
        .map_or_else(
            || Path::new(resources).join("templates/G2C.db.template"),
            PathBuf::from,
        )
}

fn create_repo(base: &str) -> Repo {
    let base: Value = serde_json::from_str(base).expect("engine config is JSON");
    let pipeline = base["PIPELINE"].clone();
    let resources = pipeline["RESOURCEPATH"]
        .as_str()
        .expect("PIPELINE.RESOURCEPATH");
    let work = std::env::temp_dir().join(format!("sz_info_sink_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("mkdir work");
    let db = work.join("G2C.db");
    std::fs::copy(sqlite_template(resources), &db).expect("copy SQLite template");
    let engine_config = serde_json::json!({
        "PIPELINE": pipeline,
        "SQL": {"CONNECTION": format!("sqlite3://na:na@{}", db.display())},
    })
    .to_string();
    let env = SzEnvironmentCore::get_instance(INSTANCE, &engine_config, false)
        .expect("initialize Senzing environment");
    let mgr = env.get_config_manager().expect("config manager");
    let config = mgr.create_config().expect("create config from template");
    config
        .register_data_source(&fixture().data_source)
        .expect("register TEST");
    let id = mgr
        .set_default_config(&config.export().expect("export"), Some("info_sink_e2e"))
        .expect("register default config");
    env.reinitialize(id)
        .expect("reinitialize on the new config");
    // A byte copy of the still-empty, configured repository for tests that
    // need a PRIVATE repository to break. The database is in WAL mode and
    // idle here (no writer, no checkpoint), so the main file plus its WAL is
    // a consistent snapshot. NEVER open the shared repository in this process
    // except through the engine: on Linux, closing ANY descriptor on the file
    // releases all of the process's POSIX locks on it, so a second SQLite
    // library, or `std::fs::copy`, would drop the engine's locks and corrupt
    // it. The copy runs in a child process, whose descriptors are its own.
    for (from, to) in sqlite_files(&db)
        .into_iter()
        .zip(sqlite_files(&pristine(&work)))
    {
        if from.exists() {
            let status = Command::new("cp")
                .arg(&from)
                .arg(&to)
                .status()
                .expect("run cp");
            assert!(
                status.success(),
                "snapshot the pristine repository: {status}"
            );
        }
    }
    Repo {
        engine_config,
        env,
        work,
    }
}

/// The pristine snapshot of the shared repository (see `create_repo`).
fn pristine(work: &Path) -> PathBuf {
    work.join("pristine.db")
}

/// A SQLite database file and its write-ahead log.
fn sqlite_files(db: &Path) -> [PathBuf; 2] {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    [db.to_path_buf(), PathBuf::from(wal)]
}

/// A private throwaway repository for one test (a copy of the pristine
/// snapshot in `case`): returns its engine config and database path.
fn private_repo(repo: &Repo, case: &Path) -> (String, PathBuf) {
    let db = case.join("private.db");
    for (from, to) in sqlite_files(&pristine(&repo.work))
        .into_iter()
        .zip(sqlite_files(&db))
    {
        if from.exists() {
            std::fs::copy(&from, &to).expect("copy pristine repository");
        }
    }
    let mut config: Value = serde_json::from_str(&repo.engine_config).unwrap();
    config["SQL"]["CONNECTION"] = Value::from(format!("sqlite3://na:na@{}", db.display()));
    (config.to_string(), db)
}

/// A genuine database failure under a running driver: another connection
/// drops the redo queue table, so the engine's next redo fetch fails
/// (`no such table: SYS_EVAL_QUEUE`, SENZ1001). Only for a PRIVATE
/// repository, which this process's engine never opens (see `create_repo`).
fn drop_redo_queue(db: &Path) {
    drop_table(db, "SYS_EVAL_QUEUE");
}

/// Drops `table` from a PRIVATE repository through its own SQLite
/// connection (waiting up to 30 s for the driver's locks).
fn drop_table(db: &Path, table: &str) {
    use libsqlite3_sys as ffi;
    use std::ffi::{CStr, CString};
    let path = CString::new(db.to_str().expect("UTF-8 path")).unwrap();
    let sql = CString::new(format!("DROP TABLE {table}")).unwrap();
    let mut conn = std::ptr::null_mut();
    // SAFETY: valid C strings and out-pointers; the connection is closed
    // once, and the error message is freed with sqlite3_free.
    unsafe {
        let rc = ffi::sqlite3_open_v2(
            path.as_ptr(),
            &mut conn,
            ffi::SQLITE_OPEN_READWRITE,
            std::ptr::null(),
        );
        assert_eq!(rc, ffi::SQLITE_OK, "open {db:?}");
        ffi::sqlite3_busy_timeout(conn, 30_000);
        let mut err = std::ptr::null_mut();
        let rc = ffi::sqlite3_exec(conn, sql.as_ptr(), None, std::ptr::null_mut(), &mut err);
        let msg = if err.is_null() {
            String::new()
        } else {
            let m = CStr::from_ptr(err).to_string_lossy().into_owned();
            ffi::sqlite3_free(err.cast());
            m
        };
        ffi::sqlite3_close(conn);
        assert_eq!(rc, ffi::SQLITE_OK, "drop {table}: {msg}");
    }
}

fn driver_bin() -> &'static str {
    env!("CARGO_BIN_EXE_sz_rabbit_combined_consumer")
}

/// A cdylib built as a dev-dependency into this test's `deps/` directory.
fn plugin(lib: &str) -> String {
    let deps = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("deps dir")
        .to_path_buf();
    let p = deps.join(format!(
        "{}{lib}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ));
    assert!(p.exists(), "plugin not built at {p:?}");
    p.to_string_lossy().into_owned()
}

/// `n` redo-generating records sharing one `tag`-unique email.
fn records(tag: &str, n: usize) -> Vec<String> {
    let f = fixture();
    (0..n)
        .map(|i| {
            f.record_template
                .replace("{tag}", tag)
                .replace("{i}", &i.to_string())
                .replace("{name}", &f.redo_names[i % f.redo_names.len()])
        })
        .collect()
}

/// A per-test scratch directory under the repository's work dir.
fn case_dir(repo: &Repo, name: &str) -> PathBuf {
    let d = repo.work.join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).expect("mkdir case dir");
    d
}

fn sink_config(dir: &Path) -> String {
    let s = &fixture().sink;
    serde_json::json!({
        "dir": dir,
        "prefix": s.prefix,
        "rotate_records": s.rotate_records,
        "rotate_secs": s.rotate_secs,
        "seal_marker": s.seal_marker,
    })
    .to_string()
}

/// The seal marker of an INFO file.
fn marker_of(file: &Path) -> PathBuf {
    let mut name = file.as_os_str().to_owned();
    name.push(".sealed");
    PathBuf::from(name)
}

/// The driver in file mode over `lines`, stdout+stderr captured to a file.
fn file_mode(repo: &Repo, case: &Path, lines: &[String]) -> (Command, PathBuf) {
    let input = case.join("in.jsonl");
    std::fs::write(&input, lines.join("\n") + "\n").expect("write input");
    let mut cmd = Command::new(driver_bin());
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", &repo.engine_config)
        .env("SENZING_INPUT_FILE", &input)
        .env("SENZING_THREADS_PER_PROCESS", "4")
        .env("SENZING_REDO_PERCENT", "25")
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "1")
        .env(
            "SENZING_INFO_SINK_FLUSH_SECS",
            fixture().sink.flush_secs.to_string(),
        )
        .env_remove("SENZING_AMQP_URL")
        .env_remove("SENZING_RABBITMQ_QUEUE")
        .env_remove("SENZING_RECORD_TRANSFORM_PLUGIN")
        .env_remove("SENZING_INFO_SINK")
        .env_remove("SENZING_REDO_DRAIN_MAX_SECS");
    (cmd, case.join("out.log"))
}

fn spawn(cmd: &mut Command, out: &Path) -> Child {
    let log = std::fs::File::create(out).expect("create log");
    let err = log.try_clone().expect("clone log");
    cmd.stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn driver")
}

fn wait_bounded(child: &mut Child, within: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(s) = child.try_wait().expect("try_wait") {
            return Some(s);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

fn read(path: &Path) -> String {
    let mut s = String::new();
    let _ = std::fs::File::open(path).and_then(|mut f| f.read_to_string(&mut s));
    s
}

/// Runs to completion; returns (exit code, combined output).
fn run(cmd: &mut Command, out: &Path) -> (i32, String) {
    let mut child = spawn(cmd, out);
    let status = wait_bounded(&mut child, Duration::from_secs(fixture().exit_within_secs));
    let log = read(out);
    let status = status.unwrap_or_else(|| panic!("driver did not exit in time\n{log}"));
    (status.code().expect("exit code (not a signal)"), log)
}

/// `(adds, redos)` from the final `Processed total of N adds, M redo records` line.
fn totals(log: &str) -> (usize, usize) {
    let prefix = &fixture().markers.final_total;
    let line = log
        .lines()
        .find_map(|l| l.find(prefix.as_str()).map(|i| &l[i + prefix.len()..]))
        .unwrap_or_else(|| panic!("no final total line\n{log}"));
    let mut w = line.split_whitespace();
    let adds = w.next().and_then(|n| n.parse().ok()).expect("adds");
    let redos = w.nth(1).and_then(|n| n.parse().ok()).expect("redos");
    (adds, redos)
}

/// Every sink file in `dir` (seal markers excluded), sorted; asserts the
/// naming contract.
fn info_files(dir: &Path) -> Vec<PathBuf> {
    let prefix = &fixture().sink.prefix;
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("list info dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_none_or(|e| e != "sealed"))
        .collect();
    files.sort();
    for (i, f) in files.iter().enumerate() {
        let name = f.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, format!("{prefix}{:06}.jsonl", i + 1), "file naming");
    }
    files
}

/// Every INFO line in `dir`, parsed.
fn info_lines(dir: &Path) -> Vec<Value> {
    info_files(dir)
        .iter()
        .flat_map(|f| {
            let text = std::fs::read_to_string(f).expect("read info file");
            assert!(text.ends_with('\n'), "{f:?} not newline-terminated");
            assert!(!text.is_empty(), "{f:?} is empty");
            text.lines()
                .map(|l| serde_json::from_str::<Value>(l).unwrap_or_else(|e| panic!("{l}: {e}")))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn affected(lines: &[Value]) -> BTreeSet<i64> {
    lines
        .iter()
        .flat_map(|v| {
            v["AFFECTED_ENTITIES"]
                .as_array()
                .unwrap_or_else(|| panic!("no AFFECTED_ENTITIES array: {v}"))
                .iter()
                .map(|e| e["ENTITY_ID"].as_i64().expect("ENTITY_ID"))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Final entity id of each record, from get_entity.
fn final_entities(repo: &Repo, lines: &[String]) -> BTreeSet<i64> {
    let engine = repo.env.get_engine().expect("engine");
    lines
        .iter()
        .map(|r| {
            let v: Value = serde_json::from_str(r).unwrap();
            let entity = engine
                .get_entity(
                    EntityRef::Record {
                        data_source: v["DATA_SOURCE"].as_str().unwrap(),
                        record_id: v["RECORD_ID"].as_str().unwrap(),
                    },
                    None,
                )
                .expect("get_entity by record");
            let e: Value = serde_json::from_str(&entity).unwrap();
            e["RESOLVED_ENTITY"]["ENTITY_ID"]
                .as_i64()
                .expect("ENTITY_ID")
        })
        .collect()
}

/// File mode load + redo with the sink: exit 0 via the drain rule (the cap is
/// off by default and not set here), every INFO line parses, lines = adds +
/// redos, every file carries its seal marker, and every record's final
/// entity (get_entity) was reported as affected.
#[test]
fn e2e_info_sink_file_mode_load_and_redo() {
    let Some(repo) = repo() else { return };
    let case = case_dir(repo, "load_redo");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISLR", fixture().redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info));
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 0, "{log}");
    assert!(log.contains(&fixture().markers.sink_closed), "{log}");
    assert!(!log.contains(&fixture().markers.cap_expired), "{log}");

    let (adds, redos) = totals(&log);
    assert_eq!(adds, input.len(), "{log}");
    assert!(redos > 0, "test data generated no redo\n{log}");
    let lines = info_lines(&info);
    assert_eq!(
        lines.len(),
        adds + redos,
        "one INFO line per add and redo\n{log}"
    );
    let per_file = fixture().sink.rotate_records as usize;
    assert!(info_files(&info).len() >= lines.len().div_ceil(per_file));
    for f in info_files(&info) {
        assert!(marker_of(&f).exists(), "{f:?} has no seal marker");
    }

    // Lines are kept raw: a redo INFO may carry an empty key and no affected
    // entities (observed: {"AFFECTED_ENTITIES":[],"DATA_SOURCE":"","RECORD_ID":""}).
    let mut ids_seen = BTreeSet::new();
    for v in &lines {
        let ds = v["DATA_SOURCE"].as_str().expect("DATA_SOURCE");
        assert!(ds == fixture().data_source || ds.is_empty(), "{v}");
        ids_seen.insert(v["RECORD_ID"].as_str().expect("RECORD_ID").to_string());
    }
    let want: BTreeSet<String> = input
        .iter()
        .map(|r| {
            serde_json::from_str::<Value>(r).unwrap()["RECORD_ID"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(want.is_subset(&ids_seen), "every record has INFO");
    let finals = final_entities(repo, &input);
    let reported = affected(&lines);
    assert!(
        finals.is_subset(&reported),
        "final entities {finals:?} not all in AFFECTED_ENTITIES {reported:?}"
    );
    eprintln!(
        "e2e_info_sink_file_mode_load_and_redo: {adds} adds, {redos} redo, {} INFO lines, {} files",
        lines.len(),
        info_files(&info).len()
    );
}

/// A 1 s drain cap with adds still running past end of input (each one
/// enqueuing more redo, so redo really keeps coming): the final probe cannot
/// prove a drain, redo fetching stops, the remaining redo is logged, the sink
/// is still closed and the process exits 3 although nothing was fatal.
#[test]
fn e2e_redo_drain_cap_exits_non_zero_and_logs_remaining() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "drain_cap");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISDC", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info))
        .env(
            "SENZING_REDO_DRAIN_MAX_SECS",
            f.drain_cap.max_secs.to_string(),
        )
        .env(
            "SENZING_THREADS_PER_PROCESS",
            f.drain_cap.threads.to_string(),
        )
        .env("SENZING_REDO_PERCENT", f.drain_cap.redo_percent.to_string())
        .env(
            "SENZING_RECORD_TRANSFORM_PLUGIN",
            plugin("sz_record_transform_example"),
        )
        .env(
            "SENZING_RECORD_TRANSFORM_CONFIG",
            format!(r#"{{"SLEEP_MS":{}}}"#, f.drain_cap.transform_sleep_ms),
        );
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 3, "drain cap must exit 3\n{log}");
    assert!(log.contains(&f.markers.cap_expired), "{log}");
    assert!(log.contains(&f.markers.sink_closed), "{log}");
    let remaining: i64 = log
        .lines()
        .find_map(|l| {
            let i = l.find(f.markers.remaining_prefix.as_str())?;
            l[i + f.markers.remaining_prefix.len()..]
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("no remaining redo count logged\n{log}"));
    let backlog = repo
        .env
        .get_engine()
        .unwrap()
        .count_redo_records()
        .expect("count_redo_records");
    assert_eq!(remaining, backlog, "logged count = what was left\n{log}");
    assert!(remaining > 0, "the cap should have left redo behind\n{log}");
    let (adds, redos) = totals(&log);
    assert_eq!(adds, input.len(), "every add still completed\n{log}");
    assert_eq!(info_lines(&info).len(), adds + redos, "{log}");
    eprintln!("e2e_redo_drain_cap: exit 3, {remaining} redo left, {adds} adds, {redos} redo");
}

/// H2 (reviewer reproduction): no redo at all, a 3 s cap and a 10 s redo
/// sleep. The cap expires while the drain rule still waits for its second
/// empty probe, but redo IS drained: one final probe confirms it and the run
/// exits 0 (it exited 3 before the fix). A startup warning names the short
/// cap.
#[test]
fn e2e_redo_drain_cap_does_not_fire_when_redo_is_drained() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let c = &f.drained_cap;
    let case = case_dir(repo, "drained_cap");
    let input: Vec<String> = (0..c.records)
        .map(|i| records(&format!("ISND{i}"), 1).remove(0))
        .collect();
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_REDO_DRAIN_MAX_SECS", c.max_secs.to_string())
        .env(
            "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
            c.redo_sleep_secs.to_string(),
        );
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 0, "redo was drained; the cap must not fire\n{log}");
    assert!(!log.contains(&f.markers.cap_expired), "{log}");
    assert!(log.contains("shorter than"), "short-cap warning\n{log}");
    // These records enqueue no redo of their own; redo left in the shared
    // repository by an earlier test (the cap case) may be drained here.
    let (adds, _) = totals(&log);
    assert_eq!(adds, input.len(), "{log}");
}

/// A sink that cannot write is FATAL (exit 255); a sink that cannot even be
/// created is a startup configuration error (exit 1). The write failure comes
/// from the fault-fixture plugin (works as root, unlike a read-only
/// directory).
#[test]
fn e2e_info_sink_failure_exits_non_zero() {
    let Some(repo) = repo() else { return };
    let case = case_dir(repo, "sink_fail");
    let input = records("ISSF", 2);

    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &fixture().faults.fail_write);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&fixture().markers.write_failed), "{log}");
    let (adds, _) = totals(&log);
    assert_eq!(
        adds, 0,
        "a record whose INFO was lost is never counted as loaded\n{log}"
    );

    // Missing directory: the plugin's create fails before engine init.
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env(
            "SENZING_INFO_SINK_CONFIG",
            sink_config(&case.join("missing")),
        );
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 1, "{log}");
    assert!(log.contains("create failed"), "{log}");
}

/// SIGTERM while the drain tail sleeps: the open INFO file is sealed by the
/// explicit close (flush timer and rotation are out of reach), every line is
/// there, and the exit is a clean 0 (a signal is not a cap expiry).
#[test]
fn e2e_info_sink_closed_on_sigterm() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "sigterm");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISST", f.redo_names.len());
    let cfg = serde_json::json!({
        "dir": info,
        "prefix": f.sink.prefix,
        "rotate_records": 1_000_000,
        "rotate_secs": 3600,
    })
    .to_string();
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", cfg)
        .env("SENZING_INFO_SINK_FLUSH_SECS", "3600")
        .env(
            "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
            f.sigterm.redo_sleep_secs.to_string(),
        );
    let mut child = spawn(&mut cmd, &out);
    let deadline = Instant::now() + Duration::from_secs(f.exit_within_secs);
    while !read(&out).contains(&f.sigterm.draining_marker) {
        assert!(
            Instant::now() < deadline,
            "never reached the drain\n{}",
            read(&out)
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "exited early\n{}",
            read(&out)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Let the drain tail settle into its long pause.
    std::thread::sleep(Duration::from_secs(3));
    assert!(info_files(&info).len() <= 1);
    // SAFETY: kill(2) on our own child.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    let log = read(&out);
    let status = status.unwrap_or_else(|| panic!("no exit after SIGTERM\n{log}"));
    assert_eq!(status.code(), Some(0), "{log}");
    assert!(log.contains(&f.markers.sink_closed), "{log}");
    let (adds, redos) = totals(&log);
    assert_eq!(adds, input.len(), "{log}");
    let lines = info_lines(&info);
    assert_eq!(info_files(&info).len(), 1, "one file, sealed only by close");
    assert_eq!(lines.len(), adds + redos, "close flushed every line\n{log}");
}

/// Polls `cond` on the driver log until it holds (panics past the deadline or
/// if the driver exits first).
fn wait_for(child: &mut Child, out: &Path, what: &str, cond: impl Fn(&str) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(fixture().exit_within_secs);
    loop {
        let log = read(out);
        if cond(&log) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}\n{log}"
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "exited before {what}\n{log}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// SIGTERMs the driver; returns (exit code, log).
fn sigterm(child: &mut Child, out: &Path) -> (Option<i32>, String) {
    // SAFETY: kill(2) on our own child.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let status = wait_bounded(child, Duration::from_secs(30));
    let log = read(out);
    let status = status.unwrap_or_else(|| panic!("no exit after SIGTERM\n{log}"));
    (status.code(), log)
}

/// INFO lines in SEALED files currently on disk. A file exists under its
/// final name from its first line on (no rename at seal) and its lines are
/// visible as soon as they are written, so only files carrying a seal marker
/// (`seal_marker` is on in the fixture) are counted.
fn sealed_lines(dir: &Path) -> usize {
    info_files(dir)
        .iter()
        .filter(|f| marker_of(f).exists())
        .map(|f| std::fs::read_to_string(f).map_or(0, |t| t.lines().count()))
        .sum()
}

/// Pure redoer (redo% = 100, no input): a redo backlog seeded straight through
/// the engine is processed WITH_INFO into the sink, the main-loop timer seals
/// files while it runs, and SIGTERM closes the sink (exit 0).
#[test]
fn e2e_info_sink_pure_redoer() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "pure_redoer");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let engine = repo.env.get_engine().expect("engine");
    for r in records("ISPR", f.redo_names.len()) {
        let v: Value = serde_json::from_str(&r).unwrap();
        engine
            .add_record(
                v["DATA_SOURCE"].as_str().unwrap(),
                v["RECORD_ID"].as_str().unwrap(),
                &r,
                None,
            )
            .expect("seed add_record");
    }
    let backlog = engine.count_redo_records().expect("count_redo_records");
    assert!(backlog > 0, "seed left no redo backlog");

    let (mut cmd, out) = file_mode(repo, &case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .env("SENZING_REDO_PERCENT", "100")
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info));
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "redo idle", |log| {
        log.contains(&f.markers.redo_idle)
    });
    // The flush timer (1 s) must seal the tail while the process still runs.
    wait_for(&mut child, &out, "timer flush", |_| {
        sealed_lines(&info) >= backlog as usize
    });
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(0), "{log}");
    assert!(log.contains(&f.markers.sink_closed), "{log}");
    let (adds, redos) = totals(&log);
    assert_eq!(adds, 0);
    assert!(redos >= backlog as usize, "{log}");
    assert_eq!(
        info_lines(&info).len(),
        redos,
        "one INFO line per redo\n{log}"
    );
    assert_eq!(engine.count_redo_records().unwrap(), 0, "redo drained");
}

/// Queue mode (RabbitMQ): the sink receives every add and redo INFO, is
/// flushed on the loop's tick and closed after the join on SIGTERM.
#[test]
fn e2e_info_sink_queue_mode() {
    let Some(repo) = repo() else { return };
    let Some(url) = std::env::var("SENZING_AMQP_URL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        assert!(
            !require_infra(),
            "IT_REQUIRE_INFRA set but SENZING_AMQP_URL is not"
        );
        eprintln!("SKIP e2e_info_sink_queue_mode: SENZING_AMQP_URL not set");
        return;
    };
    let f = fixture();
    let case = case_dir(repo, "queue");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISQM", f.redo_names.len());
    let queue = format!("{}-{}", f.queue.name_prefix, std::process::id());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(publish(&url, &queue, &input))
        .expect("publish records");

    let (mut cmd, out) = file_mode(repo, &case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .env("SENZING_AMQP_URL", &url)
        .env("SENZING_RABBITMQ_QUEUE", &queue)
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info));
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "redo idle", |log| {
        log.contains(&f.markers.redo_idle)
    });
    wait_for(&mut child, &out, "timer flush of every add", |_| {
        sealed_lines(&info) >= input.len()
    });
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(0), "{log}");
    assert!(log.contains(&f.markers.sink_closed), "{log}");
    let (adds, redos) = totals(&log);
    assert_eq!(adds, input.len(), "{log}");
    let lines = info_lines(&info);
    assert_eq!(lines.len(), adds + redos, "{log}");
    let finals = final_entities(repo, &input);
    assert!(finals.is_subset(&affected(&lines)), "{finals:?}");
}

/// Declares `queue` and publishes `records` with publisher confirms.
async fn publish(url: &str, queue: &str, records: &[String]) -> anyhow::Result<()> {
    use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Confirmation, Connection, ConnectionProperties};
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await?;
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    for body in records {
        let confirm = channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await?
            .await?;
        anyhow::ensure!(!matches!(confirm, Confirmation::Nack(_)), "publish NACKed");
    }
    conn.close(0, "done".into()).await.ok();
    Ok(())
}

/// One worker, no redo, a sleeping transform: the load proceeds one record
/// at a time so a test can act part-way through it.
fn slow_load(cmd: &mut Command) {
    cmd.env("SENZING_THREADS_PER_PROCESS", "1")
        .env("SENZING_REDO_PERCENT", "0")
        .env(
            "SENZING_RECORD_TRANSFORM_PLUGIN",
            plugin("sz_record_transform_example"),
        )
        .env(
            "SENZING_RECORD_TRANSFORM_CONFIG",
            format!(
                r#"{{"SLEEP_MS":{}}}"#,
                fixture().slow_load.transform_sleep_ms
            ),
        );
}

/// RECORD_IDs of every INFO line on disk (sealed or not), tolerating a cut
/// last line.
fn info_record_ids(dir: &Path) -> BTreeSet<String> {
    info_files(dir)
        .iter()
        .flat_map(|f| {
            std::fs::read_to_string(f)
                .expect("read info file")
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|v| v["RECORD_ID"].as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// RECORD_IDs of the WITH_INFO documents printed by `-i` (printed only after
/// the record was acknowledged, i.e. after its sink write returned).
fn printed_record_ids(log: &str) -> BTreeSet<String> {
    log.lines()
        .filter(|l| l.starts_with('{'))
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| v["RECORD_ID"].as_str().map(str::to_string))
        .collect()
}

/// H1: a SIGKILL (no flush, no seal, no destructor) loses no INFO line of a
/// record the consumer already acknowledged: `sz_is_write` returning 0 means
/// the line is in the kernel, not in an in-process buffer.
#[test]
fn e2e_info_sink_sigkill_keeps_every_acknowledged_line() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "sigkill");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISKL", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    slow_load(&mut cmd);
    cmd.arg("-i")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info))
        .env("SENZING_INFO_SINK_FLUSH_SECS", "3600");
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "acknowledged records", |log| {
        printed_record_ids(log).len() >= f.slow_load.acked_before_kill
    });
    // SAFETY: kill(2) on our own child.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGKILL) }, 0);
    let _ = child.wait();
    let log = read(&out);
    let acked = printed_record_ids(&log);
    let on_disk = info_record_ids(&info);
    assert!(
        acked.is_subset(&on_disk),
        "acknowledged {acked:?} but only {on_disk:?} reached the INFO files\n{log}"
    );
}

/// The printed `--skip-lines` value, if any.
fn resume_point(log: &str) -> Option<usize> {
    let prefix = &fixture().markers.resume_prefix;
    log.lines().find_map(|l| {
        let i = l.find(prefix.as_str())?;
        l[i + prefix.len()..].trim().parse().ok()
    })
}

/// H1/M5: the sink starts failing mid-load (its directory is moved away and
/// a regular FILE takes its place, so the next file or seal marker cannot be
/// created — ENOTDIR, which root gets too): the run is FATAL (255) and the
/// printed resume point never passes a line whose INFO is not in a sealed
/// file.
#[test]
fn e2e_info_sink_failure_mid_load_holds_resume_at_last_seal() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "fail_mid_load");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISFM", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    slow_load(&mut cmd);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", sink_config(&info));
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "sealed files", |_| {
        info_files(&info).len() >= f.slow_load.files_before_failure
    });
    let moved = case.join("info.moved");
    std::fs::rename(&info, &moved).unwrap();
    std::fs::write(&info, b"").unwrap();
    let status = wait_bounded(&mut child, Duration::from_secs(f.exit_within_secs));
    let log = read(&out);
    assert_eq!(status.and_then(|s| s.code()), Some(255), "{log}");
    // A write that rotates, or the timer's flush, hits the missing directory
    // first: either is the sink failure.
    assert!(
        log.contains(&f.markers.write_failed) || log.contains(&f.markers.flush_failed),
        "{log}"
    );
    let resume = resume_point(&log).unwrap_or_else(|| panic!("no resume point\n{log}"));
    assert!(resume < input.len(), "the load did not finish\n{log}");
    let on_disk = info_record_ids(&moved);
    for (i, r) in input.iter().take(resume).enumerate() {
        let id = serde_json::from_str::<Value>(r).unwrap()["RECORD_ID"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            on_disk.contains(&id),
            "line {} ({id}) is below the resume point {resume} but has no INFO\n{log}",
            i + 1
        );
    }
}

/// Seconds between the first and last log lines containing `from` / `to`
/// (UTC `HH:MM:SS.ffffff` timestamps of the tracing output).
fn log_span_secs(log: &str, from: &str, to: &str) -> f64 {
    let stamp = |marker: &str| -> f64 {
        let line = log
            .lines()
            .find(|l| l.contains(marker))
            .unwrap_or_else(|| panic!("no {marker:?} line\n{log}"));
        let t = line.find('T').expect("timestamp") + 1;
        let hms: Vec<f64> = line[t..t + 15]
            .split(':')
            .map(|p| p.parse().expect("time field"))
            .collect();
        hms[0] * 3600.0 + hms[1] * 60.0 + hms[2]
    };
    stamp(to) - stamp(from)
}

/// H1 cost on the real consumer (not a pass/fail gate): the same file load
/// (distinct records, 4 workers, no redo) three ways — plain, WITH_INFO
/// printed to stdout (`-i`, the engine-side cost the sink also forces), and
/// WITH_INFO into the unbuffered file sink — timed from the loader's start
/// line to its finish line. The sink's own cost is sink vs `-i`. With
/// `BENCH_BASELINE_SINK=<other plugin>` the arms are instead this sink vs
/// that one (e.g. an older buffered build). Run alone (it needs a fresh
/// repository for its records); `BENCH_ORDER=rev` runs the arms in reverse,
/// `BENCH_ROTATE_RECORDS` overrides the rotation (default 10000):
/// `cargo test --release -p sz_rabbit_combined_consumer --test info_sink_e2e -- --ignored --nocapture bench_`
#[test]
#[ignore = "benchmark; run explicitly"]
fn bench_file_load_with_and_without_info_sink() {
    const N: usize = 200;
    let Some(repo) = repo() else { return };
    let baseline = std::env::var("BENCH_BASELINE_SINK").ok();
    let mut arms = match baseline {
        Some(_) => vec![("BBAS", "baseline_sink"), ("BSNK", "sink")],
        None => vec![("BPLN", "plain"), ("BINF", "with_info"), ("BSNK", "sink")],
    };
    if std::env::var("BENCH_ORDER").as_deref() == Ok("rev") {
        arms.reverse();
    }
    // ROTATE_RECORDS=4 (the fixture) measures seal (fsync) cost; the default
    // is the plugin's production rotation, which isolates the write path.
    let rotate: u64 = std::env::var("BENCH_ROTATE_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let mut rates = std::collections::BTreeMap::new();
    for (tag, arm) in arms {
        let case = case_dir(repo, tag);
        let info = case.join("info");
        std::fs::create_dir_all(&info).unwrap();
        let bench_cfg = serde_json::json!({
            "dir": info,
            "prefix": fixture().sink.prefix,
            "rotate_records": rotate,
            "rotate_secs": 30,
        })
        .to_string();
        let input: Vec<String> = (0..N)
            .map(|i| records(&format!("{tag}{i}"), 1).remove(0))
            .collect();
        let (mut cmd, out) = file_mode(repo, &case, &input);
        cmd.env("SENZING_REDO_PERCENT", "0");
        match arm {
            "with_info" => {
                cmd.arg("-i");
            }
            "sink" => {
                cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
                    .env("SENZING_INFO_SINK_CONFIG", bench_cfg.clone());
            }
            "baseline_sink" => {
                cmd.env("SENZING_INFO_SINK", baseline.as_deref().unwrap_or_default())
                    .env("SENZING_INFO_SINK_CONFIG", bench_cfg.clone());
            }
            _ => {}
        }
        let (code, log) = run(&mut cmd, &out);
        assert_eq!(code, 0, "{log}");
        let secs = log_span_secs(&log, "File loader: reading", "File loader finished");
        let rate = N as f64 / secs;
        rates.insert(arm, rate);
        eprintln!("BENCH {arm}: {N} records in {secs:.3} s = {rate:.1} records/s");
    }
    let base = if rates.contains_key("baseline_sink") {
        "baseline_sink"
    } else {
        "with_info"
    };
    eprintln!(
        "BENCH sink vs {base}: {:.1}% slower",
        (1.0 - rates["sink"] / rates[base]) * 100.0
    );
}

/// M7: the flush timer runs during the LOAD phase (not only after the reader
/// returns): with one slow worker the reader stays blocked on the work
/// channel for seconds, yet the 1 s timer seals several files before the
/// input is exhausted.
#[test]
fn e2e_info_sink_flushes_while_the_file_still_loads() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "flush_during_load");
    let info = case.join("info");
    std::fs::create_dir_all(&info).unwrap();
    let input = records("ISFL", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    slow_load(&mut cmd);
    let cfg = serde_json::json!({
        "dir": info,
        "prefix": f.sink.prefix,
        "rotate_records": 1_000_000,
        "rotate_secs": 3600,
    })
    .to_string();
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_file"))
        .env("SENZING_INFO_SINK_CONFIG", cfg);
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "two timer-sealed files", |_| {
        info_files(&info).len() >= 2
    });
    let log = read(&out);
    assert!(
        !log.contains(&f.markers.input_exhausted),
        "files were only sealed after the input was read\n{log}"
    );
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(0), "{log}");
}

/// L8: a redo whose INFO write failed is neither counted as processed nor
/// printed (`-i`): the pure redoer drains a real backlog into a sink whose
/// every write fails (fault fixture) and must exit 255 reporting 0 redo
/// processed.
#[test]
fn e2e_failed_redo_info_write_is_not_counted_or_printed() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "redo_write_fail");
    let engine = repo.env.get_engine().expect("engine");
    for r in records("ISRW", f.redo_names.len()) {
        let v: Value = serde_json::from_str(&r).unwrap();
        engine
            .add_record(
                v["DATA_SOURCE"].as_str().unwrap(),
                v["RECORD_ID"].as_str().unwrap(),
                &r,
                None,
            )
            .expect("seed add_record");
    }
    assert!(
        engine.count_redo_records().expect("count") > 0,
        "no redo seeded"
    );
    let (mut cmd, out) = file_mode(repo, &case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .arg("-i")
        .env("SENZING_REDO_PERCENT", "100")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.fail_write);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.write_failed), "{log}");
    let (_, redos) = totals(&log);
    assert_eq!(
        redos, 0,
        "a redo whose INFO was lost is not processed\n{log}"
    );
    assert!(
        printed_record_ids(&log).is_empty(),
        "its INFO must not be printed either\n{log}"
    );
}

/// The `SENZING_AMQP_URL` for queue-mode cases, or `None` (SKIP; FAILS under
/// `IT_REQUIRE_INFRA=1`).
fn amqp_url(test: &str) -> Option<String> {
    let url = std::env::var("SENZING_AMQP_URL")
        .ok()
        .filter(|s| !s.is_empty());
    if url.is_none() {
        assert!(
            !require_infra(),
            "IT_REQUIRE_INFRA set but SENZING_AMQP_URL is not"
        );
        eprintln!("SKIP {test}: SENZING_AMQP_URL not set");
    }
    url
}

/// Messages ready in `queue` (passive declare).
async fn queue_depth(url: &str, queue: &str) -> anyhow::Result<u32> {
    use lapin::options::QueueDeclareOptions;
    use lapin::types::FieldTable;
    use lapin::{Connection, ConnectionProperties};
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    let q = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;
    conn.close(0, "done".into()).await.ok();
    Ok(q.message_count())
}

/// Queue mode with a sink that cannot write (fault fixture: write and close
/// fail, like a sink in its FAILED state): FATAL (exit 255), the close reports
/// the failed sink, and NO delivery is acked — every message is back on the
/// queue for redelivery.
#[test]
fn e2e_info_sink_failure_in_queue_mode_acks_nothing() {
    let Some(repo) = repo() else { return };
    let Some(url) = amqp_url("e2e_info_sink_failure_in_queue_mode_acks_nothing") else {
        return;
    };
    let f = fixture();
    let case = case_dir(repo, "queue_fail");
    let input = records("ISQF", 3);
    let queue = format!("{}-fail-{}", f.queue.name_prefix, std::process::id());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(publish(&url, &queue, &input))
        .expect("publish records");
    let (mut cmd, out) = file_mode(repo, &case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .env("SENZING_AMQP_URL", &url)
        .env("SENZING_RABBITMQ_QUEUE", &queue)
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.fail_write_and_close);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.write_failed), "{log}");
    assert!(log.contains("INFO sink close failed"), "{log}");
    let (adds, _) = totals(&log);
    assert_eq!(adds, 0, "nothing acked\n{log}");
    let depth = rt.block_on(queue_depth(&url, &queue)).expect("queue depth");
    assert_eq!(
        depth as usize,
        input.len(),
        "every delivery is back on the queue\n{log}"
    );
}

/// H2: the cap still fires (exit 3) when redo really keeps coming — here
/// from a "sibling" (this test, on the same repository) that adds
/// redo-generating records once the run is draining. The final probe at
/// expiry returns a redo record, which is still processed, and the cap fires.
#[test]
fn e2e_redo_drain_cap_fires_when_a_sibling_keeps_adding_redo() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let c = &f.drained_cap;
    let case = case_dir(repo, "cap_sibling");
    let input: Vec<String> = (0..c.records)
        .map(|i| records(&format!("ISKS{i}"), 1).remove(0))
        .collect();
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_REDO_DRAIN_MAX_SECS", c.max_secs.to_string())
        .env(
            "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
            c.redo_sleep_secs.to_string(),
        );
    let mut child = spawn(&mut cmd, &out);
    // Input done and the fetcher in its (10 s) empty-queue pause, in either
    // log order: it will not fetch again before the cap's final probe.
    wait_for(
        &mut child,
        &out,
        "the drain tail's empty-queue pause",
        |log| log.contains(&f.sigterm.draining_marker) && log.contains(&f.markers.redo_idle),
    );
    let engine = repo.env.get_engine().expect("engine");
    for r in records("ISKR", f.redo_names.len()) {
        let v: Value = serde_json::from_str(&r).unwrap();
        engine
            .add_record(
                v["DATA_SOURCE"].as_str().unwrap(),
                v["RECORD_ID"].as_str().unwrap(),
                &r,
                None,
            )
            .expect("sibling add_record");
    }
    assert!(
        engine.count_redo_records().expect("count") > 0,
        "no redo added"
    );
    let status = wait_bounded(&mut child, Duration::from_secs(f.exit_within_secs));
    let log = read(&out);
    assert_eq!(status.and_then(|s| s.code()), Some(3), "{log}");
    assert!(log.contains(&f.markers.cap_expired), "{log}");
    let (_, redos) = totals(&log);
    assert!(
        redos >= 1,
        "the record returned by the final probe ran\n{log}"
    );
}

/// Pure-redoer command line on `case` (no input file, no sink by default).
fn pure_redoer(repo: &Repo, case: &Path) -> (Command, PathBuf) {
    let (mut cmd, out) = file_mode(repo, case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .env("SENZING_REDO_PERCENT", "100");
    (cmd, out)
}

/// Queue-mode command line on `case` consuming `queue` (declared empty).
fn queue_mode(repo: &Repo, case: &Path, url: &str, queue: &str) -> (Command, PathBuf) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(publish(url, queue, &[]))
        .expect("declare queue");
    let (mut cmd, out) = file_mode(repo, case, &[]);
    cmd.env_remove("SENZING_INPUT_FILE")
        .env("SENZING_AMQP_URL", url)
        .env("SENZING_RABBITMQ_QUEUE", queue);
    (cmd, out)
}

/// Seeds a redo backlog in the shared repository straight through the
/// engine (`n` records sharing one `tag`-unique email); returns its size.
fn seed_redo(repo: &Repo, tag: &str, n: usize) -> i64 {
    let engine = repo.env.get_engine().expect("engine");
    for r in records(tag, n) {
        let v: Value = serde_json::from_str(&r).unwrap();
        engine
            .add_record(
                v["DATA_SOURCE"].as_str().unwrap(),
                v["RECORD_ID"].as_str().unwrap(),
                &r,
                None,
            )
            .expect("seed add_record");
    }
    let backlog = engine.count_redo_records().expect("count_redo_records");
    assert!(backlog > 0, "seed left no redo backlog");
    backlog
}

/// Waits (bounded) for the driver to exit; returns (exit code, log).
fn exit_of(child: &mut Child, out: &Path) -> (Option<i32>, String) {
    let status = wait_bounded(child, Duration::from_secs(fixture().exit_within_secs));
    let log = read(out);
    let status = status.unwrap_or_else(|| panic!("driver did not exit in time\n{log}"));
    (status.code(), log)
}

/// `--redo-stats-interval`: the pure redoer (no sink, `-i`) logs one
/// throughput line per `interval` processed redo records — reachable here
/// with a handful of redo (the 1000 default is not, under the 500-record
/// cap) — prints each redo's INFO, and with a short LONG_RECORD also its
/// periodic engine stats.
#[test]
fn e2e_redo_stats_line_follows_redo_stats_interval() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let s = &f.redo_stats;
    let case = case_dir(repo, "redo_stats");
    let backlog = seed_redo(repo, "ISRS", f.redo_names.len());
    let (mut cmd, out) = pure_redoer(repo, &case);
    cmd.arg("-i")
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_REDO_STATS_INTERVAL", s.interval.to_string())
        .env("LONG_RECORD", s.long_record_secs.to_string());
    let mut child = spawn(&mut cmd, &out);
    wait_for(
        &mut child,
        &out,
        "redo idle and periodic engine stats",
        |log| log.contains(&f.markers.redo_idle) && log.contains("Engine stats:"),
    );
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(0), "{log}");
    let (_, redos) = totals(&log);
    assert!(redos >= backlog as usize, "{log}");
    // The worker's periodic line ends in "/sec"; the final summary line
    // carries ", runtime: ..." after it.
    let lines = log
        .lines()
        .filter(|l| l.contains(&f.markers.redo_stats) && l.ends_with("/sec"))
        .count();
    assert_eq!(
        lines as u64,
        redos as u64 / s.interval,
        "one line per {} redo\n{log}",
        s.interval
    );
    assert!(
        log.lines().filter(|l| l.starts_with('{')).count() >= redos,
        "every processed redo's INFO is printed (-i)\n{log}"
    );
}

/// The redo fetcher cannot get an engine (the repository database cannot be
/// opened, so `Sz_init` fails): FATAL, exit 255, in file mode.
#[test]
fn e2e_redo_fetcher_engine_failure_is_fatal() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "fetcher_no_engine");
    let (mut cmd, out) = file_mode(repo, &case, &records("ISNE", 1));
    let mut config: Value = serde_json::from_str(&repo.engine_config).unwrap();
    config["SQL"]["CONNECTION"] = Value::from(format!(
        "sqlite3://na:na@{}",
        case.join("missing/G2C.db").display()
    ));
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", config.to_string());
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.engine_init_failed), "{log}");
}

/// Drops the redo table of the private repository once the driver logged
/// `marker` (it then reaches its next redo fetch), and returns its exit.
fn break_redo_fetch(
    cmd: &mut Command,
    out: &Path,
    db: &Path,
    marker: &str,
) -> (Option<i32>, String) {
    let mut child = spawn(cmd, out);
    wait_for(&mut child, out, marker, |log| log.contains(marker));
    drop_redo_queue(db);
    exit_of(&mut child, out)
}

/// A redo fetch that fails mid-run (the redo table is dropped under the
/// pure redoer): FATAL, exit 255, the engine error logged.
#[test]
fn e2e_redo_fetch_failure_is_fatal_in_pure_redoer() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "fetch_fail_pure");
    let (config, db) = private_repo(repo, &case);
    let (mut cmd, out) = pure_redoer(repo, &case);
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", config).env(
        "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        f.fetch_failure_redo_sleep_secs.to_string(),
    );
    let (code, log) = break_redo_fetch(&mut cmd, &out, &db, &f.markers.redo_idle);
    assert_eq!(code, Some(255), "{log}");
    assert!(log.contains(&f.markers.fetch_failed), "{log}");
}

/// Queue mode (redo% 25, mixed): a failing redo fetch reaches the async
/// loop through the fetcher's durable fatal outcome: exit 255.
#[test]
fn e2e_redo_fetch_failure_is_fatal_in_queue_mode() {
    let Some(repo) = repo() else { return };
    let Some(url) = amqp_url("e2e_redo_fetch_failure_is_fatal_in_queue_mode") else {
        return;
    };
    let f = fixture();
    let case = case_dir(repo, "fetch_fail_queue");
    let (config, db) = private_repo(repo, &case);
    let queue = format!("{}-fetch-{}", f.queue.name_prefix, std::process::id());
    let (mut cmd, out) = queue_mode(repo, &case, &url, &queue);
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", config).env(
        "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        f.fetch_failure_redo_sleep_secs.to_string(),
    );
    let (code, log) = break_redo_fetch(&mut cmd, &out, &db, &f.markers.redo_idle);
    assert_eq!(code, Some(255), "{log}");
    assert!(log.contains(&f.markers.fetch_failed), "{log}");
}

/// File mode: the drain cap expires while the drain rule still waits, and
/// the cap's final probe fails (redo table dropped during the pause): FATAL
/// (255), never reported as a cap expiry (3).
#[test]
fn e2e_redo_final_probe_failure_at_the_cap_is_fatal() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let c = &f.final_probe;
    let case = case_dir(repo, "final_probe_fail");
    let (config, db) = private_repo(repo, &case);
    let input: Vec<String> = (0..c.records)
        .map(|i| records(&format!("ISFP{i}"), 1).remove(0))
        .collect();
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", config)
        .env("SENZING_REDO_DRAIN_MAX_SECS", c.max_secs.to_string())
        .env(
            "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
            c.redo_sleep_secs.to_string(),
        );
    let mut child = spawn(&mut cmd, &out);
    // Input done and the fetcher in its empty-queue pause: its next fetch
    // is the cap's final probe.
    wait_for(&mut child, &out, "the drain tail's pause", |log| {
        log.contains(&f.sigterm.draining_marker) && log.contains(&f.markers.redo_idle)
    });
    drop_redo_queue(&db);
    let (code, log) = exit_of(&mut child, &out);
    assert_eq!(code, Some(255), "{log}");
    assert!(log.contains(&f.markers.fetch_failed), "{log}");
    assert!(!log.contains(&f.markers.cap_expired), "{log}");
}

/// A REAL plugin whose close never returns, in file mode, after the drain
/// cap fired: the close is bounded (CLOSE_TIMEOUT), the stuck sink is named,
/// the remaining-redo count is not taken (leak-on-exit run), and the run is
/// FATAL (255, outranking the cap's 3).
#[test]
fn e2e_stuck_sink_close_is_fatal_in_file_mode() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "stuck_close_file");
    let input = records("ISSC", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.block_close)
        .env(
            "SENZING_REDO_DRAIN_MAX_SECS",
            f.drain_cap.max_secs.to_string(),
        )
        .env(
            "SENZING_THREADS_PER_PROCESS",
            f.drain_cap.threads.to_string(),
        )
        .env("SENZING_REDO_PERCENT", f.drain_cap.redo_percent.to_string())
        .env(
            "SENZING_RECORD_TRANSFORM_PLUGIN",
            plugin("sz_record_transform_example"),
        )
        .env(
            "SENZING_RECORD_TRANSFORM_CONFIG",
            format!(r#"{{"SLEEP_MS":{}}}"#, f.drain_cap.transform_sleep_ms),
        );
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.close_stuck), "{log}");
    assert!(log.contains(&f.markers.cap_expired), "{log}");
    assert!(log.contains(&f.markers.remaining_unknown), "{log}");
}

/// A REAL plugin whose timed flush never returns, during a slow file load:
/// stopping the flush thread is bounded, the run is FATAL (255) and the
/// close is never attempted on the wedged sink.
#[test]
fn e2e_stuck_timed_flush_is_fatal_in_file_mode() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "stuck_flush_file");
    let input = records("ISBF", f.redo_names.len());
    let (mut cmd, out) = file_mode(repo, &case, &input);
    slow_load(&mut cmd);
    cmd.env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.block_flush);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.flush_stuck), "{log}");
    assert!(!log.contains(&f.markers.sink_closed), "{log}");
}

/// A REAL plugin whose flush fails: the pure redoer's timer flush is FATAL
/// (255).
#[test]
fn e2e_failing_sink_flush_is_fatal_in_pure_redoer() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "flush_fail_pure");
    let (mut cmd, out) = pure_redoer(repo, &case);
    cmd.env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.fail_flush);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.flush_failed), "{log}");
}

/// A REAL plugin whose flush fails: queue mode's flush tick is FATAL (255).
#[test]
fn e2e_failing_sink_flush_is_fatal_in_queue_mode() {
    let Some(repo) = repo() else { return };
    let Some(url) = amqp_url("e2e_failing_sink_flush_is_fatal_in_queue_mode") else {
        return;
    };
    let f = fixture();
    let case = case_dir(repo, "flush_fail_queue");
    let queue = format!("{}-flush-{}", f.queue.name_prefix, std::process::id());
    let (mut cmd, out) = queue_mode(repo, &case, &url, &queue);
    cmd.env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.fail_flush);
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.flush_failed), "{log}");
}

/// A REAL plugin whose close never returns, in queue mode on SIGTERM: the
/// close is bounded, the stuck sink is named, and the run is FATAL (255).
#[test]
fn e2e_stuck_sink_close_is_fatal_in_queue_mode() {
    let Some(repo) = repo() else { return };
    let Some(url) = amqp_url("e2e_stuck_sink_close_is_fatal_in_queue_mode") else {
        return;
    };
    let f = fixture();
    let case = case_dir(repo, "stuck_close_queue");
    let queue = format!("{}-stuck-{}", f.queue.name_prefix, std::process::id());
    let (mut cmd, out) = queue_mode(repo, &case, &url, &queue);
    cmd.env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.block_close);
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "redo idle", |log| {
        log.contains(&f.markers.redo_idle)
    });
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(255), "{log}");
    assert!(log.contains(&f.markers.close_stuck), "{log}");
}

/// `process_redo_record` hits a genuine database failure (a resolution table
/// of the private repository is gone; the redo queue is intact, so the fetch
/// still works): the worker reports it FATAL and the run exits 255 with no
/// redo counted.
#[test]
fn e2e_redo_processing_failure_is_fatal() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "redo_process_fail");
    let (config, db) = private_repo(repo, &case);
    // Load only (redo% 0): the redo the adds enqueue stays queued.
    let (mut cmd, out) = file_mode(repo, &case, &records("ISPF", f.redo_names.len()));
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", &config)
        .env("SENZING_REDO_PERCENT", "0");
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 0, "{log}");
    drop_table(&db, &f.redo_process_failure_table);
    let (mut cmd, out) = pure_redoer(repo, &case);
    cmd.env("SENZING_ENGINE_CONFIGURATION_JSON", &config)
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120");
    let (code, log) = run(&mut cmd, &out);
    assert_eq!(code, 255, "{log}");
    assert!(log.contains(&f.markers.redo_fatal), "{log}");
    assert_eq!(totals(&log).1, 0, "{log}");
}

/// The drain-tail re-probe: redo reads empty while this process still has
/// redo in flight, so the fetcher re-probes on the short cadence instead of
/// its long pause (logged at debug, enabled through RUST_LOG for this run).
/// One worker inside a slow INFO write and a backlog larger than the worker
/// plus the redo channel keep the fetcher blocked on the full channel until
/// its last record, so that record is in flight when the queue reads empty.
#[test]
fn e2e_redo_fetcher_reprobes_while_redo_is_in_flight() {
    let Some(repo) = repo() else { return };
    let f = fixture();
    let case = case_dir(repo, "reprobe");
    // One worker + a redo channel of 1 + 2.
    let backlog = seed_redo(repo, "ISIF", f.redo_names.len());
    assert!(backlog > 4, "backlog {backlog} would fit in flight at once");
    let (mut cmd, out) = pure_redoer(repo, &case);
    cmd.env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "120")
        .env("SENZING_THREADS_PER_PROCESS", "1")
        .env("RUST_LOG", "info,sz_combined_consumer_core::redo=debug")
        .env("SENZING_INFO_SINK", plugin("sz_info_sink_fault_fixture"))
        .env("SENZING_INFO_SINK_CONFIG", &f.faults.slow_write);
    let mut child = spawn(&mut cmd, &out);
    wait_for(&mut child, &out, "the in-flight re-probe", |log| {
        log.contains(&f.markers.reprobe)
    });
    let (code, log) = sigterm(&mut child, &out);
    assert_eq!(code, Some(0), "{log}");
}
