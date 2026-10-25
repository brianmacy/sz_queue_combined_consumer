//! Input discovery and streaming line reading.
//!
//! * A file argument is used as-is; a directory expands to its `part-*`
//!   regular files (Spark output), sorted — hidden `.part-*.crc` checksums and
//!   `_SUCCESS` markers are skipped by construction.
//! * Compression is detected from magic bytes, not the extension: gzip
//!   (multi-member) and bzip2 (multi-stream, `pbzip2`/`lbzip2` output) are
//!   decoded on the fly; anything else is read as plain text.
//! * Lines are read as raw bytes (no UTF-8 validation: the consumer
//!   dead-letters malformed records itself), one at a time through a
//!   `BufReader`, so a file is never held in memory. Blank (all-whitespace)
//!   lines are skipped; a trailing `\r` is stripped.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use bzip2::read::MultiBzDecoder;
use flate2::read::MultiGzDecoder;

/// Prefix of the Spark part files a directory argument expands to.
const PART_PREFIX: &str = "part-";
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
/// `BZh` + block-size digit `1`..`9`.
const BZIP2_MAGIC: [u8; 3] = *b"BZh";
/// Read buffer per open file.
const READ_BUFFER_BYTES: usize = 256 * 1024;

/// Expands the CLI inputs: files as given, directories to their sorted
/// `part-*` files. A missing path or a directory with no part files is an
/// error (never a silent zero-record run).
pub fn expand_inputs(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for input in inputs {
        let meta = std::fs::metadata(input)
            .with_context(|| format!("input does not exist: {}", input.display()))?;
        if meta.is_dir() {
            let parts = part_files(input)?;
            if parts.is_empty() {
                bail!(
                    "input directory {} has no {PART_PREFIX}* files",
                    input.display()
                );
            }
            files.extend(parts);
        } else {
            files.push(input.clone());
        }
    }
    Ok(files)
}

/// The `part-*` regular files of `dir`, sorted by name.
fn part_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("cannot list {}", dir.display()))? {
        let entry = entry.with_context(|| format!("cannot list {}", dir.display()))?;
        let is_part = entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.starts_with(PART_PREFIX));
        if is_part && entry.path().is_file() {
            parts.push(entry.path());
        }
    }
    parts.sort();
    Ok(parts)
}

/// Streams the non-blank lines of one (possibly compressed) file.
pub struct LineReader {
    inner: Box<dyn BufRead + Send>,
    /// Physical 1-based line number of the last line read.
    line_no: u64,
    buf: Vec<u8>,
}

impl LineReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut magic = [0u8; 3];
        let n = read_prefix(path, &mut magic)?;
        let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        let inner: Box<dyn BufRead + Send> = if n >= 2 && magic[..2] == GZIP_MAGIC {
            Box::new(BufReader::with_capacity(
                READ_BUFFER_BYTES,
                MultiGzDecoder::new(file),
            ))
        } else if n == 3 && magic == BZIP2_MAGIC {
            Box::new(BufReader::with_capacity(
                READ_BUFFER_BYTES,
                MultiBzDecoder::new(file),
            ))
        } else {
            Box::new(BufReader::with_capacity(READ_BUFFER_BYTES, file))
        };
        Ok(Self {
            inner,
            line_no: 0,
            buf: Vec::new(),
        })
    }

    /// The next non-blank line as `(line number, bytes)`, `None` at EOF.
    pub fn next_record(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        loop {
            self.buf.clear();
            let n = self
                .inner
                .read_until(b'\n', &mut self.buf)
                .with_context(|| format!("read failed after line {}", self.line_no))?;
            if n == 0 {
                return Ok(None);
            }
            self.line_no += 1;
            let mut end = self.buf.len();
            while end > 0 && matches!(self.buf[end - 1], b'\n' | b'\r') {
                end -= 1;
            }
            let line = &self.buf[..end];
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            return Ok(Some((self.line_no, line.to_vec())));
        }
    }
}

/// Reads up to `buf.len()` leading bytes (fewer for a short file).
fn read_prefix(path: &Path, buf: &mut [u8]) -> Result<usize> {
    let mut file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let mut filled = 0;
    while filled < buf.len() {
        let n = file
            .read(&mut buf[filled..])
            .with_context(|| format!("cannot read {}", path.display()))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A fresh scratch directory under the system temp dir (removed on drop).
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "sz-amq-pub-{name}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }
        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let p = self.0.join(name);
            std::fs::write(&p, bytes).expect("write scratch file");
            p
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const TEXT: &[u8] = b"{\"A\":1}\n\n  \r\n{\"A\":2}\r\n\t\n{\"A\":3}";

    fn read_all(path: &Path) -> Vec<(u64, Vec<u8>)> {
        let mut r = LineReader::open(path).expect("open");
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().expect("read") {
            out.push(rec);
        }
        out
    }

    fn expected() -> Vec<(u64, Vec<u8>)> {
        vec![
            (1, b"{\"A\":1}".to_vec()),
            (4, b"{\"A\":2}".to_vec()),
            (6, b"{\"A\":3}".to_vec()),
        ]
    }

    #[test]
    fn plain_lines_skip_blanks_and_keep_physical_line_numbers() {
        let s = Scratch::new("plain");
        assert_eq!(read_all(&s.write("in.jsonl", TEXT)), expected());
    }

    #[test]
    fn gzip_multi_member_is_detected_by_magic() {
        let s = Scratch::new("gz");
        let mut bytes = Vec::new();
        // Two concatenated gzip members: both must be read.
        for half in [&TEXT[..12], &TEXT[12..]] {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(half).unwrap();
            bytes.extend(enc.finish().unwrap());
        }
        // Extension deliberately wrong: detection is by content.
        assert_eq!(read_all(&s.write("in.txt", &bytes)), expected());
    }

    #[test]
    fn bzip2_multi_stream_is_detected_by_magic() {
        let s = Scratch::new("bz2");
        let mut bytes = Vec::new();
        for half in [&TEXT[..12], &TEXT[12..]] {
            let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
            enc.write_all(half).unwrap();
            bytes.extend(enc.finish().unwrap());
        }
        assert_eq!(read_all(&s.write("in.jsonl.bz2", &bytes)), expected());
    }

    #[test]
    fn empty_and_tiny_files_yield_nothing_or_one_line() {
        let s = Scratch::new("tiny");
        assert!(read_all(&s.write("empty", b"")).is_empty());
        assert_eq!(read_all(&s.write("one", b"x")), vec![(1, b"x".to_vec())]);
    }

    #[test]
    fn corrupt_gzip_is_a_read_error_not_eof() {
        let s = Scratch::new("corrupt");
        let p = s.write("bad.gz", &[0x1f, 0x8b, 0x08, 0, 0xde, 0xad, 0xbe, 0xef]);
        let mut r = LineReader::open(&p).expect("open");
        assert!(r.next_record().is_err(), "corruption must fail loudly");
    }

    #[test]
    fn directory_expands_to_sorted_part_files_only() {
        let s = Scratch::new("dir");
        s.write("part-00001-x.json", b"b\n");
        s.write("part-00000-x.json", b"a\n");
        s.write(".part-00000-x.json.crc", b"crc");
        s.write("_SUCCESS", b"");
        std::fs::create_dir_all(s.0.join("part-subdir")).unwrap();
        let single = s.write("single.jsonl", b"c\n");
        let files = expand_inputs(&[s.0.clone(), single.clone()]).expect("expand");
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            ["part-00000-x.json", "part-00001-x.json", "single.jsonl"]
        );
    }

    #[test]
    fn missing_input_and_empty_directory_fail() {
        let s = Scratch::new("missing");
        assert!(expand_inputs(&[s.0.join("nope")]).is_err());
        s.write("_SUCCESS", b"");
        let err = expand_inputs(std::slice::from_ref(&s.0)).unwrap_err();
        assert!(err.to_string().contains("no part-"), "{err}");
    }
}
