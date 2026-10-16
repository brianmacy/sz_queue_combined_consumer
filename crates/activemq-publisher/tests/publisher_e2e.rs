//! `sz_activemq_publisher` end-to-end tests against a real Apache ActiveMQ
//! Artemis broker (AMQP 1.0). No Senzing engine: what was published is
//! verified by draining the queue over AMQP and decoding each body with the
//! consumer's own decoder (`sz_activemq_amqp::body_bytes`). The publish →
//! consume round trip through the real consumer + engine is in
//! `crates/activemq/tests/activemq_e2e.rs`.
//!
//! Gated on `SENZING_ACTIVEMQ_URL` (with credentials, e.g.
//! `amqp://artemis:artemis@localhost:5673`) and `IT_ARTEMIS_JOLOKIA_URL`
//! (management: address settings + queue counts). Without them each test
//! prints `SKIP` and passes — or FAILS when `IT_REQUIRE_INFRA=1` (CI).
//! Runs against a stock `apache/artemis` (default AMQP max-frame-size 131072):
//! records larger than a frame are sent as several frames.
//!
//! Proven here (data in `tests/fixtures/publisher.yaml`):
//! * a Spark-style directory of plain / gzip / bzip2 part files publishes
//!   every non-blank line exactly once, durable, decodable by the consumer;
//! * an address that is full under `addressFullMessagePolicy=BLOCK` blocks
//!   the publisher (logged, no reconnect, no resend) until it is drained —
//!   then every record arrives exactly once;
//! * a broker link loss mid-run reconnects and loses nothing;
//! * bad credentials and an unreachable broker exit 1.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::Body;
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Session};
use serde_json::json;
use sz_activemq_amqp::{body_bytes, queue_source};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

// --------------------------------------------------------------------------
// Gating, fixtures
// --------------------------------------------------------------------------

/// Report a skipped test; panic instead when `IT_REQUIRE_INFRA=1` (CI).
fn skip(reason: &str) {
    let required = std::env::var("IT_REQUIRE_INFRA").is_ok_and(|v| !v.is_empty() && v != "0");
    assert!(
        !required,
        "IT_REQUIRE_INFRA set but test cannot run: {reason}"
    );
    eprintln!("{reason}");
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// The broker URL when Artemis (AMQP + Jolokia) is configured.
fn gate() -> Option<String> {
    for var in ["SENZING_ACTIVEMQ_URL", "IT_ARTEMIS_JOLOKIA_URL"] {
        if env_nonempty(var).is_none() {
            skip(&format!("SKIP: {var} not set"));
            return None;
        }
    }
    env_nonempty("SENZING_ACTIVEMQ_URL")
}

#[derive(serde::Deserialize)]
struct Fixture {
    record_template: String,
    directory: DirectoryCase,
    blocked: BlockedCase,
    link_loss: LinkLossCase,
    frozen: FrozenCase,
    large: LargeCase,
    oversize: OversizeCase,
    resume: ResumeCase,
    silent_broker: SilentBrokerCase,
    failures: FailureCase,
}

#[derive(serde::Deserialize)]
struct DirectoryCase {
    queue: String,
    records_per_part: usize,
    blank_every: usize,
    parallel: usize,
    exit_within_secs: u64,
    drained_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct BlockedCase {
    queue: String,
    count: usize,
    max_size_bytes: u64,
    page_size_bytes: u64,
    blocked_log_interval_secs: u64,
    blocked_marker: String,
    hold_blocked_secs: u64,
    blocked_within_secs: u64,
    exit_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct LinkLossCase {
    queue: String,
    count: usize,
    max_size_bytes: u64,
    page_size_bytes: u64,
    blocked_marker: String,
    reconnect_marker: String,
    blocked_within_secs: u64,
    exit_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct FrozenCase {
    queue: String,
    count: usize,
    max_pending: usize,
    started_marker: String,
    freeze_after_ms: u64,
    reconnect_marker: String,
    reconnect_within_secs: u64,
    exit_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct LargeCase {
    queue: String,
    record_template: String,
    count: usize,
    pad_bytes: Vec<usize>,
    max_pending: usize,
    client_max_frame_size: u32,
    exit_within_secs: u64,
    drain_idle_secs: u64,
    drained_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct OversizeCase {
    queue: String,
    record_template: String,
    small_count: usize,
    oversize_line: usize,
    side_max_bytes: usize,
    oversize_bytes: usize,
    failed_marker: String,
    exit_within_secs: u64,
    drained_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct ResumeCase {
    queue: String,
    count: usize,
    skip: usize,
    blank_every: usize,
    exit_within_secs: u64,
    drained_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct SilentBrokerCase {
    reconnect_timeout_secs: u64,
    exit_within_secs: u64,
}

#[derive(serde::Deserialize)]
struct FailureCase {
    bad_password: String,
    impossible_min_max_frame_size: u64,
    unreachable_url: String,
    reconnect_timeout_secs: u64,
    exit_within_secs: u64,
}

fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/publisher.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_norway::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// `name-<pid>`: unique per run against a long-lived broker.
fn unique(name: &str) -> String {
    format!("{name}-{}", std::process::id())
}

fn records(template: &str, tag: &str, range: std::ops::Range<usize>) -> Vec<String> {
    range
        .map(|i| {
            template
                .replace("{tag}", tag)
                .replace("{i}", &i.to_string())
        })
        .collect()
}

/// A scratch directory (removed on drop).
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(unique(&format!("sz-amq-pub-e2e-{name}")));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }

    /// Writes `lines` as JSONL, a blank line after every `blank_every`.
    fn jsonl(&self, name: &str, lines: &[String], blank_every: usize) -> PathBuf {
        let mut text = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            text.extend_from_slice(l.as_bytes());
            text.push(b'\n');
            if blank_every > 0 && (i + 1) % blank_every == 0 {
                text.extend_from_slice(b"  \n");
            }
        }
        let path = self.0.join(name);
        std::fs::write(&path, text).expect("write jsonl");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// --------------------------------------------------------------------------
// Jolokia (Artemis management)
// --------------------------------------------------------------------------

fn jolokia(request: &serde_json::Value) -> serde_json::Value {
    let url = env_nonempty("IT_ARTEMIS_JOLOKIA_URL").expect("gated");
    let out = Command::new("curl")
        .args(["-sS", "-X", "POST", "-H", "Content-Type: application/json"])
        .args(["-d", &request.to_string(), &url])
        .output()
        .expect("failed to run curl (Jolokia)");
    assert!(
        out.status.success(),
        "Jolokia request failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("Jolokia returned JSON")
}

fn broker_mbean() -> String {
    let brokers =
        jolokia(&json!({"type": "search", "mbean": "org.apache.activemq.artemis:broker=*"}));
    let name = brokers["value"][0]
        .as_str()
        .unwrap_or_else(|| panic!("no Artemis broker MBean: {brokers}"));
    name.to_string()
}

/// Sets BLOCK with a tiny `maxSizeBytes` on address `queue` (dynamic
/// address settings, applied before the address is auto-created).
fn block_address(queue: &str, max_size_bytes: u64, page_size_bytes: u64) {
    let settings = json!({
        "maxSizeBytes": max_size_bytes,
        "pageSizeBytes": page_size_bytes,
        "addressFullMessagePolicy": "BLOCK",
    });
    let resp = jolokia(&json!({
        "type": "exec",
        "mbean": broker_mbean(),
        "operation": "addAddressSettings(java.lang.String,java.lang.String)",
        "arguments": [queue, settings.to_string()],
    }));
    assert_eq!(resp["status"], 200, "addAddressSettings failed: {resp}");
}

fn remove_address_settings(queue: &str) {
    let _ = jolokia(&json!({
        "type": "exec",
        "mbean": broker_mbean(),
        "operation": "removeAddressSettings(java.lang.String)",
        "arguments": [queue],
    }));
}

/// Messages on the ANYCAST queue `queue`; `None` while it does not exist.
fn queue_count(queue: &str) -> Option<u64> {
    let broker = broker_mbean();
    let mbean = format!(
        "{broker},component=addresses,address=\"{queue}\",subcomponent=queues,\
         routing-type=\"anycast\",queue=\"{queue}\""
    );
    let resp = jolokia(&json!({"type": "read", "mbean": mbean, "attribute": "MessageCount"}));
    (resp["status"] == 200).then(|| resp["value"].as_u64().unwrap_or(0))
}

// --------------------------------------------------------------------------
// AMQP drain (the consumer's decoding)
// --------------------------------------------------------------------------

/// One drained message.
struct Drained {
    body: String,
    durable: bool,
}

/// Receives from `queue` until `stop` is set AND the queue has been idle
/// for `idle`, or `within` elapses. Bodies decode with `body_bytes`, exactly
/// as the consumer does.
async fn drain(
    url: &str,
    queue: &str,
    mut stop: watch::Receiver<bool>,
    idle: Duration,
    within: Duration,
) -> Vec<Drained> {
    let mut connection: ConnectionHandle<()> =
        Connection::open(unique("sz-amq-pub-e2e-drain"), url)
            .await
            .expect("drain client connects");
    let mut session: SessionHandle<()> = Session::begin(&mut connection).await.expect("session");
    let mut receiver = Receiver::builder()
        .name(unique(&format!("{queue}-drain")))
        .source(queue_source(queue))
        .attach(&mut session)
        .await
        .expect("attach receiver");
    let deadline = Instant::now() + within;
    let mut got = Vec::new();
    while Instant::now() < deadline {
        match tokio::time::timeout(idle, receiver.recv::<Body<Value>>()).await {
            Ok(next) => {
                let delivery = next.expect("receive");
                receiver.accept(&delivery).await.expect("accept");
                let durable = delivery
                    .message()
                    .header
                    .as_ref()
                    .is_some_and(|h| h.durable);
                let body = body_bytes(delivery.into_message().body);
                got.push(Drained {
                    body: String::from_utf8(body).expect("utf-8 record"),
                    durable,
                });
            }
            Err(_) if *stop.borrow_and_update() => break,
            Err(_) => {}
        }
    }
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    got
}

/// Body -> times seen.
fn tally(drained: &[Drained]) -> BTreeMap<&str, usize> {
    let mut m = BTreeMap::new();
    for d in drained {
        *m.entry(d.body.as_str()).or_default() += 1;
    }
    m
}

// --------------------------------------------------------------------------
// Publisher process
// --------------------------------------------------------------------------

struct Publisher {
    child: std::process::Child,
    out: PathBuf,
}

impl Publisher {
    fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Self {
        // One file per spawn: tests run in parallel in one process, so a
        // name derived from the args (as before) let two tests truncate and
        // delete each other's output.
        static SPAWNED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = SPAWNED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let out = std::env::temp_dir().join(format!("{}.out", unique(&format!("sz-amq-pub-{n}"))));
        let stdout = std::fs::File::create(&out).expect("stdout file");
        let stderr = stdout.try_clone().expect("stderr file");
        let child = Command::new(env!("CARGO_BIN_EXE_sz_activemq_publisher"))
            .args(args)
            .env_remove("SENZING_ACTIVEMQ_USER")
            .env_remove("SENZING_ACTIVEMQ_PASSWORD")
            .envs(envs.iter().copied())
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn publisher");
        Self { child, out }
    }

    fn output(&self) -> String {
        std::fs::read(&self.out)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default()
    }

    fn running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Waits up to `grace` (killing it after): `(exit code, output)`.
    async fn finish(mut self, grace: Duration) -> (Option<i32>, String) {
        let deadline = Instant::now() + grace;
        let code = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(st) => break st.code(),
                None if Instant::now() >= deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break None;
                }
                None => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };
        let out = self.output();
        let _ = std::fs::remove_file(&self.out);
        (code, out)
    }

    /// Polls until the output contains `marker`.
    async fn wait_for(&mut self, marker: &str, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.output().contains(marker) {
                return true;
            }
            if !self.running() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        false
    }
}

/// The overall summary counter `key` (e.g. `settled`).
fn summary_count(out: &str, key: &str) -> u64 {
    let line = out
        .lines()
        .find(|l| l.starts_with("Publish summary:"))
        .unwrap_or_else(|| panic!("no 'Publish summary' line\n{out}"));
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {key}= in {line:?}"))
}

// --------------------------------------------------------------------------
// TCP proxy (link loss)
// --------------------------------------------------------------------------

/// Forwards to `upstream`; `cut()` drops the connections open at that moment
/// while still accepting new ones (the publisher's reconnect). `freeze()`
/// blackholes the connections open at that moment instead: both sockets stay
/// open, bytes are read and discarded in both directions (no heartbeats, no
/// dispositions — a hung broker or a silent network path); new connections
/// forward normally.
struct Proxy {
    port: u16,
    generation: watch::Sender<u64>,
    freeze_epoch: watch::Sender<u64>,
}

/// Copies `r` -> `w` until EOF, discarding instead once `frozen` moves off
/// `born`.
async fn pump(
    mut r: tokio::net::tcp::OwnedReadHalf,
    mut w: tokio::net::tcp::OwnedWriteHalf,
    frozen: watch::Receiver<u64>,
    born: u64,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if *frozen.borrow() != born {
            continue; // blackholed
        }
        if w.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let (generation, gen_rx) = watch::channel(0u64);
        let (freeze_epoch, freeze_rx) = watch::channel(0u64);
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let (upstream, mut gen_rx) = (upstream.clone(), gen_rx.clone());
                let freeze_rx = freeze_rx.clone();
                let born = *gen_rx.borrow_and_update();
                let frozen_born = *freeze_rx.borrow();
                tokio::spawn(async move {
                    let Ok(outbound) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let (ir, iw) = inbound.into_split();
                    let (or, ow) = outbound.into_split();
                    tokio::select! {
                        _ = pump(ir, ow, freeze_rx.clone(), frozen_born) => {}
                        _ = pump(or, iw, freeze_rx, frozen_born) => {}
                        _ = gen_rx.wait_for(|g| *g != born) => {}
                    }
                });
            }
        });
        Self {
            port,
            generation,
            freeze_epoch,
        }
    }

    fn cut(&self) {
        self.generation.send_modify(|g| *g += 1);
    }

    fn freeze(&self) {
        self.freeze_epoch.send_modify(|g| *g += 1);
    }
}

/// `(url via the proxy port, upstream host:port)`.
fn via_proxy(url: &str, port: u16) -> (String, String) {
    let mut u = url::Url::parse(url).expect("SENZING_ACTIVEMQ_URL parses");
    let upstream = format!(
        "{}:{}",
        u.host_str().expect("broker host"),
        u.port().unwrap_or(5672)
    );
    u.set_host(Some("127.0.0.1")).expect("set host");
    u.set_port(Some(port)).expect("set port");
    (u.to_string(), upstream)
}

// ==========================================================================
// Tests
// ==========================================================================

/// A Spark-style directory (plain + gzip + bzip2 parts, .crc, _SUCCESS,
/// blank lines), --parallel: every record exactly once, durable, and
/// byte-identical after the consumer's decode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_directory_of_mixed_compression_parts() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.directory;
    let queue = unique(&case.queue);
    let dir = Scratch::new("dir");
    let n = case.records_per_part;
    let parts = [
        records(&fx.record_template, "PUB_PLAIN", 0..n),
        records(&fx.record_template, "PUB_GZ", 0..n),
        records(&fx.record_template, "PUB_BZ2", 0..n),
    ];
    dir.jsonl("part-00000-a.json", &parts[0], case.blank_every);
    let plain1 = dir.jsonl("tmp1", &parts[1], case.blank_every);
    let plain2 = dir.jsonl("tmp2", &parts[2], case.blank_every);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(&std::fs::read(&plain1).unwrap()).unwrap();
    std::fs::write(dir.0.join("part-00001-a.json.gz"), gz.finish().unwrap()).unwrap();
    let mut bz = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    bz.write_all(&std::fs::read(&plain2).unwrap()).unwrap();
    std::fs::write(dir.0.join("part-00002-a.json.bz2"), bz.finish().unwrap()).unwrap();
    std::fs::remove_file(plain1).unwrap();
    std::fs::remove_file(plain2).unwrap();
    std::fs::write(dir.0.join(".part-00000-a.json.crc"), b"not a record").unwrap();
    std::fs::write(dir.0.join("_SUCCESS"), b"").unwrap();

    let parallel = case.parallel.to_string();
    let dir_arg = dir.0.to_string_lossy().into_owned();
    let publisher = Publisher::spawn(
        &["-q", &queue, "-p", &parallel, &dir_arg],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(2),
        Duration::from_secs(case.drained_within_secs),
    )
    .await;

    assert_eq!(code, Some(0), "publisher exit\n{out}");
    let expected: BTreeSet<&str> = parts.iter().flatten().map(String::as_str).collect();
    assert_eq!(
        summary_count(&out, "settled"),
        expected.len() as u64,
        "{out}"
    );
    assert_eq!(summary_count(&out, "read"), expected.len() as u64, "{out}");
    assert_eq!(summary_count(&out, "failed"), 0, "{out}");
    assert_eq!(drained.len(), expected.len(), "exactly once\n{out}");
    let seen: BTreeSet<&str> = drained.iter().map(|d| d.body.as_str()).collect();
    assert_eq!(seen, expected, "every record, byte-identical");
    assert!(drained.iter().all(|d| d.durable), "durable by default");
    eprintln!(
        "e2e_publisher_directory: {} records from 3 parts",
        drained.len()
    );
}

/// Records from 1 KB to 150 MB (a real corpus's largest) interleaved, through a
/// 64 KiB client max-frame-size, so every record over it is SEVERAL transfer
/// frames: delivery-ids must stay contiguous — every record exactly once and
/// the broker never closes the link. Regression for Artemis closing it with
/// "Expected delivery-id N, got N+k": upstream fe2o3-amqp numbered deliveries
/// with the per-FRAME transfer-id (fails on stock 0.18.3; passes on the fork).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_multi_frame_records_keep_delivery_ids_contiguous() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.large;
    let queue = unique(&case.queue);
    let dir = Scratch::new("large");
    let lines: Vec<String> = (0..case.count)
        .map(|i| {
            let pad = "x".repeat(case.pad_bytes[i % case.pad_bytes.len()]);
            case.record_template
                .replace("{tag}", "PUB_LARGE")
                .replace("{i}", &i.to_string())
                .replace("{pad}", &pad)
        })
        .collect();
    let file = dir.jsonl("part-00000-large.json", &lines, 0);

    let max_pending = case.max_pending.to_string();
    let max_frame = case.client_max_frame_size.to_string();
    let file_arg = file.to_string_lossy().into_owned();
    let publisher = Publisher::spawn(
        &[
            "-q",
            &queue,
            "-m",
            &max_pending,
            "--max-frame-size",
            &max_frame,
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drain_started = Instant::now();
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(case.drain_idle_secs),
        Duration::from_secs(case.drained_within_secs),
    )
    .await;

    assert_eq!(code, Some(0), "publisher exit\n{out}");
    assert!(
        !out.contains("broker link lost"),
        "the broker closed the link on a multi-frame record\n{out}"
    );
    assert_eq!(summary_count(&out, "reconnects"), 0, "{out}");
    assert_eq!(summary_count(&out, "resent"), 0, "{out}");
    assert_eq!(summary_count(&out, "failed"), 0, "{out}");
    assert_eq!(summary_count(&out, "settled"), case.count as u64, "{out}");
    let counts = tally(&drained);
    let dups: Vec<usize> = counts.values().copied().filter(|&n| n != 1).collect();
    assert!(dups.is_empty(), "duplicates on the queue: {dups:?}");
    let expected: BTreeSet<&str> = lines.iter().map(String::as_str).collect();
    let seen: BTreeSet<&str> = counts.keys().copied().collect();
    assert_eq!(seen.len(), expected.len(), "record count on the queue");
    assert!(seen == expected, "every record, byte-identical");
    eprintln!(
        "e2e_publisher_multi_frame: {} records (largest {} bytes), 0 reconnects, \
         drained in {:.1}s",
        drained.len(),
        lines.iter().map(String::len).max().unwrap_or(0),
        drain_started.elapsed().as_secs_f64()
    );
}

/// addressFullMessagePolicy=BLOCK: the publisher blocks on credit (logs it,
/// no reconnect, no resend), stays blocked while nobody consumes, and
/// completes once the queue drains — every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_blocks_on_a_full_address_without_duplicates() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.blocked;
    let queue = unique(&case.queue);
    block_address(&queue, case.max_size_bytes, case.page_size_bytes);
    let dir = Scratch::new("blocked");
    let lines = records(&fx.record_template, "PUB_BLOCKED", 0..case.count);
    let file = dir.jsonl("records.jsonl", &lines, 0);
    let interval = case.blocked_log_interval_secs.to_string();
    let file_arg = file.to_string_lossy().into_owned();
    let mut publisher = Publisher::spawn(
        &["-q", &queue, "--blocked-log-interval", &interval, &file_arg],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );

    let blocked = publisher
        .wait_for(
            &case.blocked_marker,
            Duration::from_secs(case.blocked_within_secs),
        )
        .await;
    let before = queue_count(&queue).unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(case.hold_blocked_secs)).await;
    let after = queue_count(&queue).unwrap_or(0);
    let still_running = publisher.running();

    let (stop_tx, stop) = watch::channel(false);
    let drainer = tokio::spawn({
        let (url, queue) = (url.clone(), queue.clone());
        async move {
            drain(
                &url,
                &queue,
                stop,
                Duration::from_secs(3),
                Duration::from_secs(300),
            )
            .await
        }
    });
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    stop_tx.send_replace(true);
    let drained = drainer.await.expect("drainer");
    remove_address_settings(&queue);

    assert!(blocked, "publisher never reported blocking\n{out}");
    assert!(still_running, "publisher must wait, not give up\n{out}");
    assert!(
        after < case.count as u64 && after == before,
        "queue must plateau while blocked: {before} -> {after} of {}",
        case.count
    );
    assert_eq!(code, Some(0), "publisher exit\n{out}");
    assert_eq!(summary_count(&out, "settled"), case.count as u64, "{out}");
    assert_eq!(summary_count(&out, "resent"), 0, "no resend\n{out}");
    assert_eq!(summary_count(&out, "reconnects"), 0, "no reconnect\n{out}");
    let seen = tally(&drained);
    assert_eq!(drained.len(), case.count, "exactly once (no duplicates)");
    assert!(seen.values().all(|&c| c == 1));
    assert!(lines.iter().all(|l| seen.contains_key(l.as_str())));
    eprintln!(
        "e2e_publisher_blocks: plateaued at {after}, then all {} delivered once",
        case.count
    );
}

/// A link loss while blocked: reconnect, resend the unsettled records, and
/// lose nothing (at-least-once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_link_loss_reconnects_and_loses_nothing() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.link_loss;
    let queue = unique(&case.queue);
    block_address(&queue, case.max_size_bytes, case.page_size_bytes);
    let dir = Scratch::new("linkloss");
    let lines = records(&fx.record_template, "PUB_LINKLOSS", 0..case.count);
    let file = dir.jsonl("records.jsonl", &lines, 0);
    let (_, upstream) = via_proxy(&url, 0);
    let proxy = Proxy::start(upstream).await;
    let (proxied, _) = via_proxy(&url, proxy.port);
    let file_arg = file.to_string_lossy().into_owned();
    let mut publisher = Publisher::spawn(
        &[
            "-q",
            &queue,
            "--blocked-log-interval",
            "1",
            "--retry-delay",
            "1",
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", proxied.as_str())],
    );
    let blocked = publisher
        .wait_for(
            &case.blocked_marker,
            Duration::from_secs(case.blocked_within_secs),
        )
        .await;
    proxy.cut();
    let reconnected = publisher
        .wait_for(&case.reconnect_marker, Duration::from_secs(30))
        .await;

    let (stop_tx, stop) = watch::channel(false);
    let drainer = tokio::spawn({
        let (url, queue) = (url.clone(), queue.clone());
        async move {
            drain(
                &url,
                &queue,
                stop,
                Duration::from_secs(3),
                Duration::from_secs(300),
            )
            .await
        }
    });
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    stop_tx.send_replace(true);
    let drained = drainer.await.expect("drainer");
    remove_address_settings(&queue);

    assert!(blocked, "never blocked before the cut\n{out}");
    assert!(reconnected, "link loss not reported\n{out}");
    assert_eq!(code, Some(0), "publisher exit\n{out}");
    assert!(summary_count(&out, "reconnects") >= 1, "{out}");
    assert_eq!(summary_count(&out, "settled"), case.count as u64, "{out}");
    let seen = tally(&drained);
    let missing: Vec<_> = lines
        .iter()
        .filter(|l| !seen.contains_key(l.as_str()))
        .collect();
    assert!(missing.is_empty(), "{} records lost\n{out}", missing.len());
    assert_eq!(seen.len(), case.count, "no foreign records");
    eprintln!(
        "e2e_publisher_link_loss: {} unique, {} duplicate deliveries",
        seen.len(),
        drained.len() - seen.len()
    );
}

/// The connection goes silent mid-run (proxy blackholes it: no dispositions,
/// no heartbeats, socket still open) while the publisher awaits settlements.
/// It must notice via the connection idle timeout, reconnect, resend and
/// finish — never hang. Regression: the settlement future of an unsettled
/// transfer is never completed when the connection stops (fe2o3-amqp keeps
/// the outcome senders in the link's unsettled map, owned by the still-alive
/// `Sender`), so the publisher waited forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_silent_connection_reconnects_never_hangs() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.frozen;
    let queue = unique(&case.queue);
    let dir = Scratch::new("frozen");
    let lines = records(&fx.record_template, "PUB_FROZEN", 0..case.count);
    let file = dir.jsonl("records.jsonl", &lines, 0);
    let (_, upstream) = via_proxy(&url, 0);
    let proxy = Proxy::start(upstream).await;
    let (proxied, _) = via_proxy(&url, proxy.port);
    let file_arg = file.to_string_lossy().into_owned();
    let max_pending = case.max_pending.to_string();
    let mut publisher = Publisher::spawn(
        &[
            "-q",
            &queue,
            "-m",
            &max_pending,
            "--blocked-log-interval",
            "5",
            "--retry-delay",
            "1",
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", proxied.as_str())],
    );
    let started = publisher
        .wait_for(&case.started_marker, Duration::from_secs(30))
        .await;
    tokio::time::sleep(Duration::from_millis(case.freeze_after_ms)).await;
    let mid_run = publisher.running();
    proxy.freeze();
    let reconnected = publisher
        .wait_for(
            &case.reconnect_marker,
            Duration::from_secs(case.reconnect_within_secs),
        )
        .await;
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(2),
        Duration::from_secs(120),
    )
    .await;

    assert!(started, "publisher never started\n{out}");
    assert!(mid_run, "publisher finished before the freeze\n{out}");
    assert!(
        reconnected,
        "silent connection not detected within {}s (hung)\n{out}",
        case.reconnect_within_secs
    );
    assert_eq!(code, Some(0), "publisher exit\n{out}");
    assert!(summary_count(&out, "reconnects") >= 1, "{out}");
    assert_eq!(summary_count(&out, "settled"), case.count as u64, "{out}");
    let seen = tally(&drained);
    let missing = lines
        .iter()
        .filter(|l| !seen.contains_key(l.as_str()))
        .count();
    assert_eq!(missing, 0, "records lost\n{out}");
    assert_eq!(seen.len(), case.count, "no foreign records");
    eprintln!(
        "e2e_publisher_silent_connection: {} unique, {} duplicate deliveries",
        seen.len(),
        drained.len() - seen.len()
    );
}

/// A record that would need a second transfer frame is FAILED unsent (file,
/// line, size logged; exit 2) and does not disturb the rest of the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_record_over_side_max_fails_loudly_unsent() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.oversize;
    let queue = unique(&case.queue);
    let dir = Scratch::new("oversize");
    let line = |i: usize, pad: &str| {
        case.record_template
            .replace("{tag}", "PUB_OVERSIZE")
            .replace("{i}", &i.to_string())
            .replace("{pad}", pad)
    };
    let mut lines: Vec<String> = (0..case.small_count).map(|i| line(i, "x")).collect();
    let fixed = line(case.small_count, "").len();
    let big = line(case.small_count, &"x".repeat(case.oversize_bytes - fixed));
    assert_eq!(big.len(), case.oversize_bytes);
    lines.insert(case.oversize_line - 1, big);
    let file = dir.jsonl("part-00000-oversize.json", &lines, 0);
    let big = lines.remove(case.oversize_line - 1);

    let file_arg = file.to_string_lossy().into_owned();
    let side_max = case.side_max_bytes.to_string();
    let publisher = Publisher::spawn(
        &["-q", &queue, "--side-max-bytes", &side_max, &file_arg],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(2),
        Duration::from_secs(case.drained_within_secs),
    )
    .await;
    drop(big);

    assert_eq!(code, Some(2), "FAILED records exit 2\n{out}");
    let failed_line = out
        .lines()
        .find(|l| l.contains(&case.failed_marker))
        .unwrap_or_else(|| panic!("no '{}' line\n{out}", case.failed_marker));
    assert!(
        failed_line.contains(&format!("line {}", case.oversize_line))
            && failed_line.contains(&case.oversize_bytes.to_string()),
        "file:line and size logged: {failed_line}"
    );
    assert_eq!(summary_count(&out, "failed"), 1, "{out}");
    assert_eq!(summary_count(&out, "reconnects"), 0, "{out}");
    assert_eq!(
        summary_count(&out, "settled"),
        case.small_count as u64,
        "{out}"
    );
    let seen: BTreeSet<&str> = drained.iter().map(|d| d.body.as_str()).collect();
    let expected: BTreeSet<&str> = lines.iter().map(String::as_str).collect();
    assert_eq!(drained.len(), case.small_count, "nothing else on the queue");
    assert_eq!(seen, expected, "every small record, exactly once");
    eprintln!(
        "e2e_publisher_oversize: 1 FAILED unsent, {} published",
        drained.len()
    );
}

/// The same oversize record with --side-file: written there (byte-identical,
/// one line), logged `sided <file> line <n> bytes <b>`, NOT published, exit 0;
/// every small record published exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_record_over_side_max_goes_to_the_side_file() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.oversize;
    let queue = unique(&format!("{}-side", case.queue));
    let dir = Scratch::new("side");
    let line = |i: usize, pad: &str| {
        case.record_template
            .replace("{tag}", "PUB_SIDE")
            .replace("{i}", &i.to_string())
            .replace("{pad}", pad)
    };
    let mut lines: Vec<String> = (0..case.small_count).map(|i| line(i, "x")).collect();
    let fixed = line(case.small_count, "").len();
    let big = line(case.small_count, &"x".repeat(case.oversize_bytes - fixed));
    lines.insert(case.oversize_line - 1, big);
    let name = "part-00000-side.json";
    let file = dir.jsonl(name, &lines, 0);
    let big = lines.remove(case.oversize_line - 1);
    let side = dir.0.join("sided.jsonl");

    let (file_arg, side_arg) = (
        file.to_string_lossy().into_owned(),
        side.to_string_lossy().into_owned(),
    );
    let side_max = case.side_max_bytes.to_string();
    let publisher = Publisher::spawn(
        &[
            "-q",
            &queue,
            "--side-max-bytes",
            &side_max,
            "--side-file",
            &side_arg,
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(2),
        Duration::from_secs(case.drained_within_secs),
    )
    .await;

    assert_eq!(code, Some(0), "a sided record is not a failure\n{out}");
    let marker = format!(
        "sided {name} line {} bytes {}",
        case.oversize_line, case.oversize_bytes
    );
    assert!(out.contains(&marker), "no '{marker}' line\n{out}");
    assert_eq!(summary_count(&out, "sided"), 1, "{out}");
    assert_eq!(summary_count(&out, "failed"), 0, "{out}");
    assert_eq!(
        summary_count(&out, "settled"),
        case.small_count as u64,
        "{out}"
    );
    let written = std::fs::read_to_string(&side).expect("side file");
    assert_eq!(
        written,
        format!("{big}\n"),
        "side file holds exactly the oversize record"
    );
    let seen: BTreeSet<&str> = drained.iter().map(|d| d.body.as_str()).collect();
    let expected: BTreeSet<&str> = lines.iter().map(String::as_str).collect();
    assert_eq!(
        drained.len(),
        case.small_count,
        "the oversize record is not on the queue"
    );
    assert_eq!(seen, expected, "every small record, exactly once");
}

/// --resume skips the first N non-blank records of the named file (blank
/// lines do not count), publishes the rest, reports skipped=N, and ends with
/// `fileprogress <file> <total>`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publisher_resume_skips_already_published_records() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.resume;
    let queue = unique(&case.queue);
    let dir = Scratch::new("resume");
    let lines = records(&fx.record_template, "PUB_RESUME", 0..case.count);
    let name = "part-00000-resume.json";
    let file = dir.jsonl(name, &lines, case.blank_every);
    let resume = dir.0.join("resume.txt");
    std::fs::write(&resume, format!("fileprogress {name} {}\n", case.skip)).expect("resume file");

    let (file_arg, resume_arg) = (
        file.to_string_lossy().into_owned(),
        resume.to_string_lossy().into_owned(),
    );
    let publisher = Publisher::spawn(
        &["-q", &queue, "--resume", &resume_arg, &file_arg],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    );
    let (code, out) = publisher
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let (_stop_tx, stop) = watch::channel(true);
    let drained = drain(
        &url,
        &queue,
        stop,
        Duration::from_secs(2),
        Duration::from_secs(case.drained_within_secs),
    )
    .await;

    assert_eq!(code, Some(0), "publisher exit\n{out}");
    assert_eq!(summary_count(&out, "skipped"), case.skip as u64, "{out}");
    assert_eq!(
        summary_count(&out, "settled"),
        (case.count - case.skip) as u64,
        "{out}"
    );
    let last = out
        .lines()
        .rev()
        .find(|l| l.starts_with(&format!("fileprogress {name} ")))
        .unwrap_or_else(|| panic!("no fileprogress line\n{out}"));
    assert_eq!(last, format!("fileprogress {name} {}", case.count), "{out}");
    let seen: BTreeSet<&str> = drained.iter().map(|d| d.body.as_str()).collect();
    let expected: BTreeSet<&str> = lines[case.skip..].iter().map(String::as_str).collect();
    assert_eq!(
        drained.len(),
        case.count - case.skip,
        "only the unpublished tail"
    );
    assert_eq!(seen, expected, "exactly the records after the resume point");
}

/// A broker that accepts TCP but never answers the AMQP header (its event
/// loop wedged — observed on Artemis spinning in proton-j after a protocol
/// error): each connect attempt is bounded, so the publisher exits 1 once
/// --reconnect-timeout passes instead of hanging in the handshake.
#[tokio::test]
async fn e2e_publisher_silent_broker_handshake_is_bounded() {
    if gate().is_none() {
        return;
    }
    let case = fixture().silent_broker;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    // Accept and hold every socket, never writing a byte.
    let holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = listener.accept().await {
            held.push(sock);
        }
    });
    let dir = Scratch::new("silentbroker");
    let file = dir.jsonl("r.jsonl", &["{\"RECORD_ID\":\"1\"}".to_string()], 0);
    let url = format!("amqp://127.0.0.1:{port}");
    let reconnect_timeout = case.reconnect_timeout_secs.to_string();
    let file_arg = file.to_string_lossy().into_owned();
    let started = Instant::now();
    let (code, out) = Publisher::spawn(
        &[
            "-q",
            "sz-amq-pub-silent-broker",
            "--reconnect-timeout",
            &reconnect_timeout,
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    )
    .finish(Duration::from_secs(case.exit_within_secs))
    .await;
    holder.abort();
    assert_eq!(code, Some(1), "silent broker must fail, not hang\n{out}");
    assert!(out.contains("no AMQP handshake"), "{out}");
    eprintln!(
        "e2e_publisher_silent_broker: exit 1 after {:.1}s",
        started.elapsed().as_secs_f64()
    );
}

/// Wrong password: SASL fails fast (exit 1, no retry loop). Unreachable
/// broker: exit 1 once --reconnect-timeout passes. Missing input: exit 1.
#[tokio::test]
async fn e2e_publisher_startup_failures_exit_1() {
    let Some(url) = gate() else { return };
    let fx = fixture();
    let case = &fx.failures;
    let dir = Scratch::new("failures");
    let file = dir.jsonl(
        "one.jsonl",
        &records(&fx.record_template, "PUB_FAIL", 0..1),
        0,
    );
    let file_arg = file.to_string_lossy().into_owned();
    let grace = Duration::from_secs(case.exit_within_secs);

    let started = Instant::now();
    let (code, out) = Publisher::spawn(
        &[
            "-q",
            &unique("sz-amq-pub-fail"),
            "--password",
            &case.bad_password,
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    )
    .finish(grace)
    .await;
    assert_eq!(code, Some(1), "bad credentials\n{out}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "SASL failure must not retry\n{out}"
    );

    let timeout = case.reconnect_timeout_secs.to_string();
    let (code, out) = Publisher::spawn(
        &[
            "-q",
            "q",
            "--retry-delay",
            "1",
            "--reconnect-timeout",
            &timeout,
            &file_arg,
        ],
        &[("SENZING_ACTIVEMQ_URL", case.unreachable_url.as_str())],
    )
    .finish(grace)
    .await;
    assert_eq!(code, Some(1), "unreachable broker\n{out}");
    assert!(out.contains("--reconnect-timeout"), "{out}");

    // A broker whose max-frame-size is below --min-max-frame-size: refuse.
    let floor = case.impossible_min_max_frame_size.to_string();
    let (code, out) = Publisher::spawn(
        &["-q", "q", "--min-max-frame-size", &floor, &file_arg],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    )
    .finish(grace)
    .await;
    assert_eq!(code, Some(1), "max-frame-size below the floor\n{out}");
    assert!(out.contains("--min-max-frame-size"), "{out}");

    let missing = dir.0.join("missing.jsonl").to_string_lossy().into_owned();
    let (code, out) = Publisher::spawn(
        &["-q", "q", &missing],
        &[("SENZING_ACTIVEMQ_URL", url.as_str())],
    )
    .finish(grace)
    .await;
    assert_eq!(code, Some(1), "missing input\n{out}");
}
