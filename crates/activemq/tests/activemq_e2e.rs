//! ActiveMQ Artemis end-to-end tests against a real engine and a real Artemis
//! broker over AMQP 1.0.
//!
//! Gated on `SENZING_ENGINE_CONFIGURATION_JSON`, `SENZING_ACTIVEMQ_URL` (with
//! credentials, e.g. `amqp://artemis:artemis@localhost:5673`) and
//! `IT_ARTEMIS_JOLOKIA_URL` (management, e.g.
//! `http://artemis:artemis@localhost:8161/console/jolokia`: queue counts,
//! since Artemis has no depth verb over AMQP). Without them each test prints
//! `SKIP` and passes — or FAILS when `IT_REQUIRE_INFRA=1` (CI).
//!
//! What is proven here that unit tests cannot (data in `tests/fixtures/`):
//! * records load at redo% 0 / 20, and the pure redoer (100) runs with no
//!   broker settings at all (`load.yaml`);
//! * a Data-section body and an AmqpValue string (JMS TextMessage) both load;
//! * an engine reject and an unparseable body are `rejected` and Artemis moves
//!   both, verbatim, to the dead-letter address (`rejects.yaml`);
//! * shutdown: a stuck worker cannot outlive the deadline and its delivery is
//!   released for redelivery (not dead-lettered); SIGHUP is graceful; losing
//!   the broker connection is fatal (exit 255, orderly); bad credentials and a
//!   bad URL fail startup (`shutdown.yaml`);
//! * `--prefetch` is the TOTAL in-flight cap (`prefetch.yaml`).

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::annotations::OwnedKey;
use fe2o3_amqp::types::messaging::{Body, Message, Modified, Source, Target};
use fe2o3_amqp::types::primitives::{Binary, Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use serde_json::json;
use sz_rust_sdk::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const INSTANCE: &str = "sz_activemq_combined_consumer_it";
/// Annotation Artemis puts on a dead-lettered message: its source queue.
const ORIG_QUEUE_ANNOTATION: &str = "x-opt-ORIG-QUEUE";
/// The same, as a core filter on the dead-letter queue (Jolokia).
const ORIG_QUEUE_PROPERTY: &str = "_AMQ_ORIG_QUEUE";
const DEAD_LETTER_QUEUE: &str = "DLQ";

// --------------------------------------------------------------------------
// Gating
// --------------------------------------------------------------------------

/// Report a skipped test; panic instead when `IT_REQUIRE_INFRA=1` (CI) so a
/// missing broker/engine can never pass silently.
fn skip(reason: std::fmt::Arguments<'_>) {
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

/// The broker URL when the whole rig is configured, else `None` (skip).
fn gate() -> Option<String> {
    for var in [
        "SENZING_ENGINE_CONFIGURATION_JSON",
        "SENZING_ACTIVEMQ_URL",
        "IT_ARTEMIS_JOLOKIA_URL",
    ] {
        if env_nonempty(var).is_none() {
            skip(format_args!("SKIP: {var} not set"));
            return None;
        }
    }
    env_nonempty("SENZING_ACTIVEMQ_URL")
}

fn load_fixture<T: serde::de::DeserializeOwned>(name: &str) -> T {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_norway::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// `name-<pid>`: unique per run against a long-lived broker.
fn unique(name: &str) -> String {
    format!("{name}-{}", std::process::id())
}

fn records(template: &str, tag: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            template
                .replace("{tag}", tag)
                .replace("{i}", &i.to_string())
        })
        .collect()
}

// --------------------------------------------------------------------------
// AMQP client (the test's own producer / consumer)
// --------------------------------------------------------------------------

/// How a test message carries its record.
#[derive(Clone, Copy)]
enum BodyKind {
    /// One AMQP Data section (bytes).
    Data,
    /// An AmqpValue string (what a JMS TextMessage sends).
    Text,
}

struct Broker {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
}

impl Broker {
    async fn connect(url: &str) -> Self {
        let mut connection = Connection::open(unique("sz-amq-e2e"), url)
            .await
            .expect("test client connects to Artemis");
        let session = Session::begin(&mut connection).await.expect("session");
        Self {
            connection,
            session,
        }
    }

    /// Sends `bodies` to the ANYCAST queue `queue` (the `queue` target
    /// capability makes an auto-created address ANYCAST; a plain sender would
    /// create a MULTICAST address the driver could not attach to).
    async fn send(&mut self, queue: &str, bodies: &[String], kind: BodyKind) {
        let target = Target::builder()
            .address(queue)
            .capabilities(vec![Symbol::from("queue")])
            .build();
        let mut sender = Sender::builder()
            .name(unique(&format!("{queue}-sender")))
            .target(target)
            .attach(&mut self.session)
            .await
            .expect("attach sender");
        for b in bodies {
            let outcome = match kind {
                BodyKind::Data => {
                    let data = Binary::from(b.as_bytes().to_vec());
                    sender.send(Message::builder().data(data).build()).await
                }
                BodyKind::Text => {
                    let text = Value::String(b.clone());
                    sender.send(Message::builder().value(text).build()).await
                }
            };
            outcome
                .expect("send")
                .accepted_or("not accepted")
                .expect("broker accepted the message");
        }
        sender.close().await.expect("close sender");
    }

    /// Receives up to `want` messages from `address` within `within`, accepting
    /// those `keep` selects (returned as body text) and handing the rest back
    /// (`modified`, undeliverable here) untouched.
    async fn take(
        &mut self,
        address: &str,
        want: usize,
        within: Duration,
        keep: impl Fn(&Message<Body<Value>>) -> bool,
    ) -> Vec<String> {
        let source = Source::builder()
            .address(address)
            .capabilities(vec![Symbol::from("queue")])
            .build();
        let mut receiver = Receiver::builder()
            .name(unique(&format!("{address}-probe")))
            .source(source)
            .attach(&mut self.session)
            .await
            .expect("attach receiver");
        let deadline = Instant::now() + within;
        let mut got = Vec::new();
        while got.len() < want {
            let left = deadline.saturating_duration_since(Instant::now());
            let Ok(next) = tokio::time::timeout(left, receiver.recv::<Body<Value>>()).await else {
                break;
            };
            let delivery = next.expect("receive");
            if keep(delivery.message()) {
                got.push(body_text(delivery.body()));
                receiver.accept(&delivery).await.expect("accept");
            } else {
                let elsewhere = Modified {
                    delivery_failed: None,
                    undeliverable_here: Some(true),
                    message_annotations: None,
                };
                receiver.modify(&delivery, elsewhere).await.expect("modify");
            }
        }
        receiver.close().await.expect("close receiver");
        got
    }

    /// Up to `want` bodies from `queue`.
    async fn drain(&mut self, queue: &str, want: usize, within: Duration) -> Vec<String> {
        self.take(queue, want, within, |_| true).await
    }

    /// Up to `want` bodies dead-lettered from `source_queue`.
    async fn dead_lettered(
        &mut self,
        source_queue: &str,
        want: usize,
        within: Duration,
    ) -> Vec<String> {
        let key = OwnedKey::Symbol(Symbol::from(ORIG_QUEUE_ANNOTATION));
        self.take(DEAD_LETTER_QUEUE, want, within, |m| {
            m.message_annotations
                .as_ref()
                .and_then(|a| a.0.get(&key))
                .is_some_and(|v| *v == Value::String(source_queue.to_string()))
        })
        .await
    }

    async fn close(mut self) {
        let _ = self.session.end().await;
        let _ = self.connection.close().await;
    }
}

fn body_text(body: &Body<Value>) -> String {
    match body {
        Body::Data(batch) => batch
            .iter()
            .map(|d| String::from_utf8_lossy(&d.0).into_owned())
            .collect(),
        Body::Value(v) => match &v.0 {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        },
        other => format!("{other:?}"),
    }
}

// --------------------------------------------------------------------------
// Jolokia (Artemis management): queue counts
// --------------------------------------------------------------------------

fn jolokia(request: &serde_json::Value) -> serde_json::Value {
    let url = env_nonempty("IT_ARTEMIS_JOLOKIA_URL").expect("gated on IT_ARTEMIS_JOLOKIA_URL");
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

/// MBean name of the ANYCAST queue `queue` on address `queue`.
fn queue_mbean(queue: &str) -> String {
    let brokers =
        jolokia(&json!({"type": "search", "mbean": "org.apache.activemq.artemis:broker=*"}));
    let broker = brokers["value"][0]
        .as_str()
        .and_then(|n| n.split_once("broker="))
        .map(|(_, b)| b.to_string())
        .unwrap_or_else(|| panic!("no Artemis broker MBean: {brokers}"));
    format!(
        "org.apache.activemq.artemis:broker={broker},component=addresses,address=\"{queue}\",\
         subcomponent=queues,routing-type=\"anycast\",queue=\"{queue}\""
    )
}

/// `(messages, delivering)` on `queue`; `MessageCount` includes deliveries
/// not yet settled. `None` while the queue does not exist.
fn queue_counts(queue: &str) -> Option<(u64, u64)> {
    let resp = jolokia(&json!({
        "type": "read",
        "mbean": queue_mbean(queue),
        "attribute": ["MessageCount", "DeliveringCount"],
    }));
    let v = resp.get("value").filter(|_| resp["status"] == 200)?;
    Some((
        v["MessageCount"].as_u64().unwrap_or(0),
        v["DeliveringCount"].as_u64().unwrap_or(0),
    ))
}

/// Messages on the dead-letter queue that came from `source_queue`.
fn dead_letter_count(source_queue: &str) -> u64 {
    let resp = jolokia(&json!({
        "type": "exec",
        "mbean": queue_mbean(DEAD_LETTER_QUEUE),
        "operation": "countMessages(java.lang.String)",
        "arguments": [format!("{ORIG_QUEUE_PROPERTY}='{source_queue}'")],
    }));
    resp["value"].as_u64().unwrap_or(0)
}

/// Polls until `queue` holds no message (none ready, none unsettled).
async fn wait_queue_empty(queue: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if queue_counts(queue).is_some_and(|(messages, _)| messages == 0) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

/// Polls until `queue` has exactly `n` delivering (received, unsettled).
async fn wait_delivering(queue: &str, n: u64, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if queue_counts(queue).is_some_and(|(_, delivering)| delivering == n) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

// --------------------------------------------------------------------------
// Driver process
// --------------------------------------------------------------------------

fn driver_bin() -> &'static str {
    env!("CARGO_BIN_EXE_sz_activemq_combined_consumer")
}

/// A spawned driver whose stdout/stderr go to temp files.
struct Driver {
    child: std::process::Child,
    out: std::path::PathBuf,
}

impl Driver {
    /// Spawns the driver on `queue` (2 threads, redo% 0); `envs` win.
    fn spawn(queue: &str, args: &[&str], envs: &[(&str, String)]) -> Self {
        let out = std::env::temp_dir().join(format!("{}.out", unique(&format!("sz-amq-{queue}"))));
        let stdout = std::fs::File::create(&out).expect("stdout file");
        let stderr = std::fs::File::create(out.with_extension("err")).expect("stderr file");
        let child = Command::new(driver_bin())
            .args(args)
            .env("SENZING_ACTIVEMQ_QUEUE", queue)
            .env("SENZING_THREADS_PER_PROCESS", "2")
            .env("SENZING_REDO_PERCENT", "0")
            .env_remove("SENZING_INPUT_FILE")
            .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn activemq driver");
        Self { child, out }
    }

    fn signal(&self, sig: libc::c_int) {
        // kill(2) directly: the CI container has no `kill` executable on PATH.
        let rc = unsafe { libc::kill(self.child.id() as libc::pid_t, sig) };
        assert_eq!(rc, 0, "kill({sig}): {}", std::io::Error::last_os_error());
    }

    fn stdout(&self) -> String {
        std::fs::read(&self.out)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default()
    }

    /// Waits up to `grace` for exit (killing it after); returns the status
    /// and the captured `(stdout, stderr)`, files removed.
    async fn finish(
        mut self,
        grace: Duration,
    ) -> (Option<std::process::ExitStatus>, String, String) {
        let deadline = Instant::now() + grace;
        let status = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(st) => break Some(st),
                None if Instant::now() >= deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break None;
                }
                None => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };
        let stdout = self.stdout();
        let err_path = self.out.with_extension("err");
        let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
        let _ = std::fs::remove_file(&self.out);
        let _ = std::fs::remove_file(err_path);
        (status, stdout, stderr)
    }
}

/// The add count from the driver's final `Processed total of N adds ...` line.
fn final_total(stdout: &str, marker: &str) -> usize {
    let line = stdout
        .lines()
        .find(|l| l.starts_with(marker))
        .unwrap_or_else(|| panic!("driver never printed {marker:?}\n{stdout}"));
    line.trim_start_matches(marker)
        .split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("could not parse add count from {line:?}"))
}

/// Example record-transform plugin cdylib (built as a dev-dependency into the
/// same `deps/` directory as this test executable).
fn example_transform_plugin() -> String {
    let deps = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("deps dir")
        .to_path_buf();
    let p = deps.join(format!(
        "{}sz_record_transform_example.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ));
    assert!(p.exists(), "example transform plugin not built at {p:?}");
    p.to_string_lossy().into_owned()
}

/// Env making every load record sleep `sleep_ms` in the worker (plugin).
fn sleep_plugin_env(sleep_ms: u64) -> Vec<(&'static str, String)> {
    vec![
        (
            "SENZING_RECORD_TRANSFORM_PLUGIN",
            example_transform_plugin(),
        ),
        (
            "SENZING_RECORD_TRANSFORM_CONFIG",
            format!(r#"{{"SLEEP_MS":{sleep_ms}}}"#),
        ),
    ]
}

// ==========================================================================
// LOAD / REDO SHARE / BODY TYPES — tests/fixtures/load.yaml
// ==========================================================================

#[derive(serde::Deserialize)]
struct LoadFixture {
    final_total: String,
    combined: CombinedCase,
    redoer: RedoerCase,
    body_types: BodyTypesCase,
}

#[derive(serde::Deserialize)]
struct CombinedCase {
    queue: String,
    threads: usize,
    count: usize,
    redo_sleep_secs: u64,
    drained_within_secs: u64,
    record_template: String,
}

#[derive(serde::Deserialize)]
struct RedoerCase {
    seed_count: usize,
    started: String,
    started_within_secs: u64,
    threads: usize,
    redo_sleep_secs: u64,
    drained_within_secs: u64,
    record_template: String,
}

#[derive(serde::Deserialize)]
struct BodyTypesCase {
    queue: String,
    loaded_within_secs: u64,
    data_record: String,
    value_record: String,
}

/// Publishes N records, runs the driver at `redo_percent`, waits for the queue
/// to empty, then SIGTERMs and asserts a clean exit that loaded all N.
async fn run_combined(redo_percent: u8) {
    let Some(url) = gate() else { return };
    let fx: LoadFixture = load_fixture("load.yaml");
    let case = &fx.combined;
    let queue = unique(&format!("{}-{redo_percent}", case.queue));
    let batch = records(
        &case.record_template,
        &format!("AMQ_COMBINED_{redo_percent}"),
        case.count,
    );
    let mut broker = Broker::connect(&url).await;
    broker.send(&queue, &batch, BodyKind::Data).await;

    let driver = Driver::spawn(
        &queue,
        &[],
        &[
            ("SENZING_REDO_PERCENT", redo_percent.to_string()),
            ("SENZING_THREADS_PER_PROCESS", case.threads.to_string()),
            (
                "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
                case.redo_sleep_secs.to_string(),
            ),
        ],
    );
    let drained = wait_queue_empty(&queue, Duration::from_secs(case.drained_within_secs)).await;
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver.finish(Duration::from_secs(30)).await;
    broker.close().await;

    assert!(
        drained,
        "queue did not drain (redo%={redo_percent})\n{stdout}\n{stderr}"
    );
    let status = status.expect("driver did not exit after SIGTERM");
    assert!(
        status.success(),
        "driver exited {status:?}\n{stdout}\n{stderr}"
    );
    assert_eq!(
        final_total(&stdout, &fx.final_total),
        case.count,
        "every record loaded (redo%={redo_percent})\n{stdout}"
    );
    eprintln!(
        "e2e activemq redo%={redo_percent}: loaded {} records",
        case.count
    );
}

/// 0% = pure consumer endpoint: no redo fetcher, load-only.
#[tokio::test]
async fn e2e_activemq_combined_pure_consumer_0pct() {
    run_combined(0).await;
}

/// 20% = mixed scheduler (4 threads: 1 redo-preferring + 3 load-preferring).
#[tokio::test]
async fn e2e_activemq_combined_mixed_20pct() {
    run_combined(20).await;
}

/// 100% = pure redoer: no broker settings at all (the binary never opens
/// AMQP). Redo is seeded through the real engine; the redoer must drain it.
#[tokio::test]
async fn e2e_activemq_pure_redoer_100pct() {
    let Some(_) = gate() else { return };
    let fx: LoadFixture = load_fixture("load.yaml");
    let case = &fx.redoer;
    let engine_config = env_nonempty("SENZING_ENGINE_CONFIGURATION_JSON").expect("gated");
    let env: Arc<SzEnvironmentCore> =
        SzEnvironmentCore::get_instance(INSTANCE, &engine_config, false)
            .expect("initialize Senzing environment");
    let engine = env.get_engine().expect("engine handle");
    for (i, rec) in records(&case.record_template, "AMQ_REDO_SEED", case.seed_count)
        .iter()
        .enumerate()
    {
        let _ = engine.add_record(
            "TEST",
            &format!("AMQ_REDO_SEED_{i}"),
            rec,
            Some(SzFlags::ADD_RECORD_DEFAULT_FLAGS),
        );
    }

    let mut driver_cmd = Command::new(driver_bin());
    let out = std::env::temp_dir().join(format!("{}.out", unique("sz-amq-redoer")));
    let child = driver_cmd
        .env("SENZING_REDO_PERCENT", "100")
        .env("SENZING_THREADS_PER_PROCESS", case.threads.to_string())
        .env(
            "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
            case.redo_sleep_secs.to_string(),
        )
        .env_remove("SENZING_ACTIVEMQ_URL")
        .env_remove("SENZING_ACTIVEMQ_QUEUE")
        .env_remove("SENZING_INPUT_FILE")
        .stdout(Stdio::from(std::fs::File::create(&out).expect("stdout")))
        .stderr(Stdio::from(
            std::fs::File::create(out.with_extension("err")).expect("stderr"),
        ))
        .spawn()
        .expect("spawn pure redoer");
    let driver = Driver { child, out };

    // SIGTERM before the handler is installed would kill it outright.
    let deadline = Instant::now() + Duration::from_secs(case.started_within_secs);
    while !driver.stdout().contains(&case.started) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(case.drained_within_secs);
    while Instant::now() < deadline && engine.count_redo_records().unwrap_or(-1) != 0 {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver.finish(Duration::from_secs(30)).await;

    let status = status.expect("pure redoer did not exit after SIGTERM");
    assert!(
        status.success(),
        "pure redoer exited {status:?}\n{stdout}\n{stderr}"
    );
    assert_eq!(
        engine.count_redo_records().unwrap_or(-1),
        0,
        "redo backlog must drain to 0\n{stdout}"
    );
    eprintln!("e2e_activemq_pure_redoer_100pct: redo drained, clean shutdown");
}

/// Contract: a Data-section body (bytes) and an AmqpValue string (JMS
/// TextMessage) are both accepted as the record JSON.
#[tokio::test]
async fn e2e_activemq_data_and_text_bodies_both_load() {
    let Some(url) = gate() else { return };
    let fx: LoadFixture = load_fixture("load.yaml");
    let case = &fx.body_types;
    let queue = unique(&case.queue);
    let mut broker = Broker::connect(&url).await;
    broker
        .send(
            &queue,
            std::slice::from_ref(&case.data_record),
            BodyKind::Data,
        )
        .await;
    broker
        .send(
            &queue,
            std::slice::from_ref(&case.value_record),
            BodyKind::Text,
        )
        .await;

    let driver = Driver::spawn(&queue, &[], &[]);
    let drained = wait_queue_empty(&queue, Duration::from_secs(case.loaded_within_secs)).await;
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver.finish(Duration::from_secs(30)).await;
    broker.close().await;

    assert!(drained, "queue did not drain\n{stdout}\n{stderr}");
    assert!(status.expect("exit").success(), "{stdout}\n{stderr}");
    assert_eq!(final_total(&stdout, &fx.final_total), 2, "{stdout}");
    assert!(
        !stdout.contains("REJECTING:"),
        "nothing may be rejected\n{stdout}"
    );
    assert_eq!(dead_letter_count(&queue), 0, "nothing dead-lettered");
}

// ==========================================================================
// REJECTS — tests/fixtures/rejects.yaml
// ==========================================================================

#[derive(serde::Deserialize)]
struct RejectsFixture {
    queue: String,
    dead_letter_queue: String,
    valid_count: usize,
    valid_template: String,
    unparseable: String,
    engine_reject: String,
    dlq_within_secs: u64,
    settle_within_secs: u64,
    markers: RejectMarkers,
}

#[derive(serde::Deserialize)]
struct RejectMarkers {
    final_total: String,
    rejected: String,
    engine_reject: String,
    poison_warn: String,
    poison_reject: String,
}

/// An engine reject and an unparseable body are `rejected`; Artemis moves both
/// verbatim to the dead-letter address. The final line counts only the adds
/// and reports 2 rejected; each reject carries the unified marker.
#[tokio::test]
async fn e2e_activemq_rejects_land_on_the_dead_letter_address() {
    let Some(url) = gate() else { return };
    let fx: RejectsFixture = load_fixture("rejects.yaml");
    assert_eq!(fx.dead_letter_queue, DEAD_LETTER_QUEUE);
    let queue = unique(&fx.queue);
    let mut all = records(&fx.valid_template, "", fx.valid_count);
    all.push(fx.engine_reject.clone());
    all.push(fx.unparseable.clone());
    let mut broker = Broker::connect(&url).await;
    broker.send(&queue, &all, BodyKind::Data).await;

    let driver = Driver::spawn(&queue, &[], &[]);
    let dlq_within = Duration::from_secs(fx.dlq_within_secs);
    let deadline = Instant::now() + dlq_within;
    while dead_letter_count(&queue) < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let settled = wait_queue_empty(&queue, Duration::from_secs(fx.settle_within_secs)).await;
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver.finish(Duration::from_secs(30)).await;
    let mut rejects = broker.dead_lettered(&queue, 2, dlq_within).await;
    broker.close().await;

    assert!(status.expect("exit").success(), "{stdout}\n{stderr}");
    assert!(settled, "source queue must be fully settled\n{stdout}");
    for marker in [
        &fx.markers.poison_warn,
        &fx.markers.poison_reject,
        &fx.markers.engine_reject,
    ] {
        assert!(
            stdout.contains(marker.as_str()),
            "missing {marker:?}\n{stdout}"
        );
    }
    rejects.sort();
    let mut want = vec![fx.engine_reject.clone(), fx.unparseable.clone()];
    want.sort();
    assert_eq!(rejects, want, "dead-lettered content mismatch\n{stdout}");
    let total_prefix = fx
        .markers
        .final_total
        .replace("{N}", &fx.valid_count.to_string());
    let total = stdout
        .lines()
        .find(|l| l.starts_with(&total_prefix))
        .unwrap_or_else(|| panic!("expected {total_prefix:?}\n{stdout}"));
    assert!(total.contains(&fx.markers.rejected), "{total:?}\n{stdout}");
    eprintln!("e2e_activemq_rejects_land_on_the_dead_letter_address: {total}");
}

// ==========================================================================
// SHUTDOWN / FAILURES — tests/fixtures/shutdown.yaml
// ==========================================================================

#[derive(serde::Deserialize)]
struct ShutdownFixture {
    markers: Markers,
    sighup: SighupCase,
    deadline: DeadlineCase,
    link_loss: LinkLossCase,
    startup: StartupCase,
}

#[derive(serde::Deserialize)]
struct Markers {
    final_total: String,
    sighup: String,
    left_for_redelivery: String,
    link_lost: String,
    connect_failed: String,
}

#[derive(serde::Deserialize)]
struct SighupCase {
    queue: String,
    exit_within_secs: u64,
    records: Vec<String>,
}

#[derive(serde::Deserialize)]
struct DeadlineCase {
    queue: String,
    sleep_ms: u64,
    settle_secs: u64,
    exit_within_secs: u64,
    redelivered_within_secs: u64,
    record: String,
}

#[derive(serde::Deserialize)]
struct LinkLossCase {
    queue: String,
    sleep_ms: u64,
    settle_secs: u64,
    exit_code: i32,
    exit_within_secs: u64,
    redelivered_within_secs: u64,
    record: String,
}

#[derive(serde::Deserialize)]
struct StartupCase {
    queue: String,
    wrong_password: String,
    bad_url: String,
    bad_url_marker: String,
    exit_within_secs: u64,
}

fn shutdown_fixture() -> ShutdownFixture {
    load_fixture("shutdown.yaml")
}

/// SIGHUP is a graceful shutdown: drain, exit 0, final total printed.
#[tokio::test]
async fn e2e_activemq_sighup_is_graceful() {
    let Some(url) = gate() else { return };
    let fx = shutdown_fixture();
    let case = &fx.sighup;
    let queue = unique(&case.queue);
    let mut broker = Broker::connect(&url).await;
    broker.send(&queue, &case.records, BodyKind::Data).await;

    let driver = Driver::spawn(&queue, &[], &[]);
    let drained = wait_queue_empty(&queue, Duration::from_secs(120)).await;
    driver.signal(libc::SIGHUP);
    let (status, stdout, stderr) = driver
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    broker.close().await;

    assert!(drained, "queue did not drain\n{stdout}\n{stderr}");
    let status = status.expect("driver did not exit after SIGHUP");
    assert!(status.success(), "exit {status:?}\n{stdout}\n{stderr}");
    assert!(stdout.contains(&fx.markers.sighup), "{stdout}");
    assert_eq!(
        final_total(&stdout, &fx.markers.final_total),
        case.records.len(),
        "{stdout}"
    );
}

/// A worker blocked far past the 10 s grace must not keep the process alive:
/// SIGTERM -> exit within the bound. The delivery is released (not
/// dead-lettered) and receivable again.
#[tokio::test]
async fn e2e_activemq_stuck_worker_cannot_outlive_shutdown_deadline() {
    let Some(url) = gate() else { return };
    let fx = shutdown_fixture();
    let case = &fx.deadline;
    let queue = unique(&case.queue);
    let mut broker = Broker::connect(&url).await;
    broker
        .send(&queue, std::slice::from_ref(&case.record), BodyKind::Data)
        .await;

    let driver = Driver::spawn(&queue, &[], &sleep_plugin_env(case.sleep_ms));
    let received = wait_delivering(&queue, 1, Duration::from_secs(120)).await;
    tokio::time::sleep(Duration::from_secs(case.settle_secs)).await;
    let signalled = Instant::now();
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let exit_after = signalled.elapsed();
    let redelivered = broker
        .drain(&queue, 1, Duration::from_secs(case.redelivered_within_secs))
        .await;
    broker.close().await;

    assert!(received, "record was never received\n{stdout}\n{stderr}");
    let status = status.unwrap_or_else(|| {
        panic!(
            "driver alive {}s after SIGTERM\n{stdout}",
            case.exit_within_secs
        )
    });
    assert!(status.success(), "exit {status:?}\n{stdout}\n{stderr}");
    assert!(stdout.contains(&fx.markers.left_for_redelivery), "{stdout}");
    assert_eq!(final_total(&stdout, &fx.markers.final_total), 0, "{stdout}");
    assert_eq!(redelivered, vec![case.record.clone()], "{stdout}");
    assert_eq!(
        dead_letter_count(&queue),
        0,
        "released, never dead-lettered"
    );
    eprintln!(
        "e2e_activemq_stuck_worker_cannot_outlive_shutdown_deadline: exit after {exit_after:?}"
    );
}

/// A TCP proxy between the driver and the broker whose connections can all be
/// cut at once (a broker/network loss as the driver sees it).
struct Proxy {
    port: u16,
    cut: watch::Sender<bool>,
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let (cut, cut_rx) = watch::channel(false);
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let (upstream, mut cut_rx) = (upstream.clone(), cut_rx.clone());
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = cut_rx.wait_for(|cut| *cut) => {}
                    }
                });
            }
        });
        Self { port, cut }
    }

    /// Drops every proxied connection (both sides see the close).
    fn cut(&self) {
        self.cut.send_replace(true);
    }
}

/// `url` with host/port replaced by the local proxy port.
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

/// Losing the broker connection mid-run is FATAL: orderly shutdown (total
/// line) and exit 255; the unsettled in-worker record is redelivered by the
/// broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_activemq_link_loss_is_fatal() {
    let Some(url) = gate() else { return };
    let fx = shutdown_fixture();
    let case = &fx.link_loss;
    let queue = unique(&case.queue);
    let mut broker = Broker::connect(&url).await;
    broker
        .send(&queue, std::slice::from_ref(&case.record), BodyKind::Data)
        .await;

    let (_, upstream) = via_proxy(&url, 0);
    let proxy = Proxy::start(upstream).await;
    let (proxied_url, _) = via_proxy(&url, proxy.port);
    let mut envs = sleep_plugin_env(case.sleep_ms);
    envs.push(("SENZING_ACTIVEMQ_URL", proxied_url));
    let driver = Driver::spawn(&queue, &[], &envs);
    let received = wait_delivering(&queue, 1, Duration::from_secs(120)).await;
    tokio::time::sleep(Duration::from_secs(case.settle_secs)).await;
    proxy.cut();
    let (status, stdout, stderr) = driver
        .finish(Duration::from_secs(case.exit_within_secs))
        .await;
    let redelivered = broker
        .drain(&queue, 1, Duration::from_secs(case.redelivered_within_secs))
        .await;
    broker.close().await;

    assert!(received, "record was never received\n{stdout}\n{stderr}");
    let status = status.unwrap_or_else(|| {
        panic!(
            "driver still alive {}s after the link loss\n{stdout}\n{stderr}",
            case.exit_within_secs
        )
    });
    assert_eq!(status.code(), Some(case.exit_code), "{stdout}\n{stderr}");
    assert!(
        stderr.contains(&fx.markers.link_lost),
        "fatal reason\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&fx.markers.final_total),
        "orderly shutdown\n{stdout}"
    );
    assert_eq!(redelivered, vec![case.record.clone()], "{stdout}");
    eprintln!("e2e_activemq_link_loss_is_fatal: exit {:?}", status.code());
}

/// Wrong credentials fail the SASL handshake (non-zero, after engine init);
/// a non-AMQP URL fails validation before it.
#[tokio::test]
async fn e2e_activemq_bad_credentials_and_bad_url_fail_startup() {
    let Some(url) = gate() else { return };
    let fx = shutdown_fixture();
    let case = &fx.startup;
    let queue = unique(&case.queue);
    let grace = Duration::from_secs(case.exit_within_secs);

    let user = url::Url::parse(&url).expect("url").username().to_string();
    assert!(!user.is_empty(), "the rig URL must carry credentials");
    let wrong = Driver::spawn(
        &queue,
        &[],
        &[
            ("SENZING_ACTIVEMQ_USER", user),
            ("SENZING_ACTIVEMQ_PASSWORD", case.wrong_password.clone()),
        ],
    );
    let (status, stdout, stderr) = wrong.finish(grace).await;
    let status = status.expect("driver must exit on its own on bad credentials");
    assert!(
        !status.success(),
        "bad credentials must fail\n{stdout}\n{stderr}"
    );
    assert!(stderr.contains(&fx.markers.connect_failed), "{stderr}");

    let bad = Driver::spawn(
        &queue,
        &[],
        &[("SENZING_ACTIVEMQ_URL", case.bad_url.clone())],
    );
    let (status, stdout, stderr) = bad.finish(grace).await;
    assert_eq!(status.and_then(|s| s.code()), Some(1), "{stdout}\n{stderr}");
    assert!(stderr.contains(&case.bad_url_marker), "{stderr}");
}

/// `--help` / `--version` work without any broker or engine.
#[test]
fn e2e_activemq_help_and_version() {
    let help = Command::new(driver_bin())
        .arg("--help")
        .output()
        .expect("--help");
    assert!(help.status.success());
    let text = String::from_utf8_lossy(&help.stdout);
    for flag in [
        "SENZING_ACTIVEMQ_URL",
        "SENZING_ACTIVEMQ_QUEUE",
        "SENZING_PREFETCH",
    ] {
        assert!(text.contains(flag), "--help lacks {flag}\n{text}");
    }
    let version = Command::new(driver_bin())
        .arg("--version")
        .output()
        .expect("--version");
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).contains(env!("CARGO_PKG_VERSION")));
}

// ==========================================================================
// IN-FLIGHT CAP — tests/fixtures/prefetch.yaml
// ==========================================================================

#[derive(serde::Deserialize)]
struct PrefetchCase {
    queue: String,
    threads: usize,
    prefetch: u64,
    sleep_ms: u64,
    count: usize,
    sample_every_ms: u64,
    loaded_within_secs: u64,
    record_template: String,
    final_total: String,
}

/// `--prefetch N` is the TOTAL in-flight cap: with slow workers and a backlog
/// Artemis never has more than N deliveries outstanding to the driver, the
/// cap beyond threads is used, and every record still loads.
#[tokio::test]
async fn e2e_activemq_prefetch_is_the_total_in_flight_cap() {
    let Some(url) = gate() else { return };
    let case: PrefetchCase = load_fixture("prefetch.yaml");
    let queue = unique(&case.queue);
    let mut broker = Broker::connect(&url).await;
    broker
        .send(
            &queue,
            &records(&case.record_template, "", case.count),
            BodyKind::Data,
        )
        .await;

    let mut envs = sleep_plugin_env(case.sleep_ms);
    envs.push(("SENZING_THREADS_PER_PROCESS", case.threads.to_string()));
    let prefetch = case.prefetch.to_string();
    let driver = Driver::spawn(&queue, &["--prefetch", &prefetch], &envs);
    let mut max_delivering = 0;
    let deadline = Instant::now() + Duration::from_secs(case.loaded_within_secs);
    while Instant::now() < deadline {
        let Some((messages, delivering)) = queue_counts(&queue) else {
            break;
        };
        max_delivering = max_delivering.max(delivering);
        if messages == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(case.sample_every_ms)).await;
    }
    driver.signal(libc::SIGTERM);
    let (status, stdout, stderr) = driver.finish(Duration::from_secs(30)).await;
    broker.close().await;

    assert!(status.expect("exit").success(), "{stdout}\n{stderr}");
    assert_eq!(
        final_total(&stdout, &case.final_total),
        case.count,
        "{stdout}"
    );
    assert!(
        max_delivering <= case.prefetch,
        "{max_delivering} outstanding exceeds the total cap {}\n{stdout}",
        case.prefetch
    );
    assert!(
        max_delivering > case.threads as u64,
        "the cap beyond threads was never used ({max_delivering})\n{stdout}"
    );
    eprintln!("e2e_activemq_prefetch_is_the_total_in_flight_cap: max delivering {max_delivering}");
}
