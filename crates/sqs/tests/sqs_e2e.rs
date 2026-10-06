//! SQS end-to-end tests against a real engine and a real SQS API (ElasticMQ in
//! CI, or any endpoint the standard AWS env vars point at).
//!
//! Gated on BOTH `SENZING_ENGINE_CONFIGURATION_JSON` and `AWS_ENDPOINT_URL`
//! being set; otherwise each test prints `SKIP` and passes, like the RabbitMQ
//! e2e suite. Credentials/region come from the standard AWS provider chain
//! (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` — ElasticMQ
//! accepts any values).
//!
//! What is proven here that unit tests cannot:
//! * a rejected record (engine bad input) and an unparseable message BOTH land
//!   in the dead-letter queue VERBATIM with an `SzReason` attribute,
//!   discovered from the source queue's RedrivePolicy, are counted as
//!   rejected (not adds) and marked `REJECTING:` on stdout, and the source
//!   queue is left empty (no silent delete; data in `tests/fixtures/rejects.yaml`);
//! * a source queue with no RedrivePolicy and no `--dead-letter-queue-url`
//!   refuses to start (non-zero exit) unless `--allow-no-dlq` is given;
//! * shutdown (data in `tests/fixtures/shutdown.yaml`): a worker stuck in a
//!   long call cannot outlive the 10 s deadline and its message is left for
//!   redelivery, SIGHUP is graceful, persistent `ReceiveMessage` failure is
//!   fatal (exit 255), and the `--mq-recheck-secs` depth probe logs its
//!   drained/active transitions;
//! * `--prefetch` is the TOTAL in-flight cap (data in
//!   `tests/fixtures/prefetch.yaml`).

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;

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

fn gate() -> Option<()> {
    for var in ["SENZING_ENGINE_CONFIGURATION_JSON", "AWS_ENDPOINT_URL"] {
        if std::env::var(var).map(|v| v.is_empty()).unwrap_or(true) {
            skip(format_args!("SKIP: {var} not set"));
            return None;
        }
    }
    Some(())
}

fn driver_bin() -> &'static str {
    env!("CARGO_BIN_EXE_sz_sqs_combined_consumer")
}

async fn client() -> Client {
    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    Client::new(&cfg)
}

/// Creates `<name>-dlq` and `<name>` (the latter redriving to the former) and
/// returns `(source_url, dlq_url)`. Names carry the pid so parallel runs and
/// leftovers never collide.
async fn make_queue_pair(client: &Client, name: &str, with_redrive: bool) -> (String, String) {
    let dlq_name = format!("{name}-dlq");
    let dlq_url = client
        .create_queue()
        .queue_name(&dlq_name)
        .send()
        .await
        .expect("create dlq")
        .queue_url()
        .expect("dlq url")
        .to_string();
    let dlq_arn = client
        .get_queue_attributes()
        .queue_url(&dlq_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("dlq attrs")
        .attributes()
        .and_then(|a| a.get(&QueueAttributeName::QueueArn).cloned())
        .expect("dlq arn");
    let mut req = client.create_queue().queue_name(name);
    if with_redrive {
        req = req.attributes(
            QueueAttributeName::RedrivePolicy,
            format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"5"}}"#),
        );
    }
    let src_url = req
        .send()
        .await
        .expect("create source queue")
        .queue_url()
        .expect("source url")
        .to_string();
    (src_url, dlq_url)
}

async fn delete_queues(client: &Client, urls: &[&str]) {
    for u in urls {
        let _ = client.delete_queue().queue_url(*u).send().await;
    }
}

async fn send_all(client: &Client, url: &str, bodies: &[String]) {
    for b in bodies {
        client
            .send_message()
            .queue_url(url)
            .message_body(b)
            .send()
            .await
            .expect("send");
    }
}

/// Drains up to `want` messages from `url`, polling for at most `within`.
async fn drain(client: &Client, url: &str, want: usize, within: Duration) -> Vec<String> {
    let deadline = Instant::now() + within;
    let mut got = Vec::new();
    while got.len() < want && Instant::now() < deadline {
        let resp = client
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .expect("receive");
        for m in resp.messages() {
            if let Some(b) = m.body() {
                got.push(b.to_string());
            }
            if let Some(h) = m.receipt_handle() {
                let _ = client
                    .delete_message()
                    .queue_url(url)
                    .receipt_handle(h)
                    .send()
                    .await;
            }
        }
    }
    got
}

async fn approx_depth(client: &Client, url: &str) -> u32 {
    client
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .expect("attrs")
        .attributes()
        .map(|a| {
            [
                QueueAttributeName::ApproximateNumberOfMessages,
                QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            ]
            .iter()
            .filter_map(|k| a.get(k))
            .filter_map(|v| v.parse::<u32>().ok())
            .sum()
        })
        .unwrap_or(0)
}

fn spawn_driver(
    queue_url: &str,
    extra: &[&str],
    stdout_path: &std::path::Path,
) -> std::process::Child {
    spawn_driver_env(queue_url, extra, &[], stdout_path)
}

/// [`spawn_driver`] with extra environment variables (set last, so they win).
fn spawn_driver_env(
    queue_url: &str,
    extra: &[&str],
    envs: &[(&str, String)],
    stdout_path: &std::path::Path,
) -> std::process::Child {
    let out = std::fs::File::create(stdout_path).expect("stdout file");
    let err = std::fs::File::create(stdout_path.with_extension("err")).expect("stderr file");
    Command::new(driver_bin())
        .args(extra)
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .env("SENZING_SQS_QUEUE_URL", queue_url)
        .env("SENZING_THREADS_PER_PROCESS", "2")
        .env("SENZING_REDO_PERCENT", "0")
        .env("SENZING_SQS_WAIT_TIME", "1")
        .env_remove("SENZING_INPUT_FILE")
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn sqs driver")
}

fn sigterm(child: &std::process::Child) {
    send_signal(child, libc::SIGTERM);
}

fn send_signal(child: &std::process::Child, sig: libc::c_int) {
    // kill(2) directly: the CI container has no `kill` executable on PATH.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    assert_eq!(
        rc,
        0,
        "kill({sig}) failed: {}",
        std::io::Error::last_os_error()
    );
}

fn wait_bounded(
    child: &mut std::process::Child,
    grace: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(st)) => return Some(st),
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(e) => panic!("try_wait: {e}"),
        }
    }
}

fn read_to_string(p: &std::path::Path) -> String {
    let mut s = String::new();
    if let Ok(mut f) = std::fs::File::open(p) {
        let _ = f.read_to_string(&mut s);
    }
    s
}

/// `tests/fixtures/rejects.yaml`.
#[derive(serde::Deserialize)]
struct RejectsFixture {
    queue: String,
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
    reason_attribute: String,
    poison_reason: String,
    final_total: String,
    rejected: String,
    engine_reject: String,
    poison_warn: String,
    poison_reject: String,
}

fn load_fixture<T: serde::de::DeserializeOwned>(name: &str) -> T {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_norway::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// Drains up to `want` messages from `url` (polling at most `within`), each
/// with the String value of its `attribute` message attribute, if any.
async fn drain_with_attribute(
    client: &Client,
    url: &str,
    attribute: &str,
    want: usize,
    within: Duration,
) -> Vec<(String, Option<String>)> {
    let deadline = Instant::now() + within;
    let mut got = Vec::new();
    while got.len() < want && Instant::now() < deadline {
        let resp = client
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .message_attribute_names("All")
            .send()
            .await
            .expect("receive");
        for m in resp.messages() {
            let value = m
                .message_attributes()
                .and_then(|a| a.get(attribute))
                .and_then(|v| v.string_value())
                .map(str::to_string);
            got.push((m.body().unwrap_or_default().to_string(), value));
            if let Some(h) = m.receipt_handle() {
                let _ = client
                    .delete_message()
                    .queue_url(url)
                    .receipt_handle(h)
                    .send()
                    .await;
            }
        }
    }
    got
}

/// Valid records plus one engine-rejected and one unparseable message: the
/// two bad ones land in the RedrivePolicy-discovered DLQ verbatim with an
/// `SzReason` attribute, the final line counts only the adds and reports 2
/// rejected, and each reject carries the unified `REJECTING:` marker.
#[tokio::test]
async fn e2e_sqs_rejects_land_in_discovered_dlq_verbatim() {
    let Some(()) = gate() else { return };
    let fx: RejectsFixture = load_fixture("rejects.yaml");
    let client = client().await;
    let name = format!("{}-{}", fx.queue, std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;

    let mut all: Vec<String> = (0..fx.valid_count)
        .map(|i| fx.valid_template.replace("{i}", &i.to_string()))
        .collect();
    all.push(fx.engine_reject.clone());
    all.push(fx.unparseable.clone());
    send_all(&client, &src, &all).await;

    let out_path = out_path("rejects");
    let mut child = spawn_driver(&src, &[], &out_path);

    // The DLQ must receive exactly the two rejects; then the source must drain.
    let within = Duration::from_secs(fx.dlq_within_secs);
    let mut rejects =
        drain_with_attribute(&client, &dlq, &fx.markers.reason_attribute, 2, within).await;
    let deadline = Instant::now() + Duration::from_secs(fx.settle_within_secs);
    while approx_depth(&client, &src).await > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let src_left = approx_depth(&client, &src).await;

    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    let (stdout, stderr) = take_output(&out_path);
    delete_queues(&client, &[&src, &dlq]).await;

    let status = status.expect("driver did not exit after SIGTERM");
    assert!(
        status.success(),
        "driver exited {status:?}\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&format!("DeadLetter: {dlq}")),
        "DLQ must be discovered from the RedrivePolicy and announced\n{stdout}"
    );
    assert_eq!(src_left, 0, "source queue must be fully settled\n{stdout}");
    for marker in [&fx.markers.poison_warn, &fx.markers.poison_reject] {
        assert!(
            stdout.contains(marker.as_str()),
            "missing {marker:?}\n{stdout}"
        );
    }
    let engine_reason = stdout
        .lines()
        .find_map(|l| l.strip_prefix(fx.markers.engine_reject.as_str()))
        .unwrap_or_else(|| panic!("missing {:?}\n{stdout}", fx.markers.engine_reject));

    rejects.sort();
    let (bodies, reasons): (Vec<String>, Vec<Option<String>>) = rejects.into_iter().unzip();
    let mut want = vec![fx.engine_reject.clone(), fx.unparseable.clone()];
    want.sort();
    assert_eq!(bodies, want, "DLQ content mismatch\n{stdout}\n{stderr}");
    // Sorted bodies: the engine reject ('{"DATA_SOURCE":"E2E...') before the
    // unparseable one ('{"DATA_SOURCE":"TEST...').
    assert_eq!(
        reasons[0].as_deref(),
        Some(engine_reason),
        "engine reject's SzReason must be the marker's reason\n{stdout}"
    );
    assert!(
        reasons[1]
            .as_deref()
            .is_some_and(|r| r.starts_with(&fx.markers.poison_reason)),
        "poison SzReason {:?} must start with {:?}",
        reasons[1],
        fx.markers.poison_reason
    );

    let total_prefix = fx
        .markers
        .final_total
        .replace("{N}", &fx.valid_count.to_string());
    let total = stdout
        .lines()
        .find(|l| l.starts_with(&total_prefix))
        .unwrap_or_else(|| panic!("expected {total_prefix:?}\n{stdout}"));
    assert!(
        total.contains(&fx.markers.rejected),
        "expected {:?} in {total:?}\n{stdout}",
        fx.markers.rejected
    );
    eprintln!("e2e_sqs_rejects_land_in_discovered_dlq_verbatim: {total}; SzReason {reasons:?}");
}

#[tokio::test]
async fn e2e_sqs_refuses_to_start_without_dlq_unless_allowed() {
    let Some(()) = gate() else { return };
    let client = client().await;
    let name = format!("sz-e2e-nodlq-{}", std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, false).await;

    // 1. No redrive policy, no override, no --allow-no-dlq -> refuse (non-zero).
    let out_path = std::env::temp_dir().join(format!("{name}.out"));
    let mut child = spawn_driver(&src, &[], &out_path);
    let status = wait_bounded(&mut child, Duration::from_secs(60));
    let stderr = read_to_string(&out_path.with_extension("err"));
    let status = status.expect("driver should exit on its own when refusing to start");
    assert!(
        !status.success(),
        "must refuse to start without a DLQ\n{stderr}"
    );
    assert!(
        stderr.contains("no dead-letter queue"),
        "refusal must explain itself\n{stderr}"
    );

    // 2. --allow-no-dlq -> starts, reject is deleted (and named in the log).
    let bad =
        r#"{"DATA_SOURCE":"E2E_NO_SUCH_DSRC","RECORD_ID":"SQS_NODLQ","NAME_FULL":"X"}"#.to_string();
    send_all(&client, &src, &[bad]).await;
    let mut child = spawn_driver(&src, &["--allow-no-dlq"], &out_path);
    let deadline = Instant::now() + Duration::from_secs(60);
    while approx_depth(&client, &src).await > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let left = approx_depth(&client, &src).await;
    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    // tracing writes to stdout; eprintln! diagnostics to stderr. Check both.
    let output = read_to_string(&out_path) + &read_to_string(&out_path.with_extension("err"));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(out_path.with_extension("err"));
    delete_queues(&client, &[&src, &dlq]).await;

    assert!(status.expect("exit").success(), "{output}");
    assert_eq!(left, 0, "reject must be deleted under --allow-no-dlq");
    assert!(
        output
            .contains("REJECTING (no DLQ, --allow-no-dlq): deleting E2E_NO_SUCH_DSRC : SQS_NODLQ"),
        "deleted reject must be named with its body in the log\n{output}"
    );
    assert!(
        output.contains("NO dead-letter queue"),
        "running without a DLQ must warn loudly at startup\n{output}"
    );
}

// ==========================================================================
// SHUTDOWN / RECEIVE / DEPTH e2e — real ElasticMQ, real engine, real driver
// binary; test data in tests/fixtures/shutdown.yaml.
// ==========================================================================

/// `tests/fixtures/shutdown.yaml`.
#[derive(serde::Deserialize)]
struct Fixture {
    markers: Markers,
    sighup: SighupCase,
    deadline: DeadlineCase,
    receive_error: ReceiveErrorCase,
    depth: DepthCase,
}

#[derive(serde::Deserialize)]
struct Markers {
    final_total: String,
    sighup: String,
    left_for_redelivery: String,
    shutdown_error: String,
    depth_drained: String,
    depth_active: String,
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
    visibility_timeout_secs: u64,
    settle_secs: u64,
    exit_within_secs: u64,
    redelivered_within_secs: u64,
    record: String,
}

#[derive(serde::Deserialize)]
struct ReceiveErrorCase {
    queue_path: String,
    dead_letter_path: String,
    exit_code: i32,
    min_exit_secs: u64,
    max_exit_secs: u64,
}

#[derive(serde::Deserialize)]
struct DepthCase {
    queue: String,
    mq_recheck_secs: u64,
    sleep_ms: u64,
    startup_within_secs: u64,
    loaded_within_secs: u64,
    records: Vec<String>,
}

fn fixture() -> Fixture {
    load_fixture("shutdown.yaml")
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

/// `(visible, in flight)` message counts of `url`.
async fn visible_and_in_flight(client: &Client, url: &str) -> (u32, u32) {
    let attrs = client
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .expect("attrs");
    let get = |k: QueueAttributeName| -> u32 {
        attrs
            .attributes()
            .and_then(|a| a.get(&k))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (
        get(QueueAttributeName::ApproximateNumberOfMessages),
        get(QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
    )
}

/// Polls `path` until it contains `needle` `count` times or `within` passes.
async fn wait_for_output(path: &std::path::Path, needle: &str, count: usize, within: Duration) {
    let deadline = Instant::now() + within;
    while read_to_string(path).matches(needle).count() < count && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn out_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sz-e2e-{tag}-{}.out", std::process::id()))
}

/// Stdout + stderr of a finished child; both files removed.
fn take_output(path: &std::path::Path) -> (String, String) {
    let out = read_to_string(path);
    let err = read_to_string(&path.with_extension("err"));
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("err"));
    (out, err)
}

/// A worker blocked far past the 10 s grace (plugin `SLEEP_MS`) must not keep
/// the process alive: SIGTERM -> exit within the bound. SQS policy: the
/// message is left un-deleted (not dead-lettered) and reappears after its
/// visibility timeout.
#[tokio::test]
async fn e2e_sqs_stuck_worker_cannot_outlive_shutdown_deadline() {
    let Some(()) = gate() else { return };
    let fx = fixture();
    let case = &fx.deadline;
    let client = client().await;
    let name = format!("{}-{}", case.queue, std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;
    send_all(&client, &src, std::slice::from_ref(&case.record)).await;

    let out = out_path("deadline");
    let vis = case.visibility_timeout_secs.to_string();
    let mut child = spawn_driver_env(
        &src,
        &["--visibility-timeout", &vis],
        &sleep_plugin_env(case.sleep_ms),
        &out,
    );
    // Received (in flight, not visible) means it is on its way to a worker.
    let deadline = Instant::now() + Duration::from_secs(120);
    while visible_and_in_flight(&client, &src).await != (0, 1) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let received = visible_and_in_flight(&client, &src).await == (0, 1);
    tokio::time::sleep(Duration::from_secs(case.settle_secs)).await;
    let signalled_at = Instant::now();
    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(case.exit_within_secs));
    let exit_after = signalled_at.elapsed();
    let redelivered = drain(
        &client,
        &src,
        1,
        Duration::from_secs(case.redelivered_within_secs),
    )
    .await;
    let dead_lettered = drain(&client, &dlq, 1, Duration::from_secs(2)).await;
    let (stdout, stderr) = take_output(&out);
    delete_queues(&client, &[&src, &dlq]).await;

    assert!(received, "record was never received\n{stdout}\n{stderr}");
    let status = status.unwrap_or_else(|| {
        panic!(
            "driver still alive {}s after SIGTERM (stuck worker outlived the deadline)\n{stdout}",
            case.exit_within_secs
        )
    });
    assert!(
        status.success(),
        "driver exited {status:?}\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&fx.markers.left_for_redelivery),
        "in-worker message not named as left for redelivery\n{stdout}"
    );
    assert_eq!(final_total(&stdout, &fx.markers.final_total), 0, "{stdout}");
    assert_eq!(
        redelivered,
        vec![case.record.clone()],
        "message must be left un-deleted and reappear after the visibility timeout\n{stdout}"
    );
    assert!(
        dead_lettered.is_empty(),
        "SQS must not dead-letter at shutdown\n{stdout}"
    );
    eprintln!(
        "e2e_sqs_stuck_worker_cannot_outlive_shutdown_deadline: exited {exit_after:?} after \
         SIGTERM, message redelivered"
    );
}

/// SIGHUP is a graceful shutdown (previously the default disposition killed
/// the process): drain, exit 0, final total printed.
#[tokio::test]
async fn e2e_sqs_sighup_is_graceful() {
    let Some(()) = gate() else { return };
    let fx = fixture();
    let case = &fx.sighup;
    let client = client().await;
    let name = format!("{}-{}", case.queue, std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;
    send_all(&client, &src, &case.records).await;

    let out = out_path("sighup");
    let mut child = spawn_driver(&src, &[], &out);
    let deadline = Instant::now() + Duration::from_secs(120);
    while approx_depth(&client, &src).await > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let drained = approx_depth(&client, &src).await == 0;
    send_signal(&child, libc::SIGHUP);
    let status = wait_bounded(&mut child, Duration::from_secs(case.exit_within_secs));
    let (stdout, stderr) = take_output(&out);
    delete_queues(&client, &[&src, &dlq]).await;

    assert!(drained, "queue did not drain\n{stdout}\n{stderr}");
    let status = status.expect("driver did not exit within bound after SIGHUP");
    assert!(
        status.success(),
        "driver exited non-zero after SIGHUP: {status:?}\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&fx.markers.sighup),
        "SIGHUP not handled gracefully\n{stdout}"
    );
    let adds = final_total(&stdout, &fx.markers.final_total);
    assert_eq!(adds, case.records.len(), "all records loaded\n{stdout}");
    eprintln!("e2e_sqs_sighup_is_graceful: {adds} loaded, exit 0 on SIGHUP");
}

/// A port nothing listens on (bound, then released).
fn closed_local_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    l.local_addr().expect("local addr").port()
}

/// Persistent `ReceiveMessage` failure (dead endpoint) is fatal: orderly
/// shutdown and exit 255 after RECEIVE_ERROR_MAX consecutive failures, not a
/// retry loop forever. The DLQ is passed explicitly so no other SQS call
/// precedes the poller.
#[tokio::test]
async fn e2e_sqs_receive_errors_become_fatal() {
    let Some(()) = gate() else { return };
    let fx = fixture();
    let case = &fx.receive_error;
    let endpoint = format!("http://127.0.0.1:{}", closed_local_port());
    let src = format!("{endpoint}{}", case.queue_path);
    let dlq = format!("{endpoint}{}", case.dead_letter_path);

    let out = out_path("recv-error");
    let started = Instant::now();
    let mut child = spawn_driver_env(
        &src,
        &["--dead-letter-queue-url", &dlq],
        &[("AWS_ENDPOINT_URL", endpoint.clone())],
        &out,
    );
    let status = wait_bounded(&mut child, Duration::from_secs(case.max_exit_secs));
    let elapsed = started.elapsed();
    let (stdout, stderr) = take_output(&out);

    let status = status.unwrap_or_else(|| {
        panic!(
            "driver still retrying {}s against a dead endpoint\n{stdout}\n{stderr}",
            case.max_exit_secs
        )
    });
    assert_eq!(
        status.code(),
        Some(case.exit_code),
        "{status:?}\n{stdout}\n{stderr}"
    );
    assert!(
        elapsed >= Duration::from_secs(case.min_exit_secs),
        "fatal after only {elapsed:?}: must take consecutive failures\n{stdout}\n{stderr}"
    );
    assert!(
        stderr.contains(&fx.markers.shutdown_error),
        "fatal reason not reported\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&fx.markers.final_total),
        "shutdown was not orderly (no final total)\n{stdout}"
    );
    eprintln!("e2e_sqs_receive_errors_become_fatal: exit 255 after {elapsed:?}");
}

/// `--mq-recheck-secs` drives the depth probe: an empty start logs "drained",
/// a backlog the in-flight cap cannot take logs "active", and the end of the
/// load logs "drained" again.
#[tokio::test]
async fn e2e_sqs_mq_recheck_logs_depth_transitions() {
    let Some(()) = gate() else { return };
    let fx = fixture();
    let case = &fx.depth;
    let client = client().await;
    let name = format!("{}-{}", case.queue, std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;

    let out = out_path("depth");
    let recheck = case.mq_recheck_secs.to_string();
    // --prefetch 2: in-flight cap = threads, so the rest stays visible.
    let mut child = spawn_driver_env(
        &src,
        &["--mq-recheck-secs", &recheck, "--prefetch", "2"],
        &sleep_plugin_env(case.sleep_ms),
        &out,
    );
    let (drained, active) = (&fx.markers.depth_drained, &fx.markers.depth_active);
    wait_for_output(
        &out,
        drained,
        1,
        Duration::from_secs(case.startup_within_secs),
    )
    .await;
    send_all(&client, &src, &case.records).await;
    let loaded = Duration::from_secs(case.loaded_within_secs);
    wait_for_output(&out, drained, 2, loaded).await;
    let deadline = Instant::now() + loaded;
    while approx_depth(&client, &src).await > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Let the last deletes land before signalling.
    tokio::time::sleep(Duration::from_secs(2)).await;
    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    let (stdout, stderr) = take_output(&out);
    delete_queues(&client, &[&src, &dlq]).await;

    assert!(
        status.expect("exit").success(),
        "driver failed\n{stdout}\n{stderr}"
    );
    let first_drained = stdout.find(drained.as_str());
    let first_active = stdout.find(active.as_str());
    let last_drained = stdout.rfind(drained.as_str());
    assert!(
        matches!(
            (first_drained, first_active, last_drained),
            (Some(d), Some(a), Some(l)) if d < a && a < l
        ),
        "expected drained -> active -> drained depth logs\n{stdout}\n{stderr}"
    );
    assert_eq!(
        final_total(&stdout, &fx.markers.final_total),
        case.records.len(),
        "{stdout}"
    );
    eprintln!("e2e_sqs_mq_recheck_logs_depth_transitions: drained -> active -> drained");
}

/// `tests/fixtures/prefetch.yaml`.
#[derive(serde::Deserialize)]
struct PrefetchCase {
    queue: String,
    threads: usize,
    prefetch: u32,
    sleep_ms: u64,
    count: usize,
    sample_every_ms: u64,
    loaded_within_secs: u64,
    record_template: String,
    final_total: String,
}

/// `--prefetch N` is the TOTAL in-flight cap (was threads + N): with slow
/// workers and a backlog, at most N messages are ever in flight (received and
/// not yet deleted) and the cap is actually used; every record still loads.
#[tokio::test]
async fn e2e_sqs_prefetch_is_the_total_in_flight_cap() {
    let Some(()) = gate() else { return };
    let case: PrefetchCase = load_fixture("prefetch.yaml");
    let client = client().await;
    let name = format!("{}-{}", case.queue, std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;
    let records: Vec<String> = (0..case.count)
        .map(|i| case.record_template.replace("{i}", &i.to_string()))
        .collect();
    send_all(&client, &src, &records).await;

    let out = out_path("prefetch");
    let (threads, prefetch) = (case.threads.to_string(), case.prefetch.to_string());
    let mut child = spawn_driver_env(
        &src,
        &["--prefetch", &prefetch],
        &[
            &sleep_plugin_env(case.sleep_ms)[..],
            &[("SENZING_THREADS_PER_PROCESS", threads)],
        ]
        .concat(),
        &out,
    );
    let mut max_in_flight = 0;
    let deadline = Instant::now() + Duration::from_secs(case.loaded_within_secs);
    while Instant::now() < deadline {
        let (visible, in_flight) = visible_and_in_flight(&client, &src).await;
        max_in_flight = max_in_flight.max(in_flight);
        if visible + in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(case.sample_every_ms)).await;
    }
    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    let (stdout, stderr) = take_output(&out);
    delete_queues(&client, &[&src, &dlq]).await;

    assert!(
        status.expect("exit").success(),
        "driver failed\n{stdout}\n{stderr}"
    );
    assert_eq!(
        final_total(&stdout, &case.final_total),
        case.count,
        "every record must load\n{stdout}"
    );
    assert!(
        max_in_flight <= case.prefetch,
        "{max_in_flight} in flight exceeds the total cap {}\n{stdout}",
        case.prefetch
    );
    assert!(
        max_in_flight > case.threads as u32,
        "the cap beyond threads was never used ({max_in_flight} in flight)\n{stdout}"
    );
    eprintln!("e2e_sqs_prefetch_is_the_total_in_flight_cap: max in flight {max_in_flight}");
}
