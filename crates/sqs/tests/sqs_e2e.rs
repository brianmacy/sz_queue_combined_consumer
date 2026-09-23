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
//!   in the dead-letter queue VERBATIM, discovered from the source queue's
//!   RedrivePolicy, and the source queue is left empty (no silent delete);
//! * a source queue with no RedrivePolicy and no `--dead-letter-queue-url`
//!   refuses to start (non-zero exit) unless `--allow-no-dlq` is given.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;

fn gate() -> Option<()> {
    for var in ["SENZING_ENGINE_CONFIGURATION_JSON", "AWS_ENDPOINT_URL"] {
        if std::env::var(var).map(|v| v.is_empty()).unwrap_or(true) {
            eprintln!("SKIP: {var} not set");
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
    let out = std::fs::File::create(stdout_path).expect("stdout file");
    let err = std::fs::File::create(stdout_path.with_extension("err")).expect("stderr file");
    Command::new(driver_bin())
        .args(extra)
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
    // kill(2) directly: the CI container has no `kill` executable on PATH.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(
        rc,
        0,
        "kill(SIGTERM) failed: {}",
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

#[tokio::test]
async fn e2e_sqs_rejects_land_in_discovered_dlq_verbatim() {
    let Some(()) = gate() else { return };
    let client = client().await;
    let name = format!("sz-e2e-{}", std::process::id());
    let (src, dlq) = make_queue_pair(&client, &name, true).await;

    const N_VALID: usize = 12;
    let valid: Vec<String> = (0..N_VALID)
        .map(|i| {
            format!(
                r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"SQS_E2E_{i}","NAME_FULL":"Sqs Tester {i}","EMAIL_ADDRESS":"sqs{i}@example.com"}}"#
            )
        })
        .collect();
    let bad_engine =
        r#"{"DATA_SOURCE":"E2E_NO_SUCH_DSRC","RECORD_ID":"SQS_BAD_DSRC","NAME_FULL":"X Y"}"#
            .to_string();
    let bad_parse = r#"{"RECORD_ID":"SQS_BAD_PARSE","NAME_FULL":"No Data Source"}"#.to_string();
    let mut all = valid.clone();
    all.push(bad_engine.clone());
    all.push(bad_parse.clone());
    send_all(&client, &src, &all).await;

    let out_path = std::env::temp_dir().join(format!("{name}.out"));
    let mut child = spawn_driver(&src, &[], &out_path);

    // The DLQ must receive exactly the two rejects; then the source must drain.
    let rejects = drain(&client, &dlq, 2, Duration::from_secs(90)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while approx_depth(&client, &src).await > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let src_left = approx_depth(&client, &src).await;

    sigterm(&child);
    let status = wait_bounded(&mut child, Duration::from_secs(30));
    let stdout = read_to_string(&out_path);
    let stderr = read_to_string(&out_path.with_extension("err"));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(out_path.with_extension("err"));
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
    let mut got = rejects.clone();
    let mut want = vec![bad_engine, bad_parse];
    got.sort();
    want.sort();
    assert_eq!(got, want, "DLQ content mismatch\n{stdout}\n{stderr}");
    assert_eq!(src_left, 0, "source queue must be fully settled\n{stdout}");
    assert!(
        stdout.contains("Sending to deadletter: E2E_NO_SUCH_DSRC : SQS_BAD_DSRC"),
        "v4-parity reject line missing\n{stdout}"
    );
    let total = stdout
        .lines()
        .find(|l| l.starts_with("Processed total of "))
        .unwrap_or_else(|| panic!("no total line\n{stdout}"));
    assert!(
        total.starts_with(&format!("Processed total of {N_VALID} adds")),
        "expected {N_VALID} adds: {total}\n{stdout}"
    );
    eprintln!("e2e_sqs_rejects_land_in_discovered_dlq_verbatim: {N_VALID} adds, 2 dead-lettered");
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
