//! Integration tests for the combined RabbitMQ load + redo Senzing driver.
//!
//! These tests exercise REAL infrastructure — no mocks. The real-engine tests
//! are skipped (with a printed reason) when prerequisites are absent, but FAIL
//! LOUDLY if a step that should succeed does not. Prerequisites:
//!
//! * The Senzing native library at `/opt/senzing/er/lib` (set
//!   `SENZING_LIB_PATH` / `LD_LIBRARY_PATH`).
//! * `SENZING_ENGINE_CONFIGURATION_JSON` pointing at a real, INITIALIZED
//!   backend (schema DDL applied + a default config registered). sqlite is
//!   sufficient for the engine-only tests; PostgreSQL/MSSQL for the backend
//!   matrices. See `ci.yml`'s integration job for the authoritative init.
//! * A running RabbitMQ reachable via `SENZING_AMQP_URL` for the end-to-end
//!   load test (`SENZING_RABBITMQ_QUEUE` names an existing queue).
//!
//! Coverage vs the split-driver siblings (this binary subsumes BOTH):
//!   * pure-logic scheduler behavior — `redo_preferring_count` and topology
//!     validation at the 0% / 100% endpoints and interior redo% (always run);
//!   * the LOAD path — `add_record` via the same call the load workers make;
//!   * the REDO path — `count_redo_records` / `get_redo_record` /
//!     `process_redo_record`, the same calls the fetcher + redo workers make;
//!   * bad-record classification (dead-letter vs fatal).
//!
//! How to run locally:
//! ```bash
//! export SENZING_LIB_PATH=/opt/senzing/er/lib
//! export LD_LIBRARY_PATH=/opt/senzing/er/lib
//! export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION":"sqlite3://na:na@/tmp/G2C.db"}}'
//! # for the end-to-end load test additionally:
//! export SENZING_AMQP_URL='amqp://guest:guest@localhost:5672/%2F'
//! export SENZING_RABBITMQ_QUEUE='senzing-rabbitmq-queue'
//! cargo test --test integration_test -- --nocapture --test-threads=1
//! ```

use std::sync::Arc;

use sz_rabbit_combined_consumer::config::{redo_preferring_count, validate_topology};
use sz_rabbit_combined_consumer::record::{ErrorClass, ParseError, classify_error, parse_record};
use sz_rust_sdk::prelude::*;

const INSTANCE: &str = "sz_rabbit_combined_consumer_it";

/// Returns the engine configuration JSON if the environment is fully set up for
/// a real Senzing test, otherwise `None` (test is skipped).
fn engine_config() -> Option<String> {
    std::env::var("SENZING_ENGINE_CONFIGURATION_JSON")
        .ok()
        .filter(|s| !s.is_empty())
}

// --------------------------------------------------------------------------
// Pure-logic tests (always run) — record parsing + the redo% scheduler.
// --------------------------------------------------------------------------

#[test]
fn record_parsing_extracts_fields() {
    let info = parse_record(br#"{"DATA_SOURCE":"TEST","RECORD_ID":"INT_1"}"#)
        .expect("well-formed record should parse");
    assert_eq!(info.data_source, "TEST");
    assert_eq!(info.record_id, "INT_1");
}

#[test]
fn missing_data_source_is_dead_letter_parse_error() {
    // A missing DATA_SOURCE is a poison record: the consumer side dead-letters
    // it (basic_reject, no requeue) and keeps running — it is NOT fatal.
    let err = parse_record(br#"{"NAME_FULL":"No Identifiers"}"#).unwrap_err();
    assert_eq!(err, ParseError::MissingField("DATA_SOURCE"));
}

/// The scheduler split |B| = redo-preferring worker count. The 0% endpoint
/// starts NO redo fetcher (|B| = 0, redo channel never exists); the 100%
/// endpoint devotes the whole pool to redo (|B| = N, load channel never
/// exists); interior values are a clamped rounded share.
#[test]
fn scheduler_endpoint_and_interior_splits() {
    // 0% = pure consumer: no redo-preferring workers.
    assert_eq!(redo_preferring_count(12, 0), 0);
    // 100% = pure redoer: entire pool prefers redo.
    assert_eq!(redo_preferring_count(12, 100), 12);
    // Compiled default 20% at N=12 -> 2 redo-preferring (== the 100M-arm floor).
    assert_eq!(redo_preferring_count(12, 20), 2);
    // Small non-zero interior clamps up to at least one redo-preferring worker.
    assert_eq!(redo_preferring_count(12, 1), 1);
    // Large interior clamps to N-1 so at least one load-preferring worker
    // remains (the interior case always keeps both classes populated).
    assert_eq!(redo_preferring_count(12, 99), 11);
}

/// Topology validation is the loud startup gate (design §5): AMQP is required
/// below 100%, and an interior redo% needs at least two workers to host both
/// preference classes. The 100% endpoint runs with AMQP entirely unset.
#[test]
fn scheduler_topology_validation() {
    // redo% < 100 requires URL + queue.
    assert!(validate_topology(12, 0, None, None).is_err());
    assert!(validate_topology(12, 20, Some("amqp://h"), None).is_err());
    assert!(validate_topology(12, 20, Some("amqp://h"), Some("q")).is_ok());
    // Interior redo% needs >= 2 workers.
    assert!(validate_topology(1, 20, Some("amqp://h"), Some("q")).is_err());
    assert!(validate_topology(2, 20, Some("amqp://h"), Some("q")).is_ok());
    // 100% pure redoer: no AMQP needed, single worker allowed.
    assert!(validate_topology(1, 100, None, None).is_ok());
}

// --------------------------------------------------------------------------
// Real-engine tests (skipped without SENZING_ENGINE_CONFIGURATION_JSON).
// --------------------------------------------------------------------------

/// LOAD path: initialize the environment and add a record — the same call path
/// the load-preferring workers use. Fails loudly if the engine is configured
/// but `add_record` / `get_stats` error.
#[test]
fn real_engine_load_path_add_record() {
    let Some(config) = engine_config() else {
        eprintln!(
            "SKIP real_engine_load_path_add_record: SENZING_ENGINE_CONFIGURATION_JSON not set \
             (requires /opt/senzing + an initialized backend)"
        );
        return;
    };

    let env: Arc<SzEnvironmentCore> = SzEnvironmentCore::get_instance(INSTANCE, &config, false)
        .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");

    let body = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"IT_REC_1","NAME_FULL":"Integration Tester","EMAIL_ADDRESS":"it@example.com"}"#;
    let info = parse_record(body.as_bytes()).expect("well-formed record should parse");
    let result = engine.add_record(
        &info.data_source,
        &info.record_id,
        body,
        Some(SzFlags::ADD_RECORD_DEFAULT),
    );
    assert!(result.is_ok(), "add_record failed: {:?}", result.err());

    // Stats must be retrievable (the dedicated stats thread depends on it).
    let stats = engine.get_stats();
    assert!(stats.is_ok(), "get_stats failed: {:?}", stats.err());

    // NOTE: do NOT call destroy_global_instance() here. The Senzing engine is a
    // process-global singleton shared across every test in this binary. Tearing
    // it down mid-suite leaves the singleton's is_initialized flag true while
    // the native engine is gone, so the next test's get_instance() returns a
    // DEAD engine. Process exit reclaims the singleton. The suite runs with
    // --test-threads=1 (see ci.yml) so parallel get_instance() cannot race.
}

/// REDO path: the same calls the single fetcher + redo-preferring workers make.
/// `count_redo_records` must succeed (the monitor / status-line backlog signal),
/// and if the load above produced redo work, `get_redo_record` +
/// `process_redo_record` must round-trip without error.
#[test]
fn real_engine_redo_path() {
    let Some(config) = engine_config() else {
        eprintln!("SKIP real_engine_redo_path: engine config not set");
        return;
    };

    let env: Arc<SzEnvironmentCore> = SzEnvironmentCore::get_instance(INSTANCE, &config, false)
        .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");

    // Add a couple of related records to make redo work likely (mirrors how the
    // combined driver's load path feeds its own redo queue).
    for (rid, name) in [("IT_REDO_1", "Redo Tester"), ("IT_REDO_2", "Redo Tester")] {
        let body = format!(
            r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"{rid}","NAME_FULL":"{name}","EMAIL_ADDRESS":"redo@example.com"}}"#
        );
        let info = parse_record(body.as_bytes()).expect("well-formed record should parse");
        let r = engine.add_record(
            &info.data_source,
            &info.record_id,
            &body,
            Some(SzFlags::ADD_RECORD_DEFAULT),
        );
        assert!(r.is_ok(), "add_record (redo seed) failed: {:?}", r.err());
    }

    // count_redo_records is the backlog signal — it must always succeed
    // (monitoring ONLY; the driver never uses it as a loop/emptiness condition).
    let count = engine.count_redo_records();
    assert!(
        count.is_ok(),
        "count_redo_records failed: {:?}",
        count.err()
    );

    // Drain whatever redo work exists via the fetcher's exact call pair. The
    // queue may legitimately be empty (an empty string) depending on the
    // backend/config; that is not a failure. When a record IS returned,
    // processing it must not error.
    let mut processed = 0usize;
    for _ in 0..16 {
        let record = engine.get_redo_record().expect("get_redo_record failed");
        if record.trim().is_empty() {
            break;
        }
        let r = engine.process_redo_record(&record, None);
        assert!(r.is_ok(), "process_redo_record failed: {:?}", r.err());
        processed += 1;
    }
    eprintln!("real_engine_redo_path: processed {processed} redo record(s)");
    // Singleton is intentionally left initialized (see the note above).
}

/// Confirms a deliberately malformed record is classified for the dead-letter
/// queue rather than crashing the driver, using the REAL engine.
#[test]
fn real_engine_bad_record_is_dead_lettered() {
    let Some(config) = engine_config() else {
        eprintln!("SKIP real_engine_bad_record_is_dead_lettered: engine config not set");
        return;
    };

    let env: Arc<SzEnvironmentCore> = SzEnvironmentCore::get_instance(INSTANCE, &config, false)
        .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");

    // A WELL-FORMED record (valid DATA_SOURCE + RECORD_ID) whose content the
    // ENGINE rejects as bad input. An invalid name like "**" triggers the DQM
    // plugin (SENZ0082), which both the load and redo paths classify as a
    // dead-letter/drop, never fatal.
    let body = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"IT_BAD_1","PRIMARY_NAME_FULL":"**"}"#;
    let info = parse_record(body.as_bytes()).expect("record is well-formed and must parse");
    if let Err(e) = engine.add_record(
        &info.data_source,
        &info.record_id,
        body,
        Some(SzFlags::ADD_RECORD_DEFAULT),
    ) {
        assert_eq!(
            classify_error(&e),
            ErrorClass::BadInputOrTimeout,
            "engine-rejected record should classify as dead-letter, got: {e}"
        );
    }
    // Singleton is intentionally left initialized (see the note above).
}

// ==========================================================================
// FULL END-TO-END loop tests — publish to a live test broker, run the ACTUAL
// driver binary (main -> combined::run / pure_redoer::run) against a live
// test DB, and drive a deterministic graceful shutdown via SIGTERM.
//
// Why spawn the compiled binary rather than call the functions in-process:
//   * it exercises the REAL path (env init, one Sz_init/process, the tokio vs
//     pure-std::thread branch in main, teardown) exactly as deployed;
//   * SIGTERM to a child is the driver's documented graceful-shutdown trigger
//     and gives DETERMINISTIC termination — the driver drains in-flight work
//     and exits 0, which the test asserts;
//   * it avoids entangling the process-global engine singleton and RUNNING
//     atomics across tests (the child gets its own).
//
// SAFETY: these connect ONLY to the isolated test broker/DB named by
// SENZING_AMQP_URL / SENZING_ENGINE_CONFIGURATION_JSON on their test ports and
// to a dedicated `sz-combined-e2e-queue` — never production infra. They are
// skipped (printed reason) when those env vars are absent.
// ==========================================================================

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const E2E_QUEUE: &str = "sz-combined-e2e-queue";

fn amqp_url() -> Option<String> {
    std::env::var("SENZING_AMQP_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Build a tokio current-thread runtime for the test's own AMQP helper calls.
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build test tokio runtime")
}

/// Declare (idempotent) + purge the test queue, then publish `records`,
/// awaiting each publisher confirm so we know the broker has them before the
/// driver starts. Mirrors `sz_rabbit_publisher`'s confirm-based publish.
async fn publish_records(url: &str, queue: &str, records: &[String]) -> anyhow::Result<()> {
    use lapin::options::{BasicPublishOptions, QueueDeclareOptions, QueuePurgeOptions};
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Confirmation, Connection, ConnectionProperties};

    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_purge(queue.into(), QueuePurgeOptions::default())
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
        // A nacked publish means the broker did not accept it — fail loudly.
        anyhow::ensure!(
            !matches!(confirm, Confirmation::Nack(_)),
            "broker NACKed a published record"
        );
    }
    conn.close(0, "publish done".into()).await.ok();
    Ok(())
}

/// Ready-message count of the test queue via a passive declare (the same probe
/// the driver uses for its diagnostic MQ depth line).
async fn queue_ready_count(url: &str, queue: &str) -> anyhow::Result<u32> {
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
    let count = q.message_count();
    conn.close(0, "probe done".into()).await.ok();
    Ok(count)
}

/// N unique well-formed TEST records for an e2e run.
fn make_records(prefix: &str, n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"{prefix}_{i}","NAME_FULL":"E2E Tester {i}","EMAIL_ADDRESS":"e2e{i}@example.com","ADDR_FULL":"{i} Test St"}}"#
            )
        })
        .collect()
}

/// Path to the driver binary cargo built for this integration-test run.
fn driver_bin() -> &'static str {
    env!("CARGO_BIN_EXE_sz_rabbit_combined_consumer")
}

/// Send SIGTERM to a child pid (the driver's graceful-shutdown trigger).
fn sigterm(pid: u32) {
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status();
}

/// Wait up to `grace` for the child to exit; SIGKILL + reap if it overruns.
fn wait_bounded(
    child: &mut std::process::Child,
    grace: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => return None,
        }
    }
}

/// Shared driver of the combined (redo% < 100) e2e scenarios. Publishes N
/// records, runs the binary at `redo_percent`, waits for the queue to drain,
/// then SIGTERMs and asserts a clean exit that loaded all N records.
fn run_combined_e2e(redo_percent: u8) {
    let (Some(engine), Some(url)) = (engine_config(), amqp_url()) else {
        eprintln!(
            "SKIP run_combined_e2e({redo_percent}): SENZING_ENGINE_CONFIGURATION_JSON \
             and/or SENZING_AMQP_URL not set (requires the isolated test broker + DB)"
        );
        return;
    };
    let _ = &engine; // inherited into the child via the process environment.

    const N: usize = 10;
    let records = make_records(&format!("E2E_{redo_percent}"), N);

    rt().block_on(publish_records(&url, E2E_QUEUE, &records))
        .expect("failed to publish e2e records to the test queue");

    // Capture the child's stdout so we can read its final "Processed total ..."
    // line (the authoritative load count the driver reports on shutdown).
    let out_path =
        std::env::temp_dir().join(format!("sz_e2e_{redo_percent}_{}.out", std::process::id()));
    let out_file = std::fs::File::create(&out_path).expect("create child stdout file");

    let mut child = Command::new(driver_bin())
        .env("SENZING_REDO_PERCENT", redo_percent.to_string())
        .env("SENZING_THREADS_PER_PROCESS", "4")
        .env("SENZING_RABBITMQ_QUEUE", E2E_QUEUE)
        // short redo pause so the fetcher (20% case) does not sleep 60s at the
        // drain tail and stall the test.
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "2")
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn combined driver binary");

    // Wait for the driver to consume the whole queue (bounded). This also
    // proves it attached and is processing.
    let drained = {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut drained = false;
        while Instant::now() < deadline {
            if let Ok(0) = rt().block_on(queue_ready_count(&url, E2E_QUEUE)) {
                drained = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        drained
    };
    // Let any prefetched-but-in-flight deliveries finish + ack before we stop.
    std::thread::sleep(Duration::from_secs(3));

    sigterm(child.id());
    let status = wait_bounded(&mut child, Duration::from_secs(30));

    let mut stdout = String::new();
    let _ = std::fs::File::open(&out_path).and_then(|mut f| f.read_to_string(&mut stdout));
    let _ = std::fs::remove_file(&out_path);

    assert!(
        drained,
        "queue did not drain within timeout (redo%={redo_percent})\n{stdout}"
    );
    let status = status.expect("driver did not exit within grace after SIGTERM");
    assert!(
        status.success(),
        "driver exited non-zero (redo%={redo_percent}): {status:?}\n{stdout}"
    );
    let total_line = stdout
        .lines()
        .find(|l| l.starts_with("Processed total of "))
        .unwrap_or_else(|| {
            panic!("driver never printed its shutdown total (redo%={redo_percent})\n{stdout}")
        });
    let adds: usize = total_line
        .trim_start_matches("Processed total of ")
        .split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("could not parse add count from: {total_line:?}"));
    assert_eq!(
        adds, N,
        "expected all {N} records loaded (redo%={redo_percent}), driver reported {adds}\n{stdout}"
    );
    eprintln!("e2e combined redo%={redo_percent}: loaded {adds}/{N} records, clean shutdown");
}

/// 0% = pure consumer endpoint: no redo fetcher, load-only.
#[test]
fn e2e_combined_pure_consumer_0pct() {
    run_combined_e2e(0);
}

/// 20% = mixed scheduler: with 4 threads, |B| = 1 redo-preferring + 3
/// load-preferring, exercising the cross-over fallback path while loading.
#[test]
fn e2e_combined_mixed_20pct() {
    run_combined_e2e(20);
}

/// 100% = pure redoer endpoint: no AMQP is opened, no tokio runtime is built;
/// the binary runs the pure `std::thread` fetcher + redo-worker + monitor
/// shape and drains the redo queue. We seed redo work via the real engine
/// first, run the redoer, then SIGTERM and assert a clean exit with the redo
/// backlog drained to zero.
#[test]
fn e2e_pure_redoer_100pct() {
    let Some(engine_cfg) = engine_config() else {
        eprintln!("SKIP e2e_pure_redoer_100pct: engine config not set");
        return;
    };

    // Seed: add a batch of records through the real engine; add_record enqueues
    // redo for affected entities, giving the redoer something to drain. (Even
    // if the backend produces no redo, the run still exercises the full
    // fetcher/worker/monitor/graceful-shutdown path.)
    let env: Arc<SzEnvironmentCore> = SzEnvironmentCore::get_instance(INSTANCE, &engine_cfg, false)
        .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");
    for rec in make_records("E2E_REDO_SEED", 12) {
        let info = parse_record(rec.as_bytes()).expect("seed record parses");
        let _ = engine.add_record(
            &info.data_source,
            &info.record_id,
            &rec,
            Some(SzFlags::ADD_RECORD_DEFAULT),
        );
    }
    let backlog_before = engine.count_redo_records().unwrap_or(0);
    eprintln!("e2e_pure_redoer_100pct: redo backlog before = {backlog_before}");

    let out_path = std::env::temp_dir().join(format!("sz_e2e_redoer_{}.out", std::process::id()));
    let out_file = std::fs::File::create(&out_path).expect("create child stdout file");

    // No AMQP env needed at redo%=100 (the binary never opens it); remove the
    // queue var to prove the pure-redoer path runs with AMQP entirely unset.
    let mut child = Command::new(driver_bin())
        .env("SENZING_REDO_PERCENT", "100")
        .env("SENZING_THREADS_PER_PROCESS", "2")
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "2")
        .env_remove("SENZING_RABBITMQ_QUEUE")
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn pure-redoer driver binary");

    // Give the redoer time to start and drain whatever redo exists, polling the
    // backlog from our own engine handle until it reaches 0 (bounded).
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut drained_to_zero = false;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(0) = engine.count_redo_records() {
            // one extra settle so any cascade re-enqueue also clears
            std::thread::sleep(Duration::from_secs(2));
            if let Ok(0) = engine.count_redo_records() {
                drained_to_zero = true;
                break;
            }
        }
    }

    sigterm(child.id());
    let status = wait_bounded(&mut child, Duration::from_secs(30));

    let mut stdout = String::new();
    let _ = std::fs::File::open(&out_path).and_then(|mut f| f.read_to_string(&mut stdout));
    let _ = std::fs::remove_file(&out_path);

    let status = status.expect("pure redoer did not exit within grace after SIGTERM");
    assert!(
        status.success(),
        "pure redoer exited non-zero: {status:?}\n{stdout}"
    );
    let final_backlog = engine.count_redo_records().unwrap_or(-1);
    assert_eq!(
        final_backlog, 0,
        "redo backlog should be drained to 0 after the redoer ran, got {final_backlog}\n{stdout}"
    );
    eprintln!(
        "e2e_pure_redoer_100pct: drained_to_zero={drained_to_zero}, final backlog={final_backlog}, clean shutdown"
    );
    // Singleton intentionally left initialized (see the note on the earlier
    // real-engine tests).
}

// ==========================================================================
// TRUTH-SET e2e — real cross-source entity resolution + real redo drain +
// a correctness signal, all at max allowed volume (<= 500 DSR eval cap).
//
// Uses the Senzing demo truth set (CUSTOMERS 120 + REFERENCE 22 + WATCHLIST
// 17 = 159 records that RESOLVE across sources) plus an optional TEST top-up
// slice of the 100M dataset, published through the ACTUAL driver binary.
//
// Deterministic two-phase design (why these redo% modes):
//   * Phase A loads at redo% = 0 (PURE CONSUMER). At 0% the driver processes
//     ZERO redo, so the ER-generated redo backlog accumulates intact — a
//     DETERMINISTIC non-zero backlog (the demo truth set yields ~5). A 20%
//     load would auto-drain its own small redo, leaving nothing for the pure
//     redoer to demonstrably drain; 20% mixed mode is covered separately by
//     `e2e_combined_mixed_20pct`.
//   * Phase B drains at redo% = 100 (PURE REDOER) and we assert the non-zero
//     backlog goes to 0 — the real fix for the earlier "backlog was 0" caveat.
//
// Correctness: the demo truth set's known-good resolution is 85 entities from
// 159 records (85 distinct CLUSTER_IDs in truthset_key.csv). We assert Senzing
// produced exactly 85 entities SCOPED to the three truth-set data sources
// (so the TEST top-up singletons do not perturb the count).
//
// SAFETY: isolated test broker/DB + a dedicated `sz-combined-truthset-queue`.
// Prereqs (skips with a printed reason if absent): SENZING_ENGINE_CONFIGURATION_JSON,
// SENZING_AMQP_URL, IT_TRUTHSET_DIR (demo dir), IT_PG_DSN (libpq slash-form for
// the scoped entity-count query); IT_TOPUP_JSONL (optional TEST top-up file).
// ==========================================================================

const TRUTHSET_QUEUE: &str = "sz-combined-truthset-queue";
/// Known-good resolved-entity count for the demo truth set (85 distinct
/// CLUSTER_IDs in truthset_key.csv; confirmed to match Senzing's output).
const TRUTHSET_KNOWN_GOOD_ENTITIES: i64 = 85;

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// Read a `.jsonl` file into one String per non-blank line.
fn read_jsonl(path: &std::path::Path) -> Vec<String> {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Run a single scalar-returning query against the test PG via `psql -tAc`.
fn psql_scalar_i64(dsn: &str, sql: &str) -> i64 {
    let out = Command::new("psql")
        .arg(dsn)
        .arg("-tAc")
        .arg(sql)
        .output()
        .expect("failed to run psql (is postgresql-client installed?)");
    assert!(
        out.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|_| {
            panic!(
                "non-integer psql result: {:?}",
                String::from_utf8_lossy(&out.stdout)
            )
        })
}

/// Resolved-entity count scoped to the truth-set data sources, so TEST top-up
/// records do not affect it. Joins records -> observed entities -> resolved
/// entities in the Senzing core schema.
fn scoped_truthset_entities(dsn: &str) -> i64 {
    psql_scalar_i64(
        dsn,
        "SELECT count(DISTINCT ok.res_ent_id) \
         FROM dsrc_record dr \
         JOIN obs_ent oe ON oe.dsrc_id = dr.dsrc_id AND oe.ent_src_key = dr.ent_src_key \
         JOIN res_ent_okey ok ON ok.obs_ent_id = oe.obs_ent_id \
         WHERE (dr.json_data::json->>'DATA_SOURCE') IN ('CUSTOMERS','REFERENCE','WATCHLIST')",
    )
}

/// Spawn the driver binary with the given redo% + queue, capturing stdout.
fn spawn_driver(
    redo_percent: u8,
    threads: usize,
    queue: Option<&str>,
    tag: &str,
) -> (std::process::Child, std::path::PathBuf) {
    let out_path = std::env::temp_dir().join(format!("sz_ts_{tag}_{}.out", std::process::id()));
    let out_file = std::fs::File::create(&out_path).expect("create child stdout file");
    let mut cmd = Command::new(driver_bin());
    cmd.env("SENZING_REDO_PERCENT", redo_percent.to_string())
        .env("SENZING_THREADS_PER_PROCESS", threads.to_string())
        .env("SENZING_REDO_SLEEP_TIME_IN_SECONDS", "2")
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::null());
    match queue {
        Some(q) => {
            cmd.env("SENZING_RABBITMQ_QUEUE", q);
        }
        None => {
            cmd.env_remove("SENZING_RABBITMQ_QUEUE");
        }
    }
    let child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn driver ({tag}): {e}"));
    (child, out_path)
}

/// Resolve the demo truth-set directory: `IT_TRUTHSET_DIR` if set, else the
/// vendored git submodule at `<crate>/truth-sets/truthsets/demo`. Returns
/// `None` (test skips) when neither exists — e.g. the submodule was not checked
/// out (`git submodule update --init`).
fn truthset_dir() -> Option<std::path::PathBuf> {
    if let Some(d) = env_nonempty("IT_TRUTHSET_DIR") {
        return Some(std::path::PathBuf::from(d));
    }
    let submodule =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("truth-sets/truthsets/demo");
    submodule
        .join("customers.jsonl")
        .exists()
        .then_some(submodule)
}

#[test]
fn e2e_truthset_resolution_and_redo_drain() {
    let (Some(_engine_cfg), Some(url), Some(dir), Some(pg_dsn)) = (
        engine_config(),
        amqp_url(),
        truthset_dir(),
        env_nonempty("IT_PG_DSN"),
    ) else {
        eprintln!(
            "SKIP e2e_truthset_resolution_and_redo_drain: needs SENZING_ENGINE_CONFIGURATION_JSON, \
             SENZING_AMQP_URL, IT_PG_DSN (libpq DSN for psql), and the demo truth set (git \
             submodule at truth-sets/truthsets/demo, or IT_TRUTHSET_DIR)"
        );
        return;
    };

    // Assemble the payload: the 3 truth-set sources (159 resolving records) +
    // an OPTIONAL TEST top-up slice, scaling volume toward (but <=) 500 DSRs.
    // The top-up (e.g. a /public_data slice via IT_TOPUP_JSONL) is skipped when
    // absent (e.g. in CI) — the known-good entity assertion is scoped to the
    // truth-set data sources, so it holds with truth-set-only records too.
    let mut records = Vec::new();
    for f in ["customers.jsonl", "reference.jsonl", "watchlist.jsonl"] {
        records.extend(read_jsonl(&dir.join(f)));
    }
    let truthset_n = records.len();
    if let Some(topup) = env_nonempty("IT_TOPUP_JSONL") {
        let topup_path = std::path::Path::new(&topup);
        if topup_path.exists() {
            records.extend(read_jsonl(topup_path));
        } else {
            eprintln!("e2e_truthset: IT_TOPUP_JSONL set but {topup} missing; using truth-set only");
        }
    }
    let total_n = records.len();
    eprintln!(
        "e2e_truthset: publishing {total_n} records ({truthset_n} truth-set + {} top-up)",
        total_n - truthset_n
    );
    assert!(
        total_n <= 500,
        "refusing to exceed the 500-DSR eval cap: total_n={total_n}"
    );

    // Engine handle for reading the redo backlog (the driver's own signal).
    let env: Arc<SzEnvironmentCore> =
        SzEnvironmentCore::get_instance(INSTANCE, &_engine_cfg, false)
            .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");

    rt().block_on(publish_records(&url, TRUTHSET_QUEUE, &records))
        .expect("failed to publish truth-set payload to the test queue");

    // --- Phase A: PURE CONSUMER (redo% = 0) — load everything, drain NO redo.
    let (mut child_a, out_a) = spawn_driver(0, 6, Some(TRUTHSET_QUEUE), "loadA");
    let drained_load = {
        let deadline = Instant::now() + Duration::from_secs(240);
        let mut ok = false;
        while Instant::now() < deadline {
            if let Ok(0) = rt().block_on(queue_ready_count(&url, TRUTHSET_QUEUE)) {
                ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        ok
    };
    std::thread::sleep(Duration::from_secs(3)); // let prefetched in-flight ack
    sigterm(child_a.id());
    let status_a = wait_bounded(&mut child_a, Duration::from_secs(30));
    let mut stdout_a = String::new();
    let _ = std::fs::File::open(&out_a).and_then(|mut f| f.read_to_string(&mut stdout_a));
    let _ = std::fs::remove_file(&out_a);

    assert!(
        drained_load,
        "load queue did not drain within timeout\n{stdout_a}"
    );
    let status_a = status_a.expect("Phase A driver did not exit within grace after SIGTERM");
    assert!(
        status_a.success(),
        "Phase A (redo%=0 load) exited non-zero: {status_a:?}\n{stdout_a}"
    );
    let adds_a: usize = stdout_a
        .lines()
        .find(|l| l.starts_with("Processed total of "))
        .and_then(|l| {
            l.trim_start_matches("Processed total of ")
                .split_whitespace()
                .next()
        })
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("Phase A never printed a shutdown total\n{stdout_a}"));
    assert_eq!(
        adds_a, total_n,
        "Phase A should have loaded+acked all {total_n} records, reported {adds_a}\n{stdout_a}"
    );

    // Correctness (pre-redo): real cross-source MERGING already occurred, but
    // resolution is NOT yet final because redo%=0 processed no redo — pending
    // redo finalizes the last merge(s). So here we only require that merging
    // happened (0 < entities < record_count); the exact known-good count is
    // asserted after the redo drains in Phase B.
    let entities_pre = scoped_truthset_entities(&pg_dsn);
    assert!(
        entities_pre > 0 && entities_pre < truthset_n as i64,
        "expected real cross-source merging (0 < entities < {truthset_n}) after the \
         pure-consumer load, got {entities_pre}\n{stdout_a}"
    );

    // A real, NON-ZERO redo backlog must exist now (redo%=0 processed none).
    let backlog_mid = engine
        .count_redo_records()
        .expect("count_redo_records failed after load");
    assert!(
        backlog_mid > 0,
        "expected a non-zero redo backlog after the pure-consumer load, got {backlog_mid} \
         (truth-set cross-source resolution should enqueue redo)"
    );
    eprintln!(
        "e2e_truthset: Phase A loaded {adds_a}/{total_n}, pre-redo entities={entities_pre} \
         (not yet final), redo backlog now = {backlog_mid}"
    );

    // --- Phase B: PURE REDOER (redo% = 100) — drain the backlog to 0.
    let (mut child_b, out_b) = spawn_driver(100, 4, None, "redoB");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut drained_to_zero = false;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(0) = engine.count_redo_records() {
            std::thread::sleep(Duration::from_secs(2)); // settle for cascades
            if let Ok(0) = engine.count_redo_records() {
                drained_to_zero = true;
                break;
            }
        }
    }
    sigterm(child_b.id());
    let status_b = wait_bounded(&mut child_b, Duration::from_secs(30));
    let mut stdout_b = String::new();
    let _ = std::fs::File::open(&out_b).and_then(|mut f| f.read_to_string(&mut stdout_b));
    let _ = std::fs::remove_file(&out_b);

    let status_b = status_b.expect("Phase B pure redoer did not exit within grace after SIGTERM");
    assert!(
        status_b.success(),
        "Phase B (redo%=100 pure redoer) exited non-zero: {status_b:?}\n{stdout_b}"
    );
    let final_backlog = engine.count_redo_records().unwrap_or(-1);
    assert_eq!(
        final_backlog, 0,
        "pure redoer must drain the redo backlog to 0, got {final_backlog}\n{stdout_b}"
    );
    // Resolution is stable after redo processing.
    let entities_after = scoped_truthset_entities(&pg_dsn);
    assert_eq!(
        entities_after, TRUTHSET_KNOWN_GOOD_ENTITIES,
        "truth-set entity count changed after redo drain: {entities_after}"
    );
    eprintln!(
        "e2e_truthset: Phase B pure redoer drained backlog {backlog_mid} -> 0 \
         (drained_to_zero={drained_to_zero}), entities stable at {entities_after}, clean shutdown"
    );
    // Singleton intentionally left initialized (see the earlier real-engine tests).
}
