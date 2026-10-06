//! The mixed-mode run path (redo% < 100): the RabbitMQ [`Transport`] plugged
//! into the shared core loop ([`queue_loop::run`]), which owns the engine
//! worker pool, the redo fetcher (redo% > 0), stats, the long-record monitor
//! and the bounded-grace shutdown.
//!
//! Inherited from `sz_rabbit_consumer_rust` (see its module docs for the full
//! rationale): one lapin `Connection` + `Channel`, single async consumer,
//! `basic_qos` prefetch, acks/rejects ONLY on the async task. RabbitMQ policy:
//! a load record running past `2 * LONG_RECORD` is dead-lettered, and at
//! shutdown a delivery still inside a worker is dead-lettered while
//! queued-but-unstarted ones are left unacked for broker requeue (design §6).

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, Consumer};
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::queue_loop::{self, DeadLetterReason, Delivery, Policy, Transport};
use sz_combined_consumer_core::queue_run::RunOutcome;

/// RabbitMQ settle policy (consumer parity).
const RABBIT_POLICY: Policy = Policy {
    dead_letter_long_records: true,
    dead_letter_in_worker_at_shutdown: true,
    stuck_records_label: "load records",
};

/// Runs the combined driver until SIGINT/SIGTERM/SIGHUP or a fatal engine
/// error.
pub async fn run(config: Config, env: Arc<SzEnvironmentCore>) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    let load_pref = threads - redo_pref;
    tracing::info!(
        "Threads: {threads} (load-preferring: {load_pref}, redo-preferring: {redo_pref}, \
         redo%: {}, prefetch: {})",
        config.redo_percent,
        config.prefetch
    );
    // DIAGNOSTIC: license as seen right after engine init (before any config
    // reload / reinitialize). Compare against "LICENSE AFTER REINIT" to prove
    // whether reinitialize() drops the init-JSON license -> demo recordLimit.
    match env.get_product().and_then(|p| p.get_license()) {
        Ok(lic) => tracing::info!("LICENSE AFTER INIT: {lic}"),
        Err(e) => tracing::warn!("get_license after init failed: {e}"),
    }
    // Log the engine's active-config-id vs the registered default at startup, so we
    // can see whether the first reconcile reinit is real (active != default) — keyed
    // on get_active_config_id(), the engine's true state.
    sz_combined_consumer_core::config_reload::log_startup_config(&env);
    let url = config
        .url
        .clone()
        .context("AMQP URL required for redo% < 100 (validated at startup)")?;
    let queue = config
        .queue
        .clone()
        .context("queue required for redo% < 100 (validated at startup)")?;

    // The core loop spawns the engine pool BEFORE awaiting this connect (a
    // connect failure then stops it), preserving the original ordering.
    let connect = RabbitTransport::connect(url, queue, config.prefetch);
    queue_loop::run(&config, env, connect, RABBIT_POLICY).await
}

/// One lapin connection + channel + push consumer on the source queue.
pub struct RabbitTransport {
    connection: Connection,
    channel: Channel,
    consumer: Consumer,
    queue: String,
}

impl RabbitTransport {
    /// Connects, asserts the queue exists, sets `basic_qos` and starts the
    /// consumer.
    async fn connect(url: String, queue: String, prefetch: u16) -> Result<Self> {
        tracing::info!("Connecting to RabbitMQ");
        let connection = Connection::connect(&url, ConnectionProperties::default())
            .await
            .context("failed to connect to RabbitMQ")?;
        let channel = connection
            .create_channel()
            .await
            .context("failed to create channel")?;

        // Passive declare: assert the queue exists; do not create it.
        passive_declare(&channel, &queue)
            .await
            .with_context(|| format!("queue '{queue}' does not exist (passive declare)"))?;

        // prefetch = threads + 2 by default: the +2 overshoot keeps a standing
        // load_ch buffer that masks the ack round-trip so the non-blocking worker
        // dispatch never stalls per-record (design §1.2/§2.3). Not materially
        // larger: prefetched messages are invisible to sibling processes.
        channel
            .basic_qos(prefetch, BasicQosOptions::default())
            .await
            .context("failed to set basic_qos")?;

        let consumer = channel
            .basic_consume(
                queue.as_str().into(),
                crate::INSTANCE_NAME.into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("failed to start consuming")?;
        Ok(Self {
            connection,
            channel,
            consumer,
            queue,
        })
    }
}

/// Passive `queue_declare`: existence check at connect, depth probe after.
async fn passive_declare(channel: &Channel, queue: &str) -> lapin::Result<lapin::Queue> {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
}

impl Transport for RabbitTransport {
    async fn recv(&mut self) -> Option<Result<Delivery>> {
        let next = self.consumer.next().await?;
        Some(
            next.map(|d| Delivery {
                tag: d.delivery_tag,
                body: d.data,
            })
            .map_err(|e| anyhow!("AMQP consume error: {e}")),
        )
    }

    async fn ack(&mut self, tag: u64) {
        if let Err(e) = self
            .channel
            .basic_ack(tag, BasicAckOptions::default())
            .await
        {
            tracing::error!("basic_ack failed for {tag}: {e:#}");
        }
    }

    /// `basic_reject(requeue=false)`: the queue's dead-letter exchange (if
    /// any) receives it. AMQP reject cannot carry the reason; it is on the
    /// `REJECTING:` stdout marker the core loop printed.
    async fn dead_letter(&mut self, tag: u64, reason: DeadLetterReason) {
        let Err(e) = self
            .channel
            .basic_reject(tag, BasicRejectOptions { requeue: false })
            .await
        else {
            return;
        };
        // Best effort at shutdown (the connection is about to close).
        if reason != DeadLetterReason::Shutdown {
            tracing::error!("basic_reject failed for {tag} ({reason}): {e:#}");
        }
    }

    async fn depth(&mut self) -> Option<u32> {
        match passive_declare(&self.channel, &self.queue).await {
            Ok(q) => Some(q.message_count()),
            Err(e) => {
                tracing::warn!("MQ depth probe failed: {e:#}");
                None
            }
        }
    }

    /// Unacked deliveries are requeued by the broker on close.
    async fn close(self) {
        if let Err(e) = self.connection.close(0, "shutting down".into()).await {
            tracing::warn!("error closing connection: {e:#}");
        }
    }
}
