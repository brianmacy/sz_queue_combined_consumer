//! Apache ActiveMQ Artemis [`Transport`] (AMQP 1.0, `fe2o3-amqp`) for the
//! shared core queue loop ([`queue_loop::run`]), which owns the engine worker
//! pool, the redo fetcher (redo% > 0), stats, the long-record monitor and the
//! bounded-grace shutdown. Only ingestion and settle are Artemis-specific:
//!
//! | Concern      | RabbitMQ (lapin)            | Artemis (here)                                 |
//! |--------------|-----------------------------|------------------------------------------------|
//! | ingest       | push consumer stream        | AMQP receiver link, source capability `queue`  |
//! | backpressure | basic_qos prefetch          | manual link credit = prefetch - unsettled      |
//! | ack success  | basic_ack                   | `accepted` outcome                             |
//! | dead-letter  | basic_reject(requeue=false) | `rejected` outcome -> address's dead-letter address |
//! | long record  | reject at 2x LONG_RECORD    | nothing (no ack timeout; never dead-lettered)  |
//! | release      | left unacked -> close       | `released` outcome (no delivery-count bump)    |
//! | identity     | u64 delivery tag            | synthetic u64 -> delivery-info map             |
//!
//! ## Queue semantics
//! The receiver source carries the `queue` capability, which makes Artemis
//! bind it to an ANYCAST queue (point-to-point, competing consumers). The
//! queue string may be a plain name or an FQQN (`address::queue`). An address
//! that exists only as MULTICAST refuses the attach (startup error). With the
//! broker's default `auto-create-queues`, a missing queue is created on
//! attach (Artemis policy, not ours).
//!
//! ## Dead-letter
//! `rejected` is terminal: Artemis moves the message to the address's
//! configured dead-letter address (`artemis create` default: `DLQ` for `#`),
//! annotated `x-opt-ORIG-QUEUE` / `x-opt-ORIG-ADDRESS`. An address with no
//! dead-letter address DROPS a rejected message (like RabbitMQ without a DLX);
//! the reason is on the `REJECTING:` stdout marker and in the `rejected`
//! error description.
//!
//! ## Receive errors
//! Per-message errors leave the link usable and are NOT fatal: a message
//! whose sections the receiver cannot decode (e.g. invalid UTF-8 in an
//! AmqpValue string) arrives unsettled with its delivery info, and one over
//! the link's max-message-size has already been `rejected` by `fe2o3-amqp`.
//! Both reach the core loop as an empty body, so they get the poison-message
//! path (`REJECTING:` marker, counted as rejected) and the undecodable one is
//! `rejected` (dead-lettered) here. Every other receive error (connection,
//! session or link gone; protocol violation) is FATAL: orderly shutdown,
//! exit 255. Unsettled deliveries are redelivered by the broker after the
//! connection drops (verified: delivery-count unchanged).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::RecvError;
use fe2o3_amqp::link::delivery::{Delivery as AmqpDelivery, DeliveryInfo};
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::definitions::{Error as AmqpErrorBody, ErrorCondition};
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Source};
use fe2o3_amqp::types::primitives::{Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Session};
use sz_rust_sdk::prelude::*;
use tracing::{error, info, warn};

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::queue_loop::{self, DeadLetterReason, Delivery, Policy, Transport};
use sz_combined_consumer_core::queue_run::RunOutcome;

use crate::{ActiveMqParams, INSTANCE_NAME};

/// Artemis settle policy: no ack timeout and an uninterruptible engine call,
/// so a long record is only logged, never dead-lettered (SQS parity).
const ACTIVEMQ_POLICY: Policy = Policy {
    dead_letter_long_records: false,
    stuck_records_label: "records",
};

/// Source capability that makes Artemis attach to an ANYCAST queue.
const QUEUE_CAPABILITY: &str = "queue";
/// Error condition on the `rejected` outcome (informational; Artemis
/// dead-letters on `rejected` whatever the condition).
const REJECT_CONDITION: &str = "senzing:rejected-record";
/// Cap on the `rejected` error description, in bytes.
const REJECT_DESCRIPTION_MAX_BYTES: usize = 1024;
/// Local idle timeout: a broker silent for this long (no frames, no
/// heartbeats) fails the connection, which then fails the receive (fatal).
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bound on the orderly link/session/connection close at shutdown.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs the Artemis driver until SIGINT/SIGTERM/SIGHUP, a fatal engine error
/// or the loss of the broker link.
pub async fn run(
    config: &Config,
    params: ActiveMqParams,
    env: Arc<SzEnvironmentCore>,
) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    info!(
        "ActiveMQ threads: {threads} (load-preferring: {}, redo-preferring: {redo_pref}, \
         redo%: {}, broker: {}, queue: {}, prefetch: {})",
        threads - redo_pref,
        config.redo_percent,
        params.url,
        params.queue,
        config.prefetch
    );
    let cap = u32::try_from(config.prefetch).unwrap_or(u32::MAX);
    // The core loop spawns the engine pool BEFORE awaiting this connect (a
    // connect failure then stops it).
    let connect = ActiveMqTransport::connect(params, cap);
    queue_loop::run(config, env, connect, ACTIVEMQ_POLICY).await
}

/// One AMQP connection + session + receiver link on the source queue.
pub struct ActiveMqTransport {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    receiver: Receiver,
    /// Received deliveries the core loop has yet to settle, by synthetic
    /// tag; `None` when the link already settled it (oversized message).
    unsettled: HashMap<u64, Option<DeliveryInfo>>,
    next_tag: u64,
    /// Total in-flight cap (`--prefetch`).
    cap: u32,
    /// Set by `stop_intake`: credit goes to 0 at the next settle.
    intake_stopped: bool,
    /// Credit 0 already granted after `stop_intake` (no repeat flows).
    credit_closed: bool,
}

impl ActiveMqTransport {
    /// Opens the connection (SASL PLAIN when credentials are set), a session
    /// and a manual-credit receiver on `params.queue`, then grants `cap`
    /// credit.
    async fn connect(params: ActiveMqParams, cap: u32) -> Result<Self> {
        info!("Connecting to ActiveMQ at {}", params.url);
        let container_id = format!("{INSTANCE_NAME}-{}", std::process::id());
        let mut builder = Connection::builder()
            .container_id(container_id)
            .idle_time_out(IDLE_TIMEOUT.as_millis() as u32);
        if let Some(c) = params.credentials {
            builder = builder.sasl_profile(SaslProfile::Plain {
                username: c.user,
                password: c.password,
            });
        }
        let mut connection = builder
            .open(params.url.as_str())
            .await
            .context("failed to connect to ActiveMQ")?;
        let mut session = Session::begin(&mut connection)
            .await
            .context("failed to begin AMQP session")?;
        let source = Source::builder()
            .address(params.queue.clone())
            .capabilities(vec![Symbol::from(QUEUE_CAPABILITY)])
            .build();
        let mut receiver = Receiver::builder()
            .name(format!("{INSTANCE_NAME}-{}", params.queue))
            .source(source)
            .credit_mode(CreditMode::Manual)
            .auto_accept(false)
            .attach(&mut session)
            .await
            .with_context(|| format!("failed to attach a receiver to queue '{}'", params.queue))?;
        receiver
            .set_credit(cap)
            .await
            .context("failed to grant link credit")?;
        Ok(Self {
            connection,
            session,
            receiver,
            unsettled: HashMap::new(),
            next_tag: 1,
            cap,
            intake_stopped: false,
            credit_closed: false,
        })
    }

    /// Re-grants credit so `unsettled + credit == cap` (0 once intake has
    /// stopped). AMQP credit is relative to the receiver's delivery count, so
    /// deliveries already in transit are covered and the broker never has more
    /// than `cap` outstanding to this link.
    async fn replenish(&mut self) {
        if self.credit_closed {
            return;
        }
        let credit = if self.intake_stopped {
            self.credit_closed = true;
            0
        } else {
            let unsettled = u32::try_from(self.unsettled.len()).unwrap_or(u32::MAX);
            self.cap.saturating_sub(unsettled)
        };
        if let Err(e) = self.receiver.set_credit(credit).await {
            warn!("AMQP flow (credit {credit}) failed: {e}");
        }
    }

    /// Removes `tag` from the unsettled map, logging an unknown tag. `None`
    /// also when the link already settled the delivery (nothing to send).
    fn take(&mut self, tag: u64, verb: &str) -> Option<DeliveryInfo> {
        let entry = self.unsettled.remove(&tag);
        if entry.is_none() {
            warn!("no unsettled AMQP delivery {tag}; cannot {verb}");
        }
        entry.flatten()
    }
}

/// The record bytes of an AMQP body: `Data` sections (concatenated) or an
/// `AmqpValue` string (a JMS `TextMessage`) or binary. Anything else yields
/// an empty body, which the core loop dead-letters as malformed.
fn body_bytes(body: Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => batch.into_iter().flat_map(|d| d.0.into_vec()).collect(),
        Body::Value(AmqpValue(Value::String(s))) => s.into_bytes(),
        Body::Value(AmqpValue(Value::Binary(b))) => b.into_vec(),
        other => {
            warn!("unsupported AMQP body (expected Data or a string AmqpValue): {other:?}");
            Vec::new()
        }
    }
}

/// Maps one receive result to what the core loop gets: the delivery info to
/// settle (`None`: the link already settled it) and the record bytes.
/// Per-message errors leave the link usable, so they become an empty body
/// (dead-lettered as malformed); any other error is fatal.
fn received(
    result: Result<AmqpDelivery<Body<Value>>, RecvError>,
) -> Result<(Option<DeliveryInfo>, Vec<u8>)> {
    match result {
        Ok(delivery) => {
            let (info, message) = delivery.into_parts();
            Ok((Some(info), body_bytes(message.body)))
        }
        Err(RecvError::MessageDecode(e)) => {
            warn!("undecodable AMQP message (dead-lettering it): {}", e.source);
            Ok((Some(e.info), Vec::new()))
        }
        Err(RecvError::MessageSizeExceeded(e)) => {
            warn!("oversized AMQP message (already rejected by the link): {e}");
            Ok((None, Vec::new()))
        }
        Err(e) => Err(anyhow!("ActiveMQ receive failed (link lost): {e}")),
    }
}

/// `reason` cut on a char boundary to [`REJECT_DESCRIPTION_MAX_BYTES`].
fn reject_description(reason: &str) -> String {
    let mut end = reason.len().min(REJECT_DESCRIPTION_MAX_BYTES);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_string()
}

impl Transport for ActiveMqTransport {
    /// `Receiver::recv` is cancel-safe; everything after it is synchronous.
    async fn recv(&mut self) -> Option<Result<Delivery>> {
        let (info, body) = match received(self.receiver.recv().await) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        let tag = self.next_tag;
        self.next_tag += 1;
        self.unsettled.insert(tag, info);
        Some(Ok(Delivery { tag, body }))
    }

    async fn ack(&mut self, tag: u64) {
        let Some(info) = self.take(tag, "accept") else {
            return;
        };
        if let Err(e) = self.receiver.accept(info).await {
            error!("AMQP accept failed for {tag}: {e}");
        }
        self.replenish().await;
    }

    /// `rejected` outcome: Artemis routes the message to the address's
    /// dead-letter address (dropped if none is configured).
    async fn dead_letter(&mut self, tag: u64, reason: DeadLetterReason) {
        let Some(info) = self.take(tag, "reject") else {
            return;
        };
        let condition = ErrorCondition::Custom(Symbol::from(REJECT_CONDITION));
        let error = AmqpErrorBody::new(
            condition,
            Some(reject_description(&reason.to_string())),
            None,
        );
        if let Err(e) = self.receiver.reject(info, error).await {
            error!("AMQP reject failed for {tag} ({reason}): {e}");
        }
        self.replenish().await;
    }

    /// `released` outcome: immediately redeliverable, and Artemis does not
    /// count it as a delivery attempt. Intake has stopped by now, so the
    /// credit is 0 and the release is not pushed straight back to this link.
    async fn release(&mut self, tag: u64) {
        self.replenish().await;
        let Some(info) = self.take(tag, "release") else {
            return;
        };
        if let Err(e) = self.receiver.release(info).await {
            warn!("AMQP release failed for {tag}: {e}; the broker redelivers it on close");
        }
    }

    fn stop_intake(&mut self) {
        self.intake_stopped = true;
    }

    /// Artemis exposes no queue depth over AMQP (only via management).
    async fn depth(&mut self) -> Option<u32> {
        None
    }

    /// Closes link, session and connection (bounded). Anything still
    /// unsettled (e.g. buffered but never dispatched) is redelivered by the
    /// broker.
    async fn close(mut self) {
        if !self.unsettled.is_empty() {
            info!(
                "leaving {} unsettled AMQP delivery(ies) for redelivery",
                self.unsettled.len()
            );
        }
        let closing = async {
            if let Err(e) = self.receiver.close().await {
                warn!("error closing AMQP receiver: {e}");
            }
            if let Err(e) = self.session.end().await {
                warn!("error ending AMQP session: {e}");
            }
            if let Err(e) = self.connection.close().await {
                warn!("error closing AMQP connection: {e}");
            }
        };
        if tokio::time::timeout(CLOSE_TIMEOUT, closing).await.is_err() {
            warn!("AMQP close timed out after {CLOSE_TIMEOUT:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp::link::{LinkStateError, MessageSizeExceeded};
    use fe2o3_amqp::types::messaging::Data;
    use fe2o3_amqp::types::primitives::Binary;

    #[test]
    fn data_sections_are_concatenated() {
        let body: Body<Value> = Body::Data(
            vec![
                Data(Binary::from(b"{\"A\":".to_vec())),
                Data(Binary::from(b"1}".to_vec())),
            ]
            .into(),
        );
        assert_eq!(body_bytes(body), b"{\"A\":1}");
    }

    #[test]
    fn string_and_binary_values_are_accepted() {
        let text = Body::Value(AmqpValue(Value::String("{\"A\":1}".into())));
        assert_eq!(body_bytes(text), b"{\"A\":1}");
        let bin = Body::Value(AmqpValue(Value::Binary(Binary::from(b"x".to_vec()))));
        assert_eq!(body_bytes(bin), b"x");
    }

    #[test]
    fn other_bodies_become_empty_poison() {
        assert!(body_bytes(Body::Value(AmqpValue(Value::Long(7)))).is_empty());
        assert!(body_bytes(Body::Empty).is_empty());
    }

    #[test]
    fn oversized_message_is_a_poison_delivery_already_settled() {
        let oversized = RecvError::MessageSizeExceeded(MessageSizeExceeded {
            size: 2048,
            max_size: 1024,
        });
        let (info, body) = received(Err(oversized)).expect("not fatal");
        assert!(info.is_none(), "the link already rejected it");
        assert!(body.is_empty(), "empty body: dead-lettered as malformed");
    }

    #[test]
    fn link_level_receive_errors_are_fatal() {
        for e in [
            RecvError::LinkStateError(LinkStateError::RemoteClosed),
            RecvError::LinkStateError(LinkStateError::RemoteDetached),
            RecvError::TransferLimitExceeded,
            RecvError::DeliveryIdIsNone,
        ] {
            let err = received(Err(e)).expect_err("fatal");
            assert!(err.to_string().contains("link lost"), "{err}");
        }
    }

    #[test]
    fn reject_description_is_capped_on_a_char_boundary() {
        assert_eq!(reject_description("short"), "short");
        let long = "é".repeat(REJECT_DESCRIPTION_MAX_BYTES);
        let cut = reject_description(&long);
        assert!(cut.len() <= REJECT_DESCRIPTION_MAX_BYTES);
        assert!(cut.chars().all(|c| c == 'é'));
    }
}
