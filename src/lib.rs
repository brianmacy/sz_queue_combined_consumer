//! Combined RabbitMQ load + redo Senzing driver.
//!
//! One binary subsumes both `sz_rabbit_consumer` (load: `add_record` from a
//! RabbitMQ queue) and `sz_simple_redoer` (redo: `process_redo_record` from the
//! engine's own redo queue), governed by a single `SENZING_REDO_PERCENT`
//! parameter in `[0, 100]`:
//!
//! * `0`   — pure loader: no redo fetcher is started, zero redo-related calls.
//! * `100` — pure redoer: the AMQP connection (and the tokio runtime) is never
//!   opened; the binary degenerates to the redoer's pure `std::thread` shape.
//! * `(0, 100)` — combined: one worker pool services both work types. A static
//!   split of the pool into redo-preferring and load-preferring workers (with
//!   cross-over fallback when the preferred channel is empty) enforces the redo
//!   share as a capacity floor while the MQ is busy and drains redo with the
//!   full pool once the MQ empties — with no explicit mode state machine.
//!
//! Design: `~/.claude/plans/dbperf_combined_consumer_design.md`. The AMQP layer
//! is inherited from `sz_rabbit_consumer_rust` (tokio + lapin, single
//! connection/channel, acks only on the async task); the redo side is inherited
//! from `sz_simple_redoer_rust` (single serial fetcher thread feeding a small
//! bounded channel). The Senzing SDK requires synchronous engine calls on OS
//! threads, one `Sz_init` per process, with each thread deriving its own engine
//! handle — see the reference drivers and the dbperf-faq
//! `architecture/consumer-redoer-concurrency` article.

pub mod combined;
pub mod config;
pub mod pure_redoer;
pub mod record;
pub mod redo;
pub mod stats;
pub mod worker;

pub use config::{Args, Config};

/// Instance/module name passed to the Senzing environment.
pub const INSTANCE_NAME: &str = "sz_rabbit_combined_consumer";
