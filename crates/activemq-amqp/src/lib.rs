//! Apache ActiveMQ Artemis AMQP 1.0 plumbing shared by the ActiveMQ consumer
//! (`sz_activemq_combined_consumer`) and publisher (`sz_activemq_publisher`),
//! so both sides agree on:
//!
//! * the broker URL and SASL PLAIN credential resolution
//!   (`SENZING_ACTIVEMQ_URL` / `_USER` / `_PASSWORD` semantics:
//!   flag/env credentials override percent-decoded URL userinfo, and the URL
//!   is stripped of credentials so it is safe to log);
//! * the queue name (plain or FQQN `address::queue`) and the `queue`
//!   terminus capability that makes Artemis use an ANYCAST queue;
//! * the record body: the publisher sends one `Data` section holding the raw
//!   record bytes ([`record_message`]); the consumer accepts `Data`
//!   sections or an `AmqpValue` string/binary ([`body_bytes`]).
//!
//! No Senzing dependency: the publisher links neither libSz nor the core.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use fe2o3_amqp::Connection;
use fe2o3_amqp::connection::{ConnectionHandle, OpenError};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Data, Header, Message, Source, Target};
use fe2o3_amqp::types::primitives::{Binary, Symbol, Value};
use percent_encoding::percent_decode_str;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tracing::warn;
use url::Url;

/// Terminus capability that makes Artemis attach to (or auto-create) an
/// ANYCAST queue. A plain terminus would auto-create a MULTICAST address the
/// consumer could not attach to.
pub const QUEUE_CAPABILITY: &str = "queue";

/// Separator of an Artemis fully qualified queue name (`address::queue`).
pub const FQQN_SEPARATOR: &str = "::";

/// Local idle timeout: a broker silent for this long (no frames, no
/// heartbeats) fails the connection. Artemis heartbeats at half of it, so a
/// link whose credit is withheld (address full, BLOCK policy) stays open.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// SASL PLAIN credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    /// Never prints the password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Credentials {{ user: {:?}, password: *** }}", self.user)
    }
}

/// Validated broker parameters.
#[derive(Debug, Clone)]
pub struct BrokerParams {
    /// Broker URL with any embedded credentials removed.
    pub url: Url,
    /// `None` = no SASL PLAIN (anonymous).
    pub credentials: Option<Credentials>,
    /// Queue name or FQQN, used as the link's source/target address.
    pub queue: String,
}

/// Parses an `amqp://` / `amqps://` URL and resolves the SASL credentials:
/// `user` / `password` (env/flag) each override the URL's (percent-decoded)
/// userinfo. Returns the URL stripped of credentials (so it is safe to log).
pub fn parse_broker_url(
    raw: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> Result<(Url, Option<Credentials>), String> {
    let mut url = Url::parse(raw).map_err(|e| format!("invalid ActiveMQ URL: {e}"))?;
    if !matches!(url.scheme(), "amqp" | "amqps") {
        return Err(format!(
            "ActiveMQ URL must use amqp:// or amqps://, got {}://",
            url.scheme()
        ));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err("ActiveMQ URL has no host".to_string());
    }
    let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
    let url_user = Some(decode(url.username())).filter(|s| !s.is_empty());
    let url_password = url.password().map(decode);
    // Strip userinfo: credentials travel only in the SASL profile. Both
    // setters only fail for a cannot-be-a-base URL, excluded by the host check.
    let _ = url.set_username("");
    let _ = url.set_password(None);

    let user = user.map(str::to_string).or(url_user);
    let password = password.map(str::to_string).or(url_password);
    let credentials = match (user.filter(|u| !u.is_empty()), password) {
        (Some(user), password) => Some(Credentials {
            user,
            password: password.unwrap_or_default(),
        }),
        (None, Some(_)) => {
            return Err(
                "ActiveMQ password given without a user (set SENZING_ACTIVEMQ_USER or put \
                 user:password in the URL)"
                    .to_string(),
            );
        }
        (None, None) => None,
    };
    Ok((url, credentials))
}

/// Accepts a plain queue name or an Artemis FQQN (`address::queue`, both
/// parts non-empty).
pub fn validate_queue(queue: &str) -> Result<String, String> {
    if let Some((address, name)) = queue.split_once(FQQN_SEPARATOR)
        && (address.is_empty() || name.is_empty() || name.contains(FQQN_SEPARATOR))
    {
        return Err(format!(
            "invalid fully qualified queue name {queue:?}: expected address::queue"
        ));
    }
    Ok(queue.to_string())
}

/// Opens an AMQP connection to `params.url` (SASL PLAIN when credentials are
/// set) with the shared idle timeout.
pub async fn open_connection(
    params: &BrokerParams,
    container_id: String,
) -> Result<ConnectionHandle<()>, OpenError> {
    let mut builder = Connection::builder()
        .container_id(container_id)
        .idle_time_out(IDLE_TIMEOUT.as_millis() as u32);
    if let Some(c) = &params.credentials {
        builder = builder.sasl_profile(SaslProfile::Plain {
            username: c.user.clone(),
            password: c.password.clone(),
        });
    }
    builder.open(params.url.as_str()).await
}

/// AMQP 1.0 default `max-frame-size` when the peer's Open omits it.
const DEFAULT_REMOTE_MAX_FRAME_SIZE: u32 = u32::MAX;
/// Bytes the [`OpenObserver`] buffers while looking for the peer's Open
/// before giving up (protocol headers + SASL frames + Open are < 1 KiB).
const OPEN_SCAN_LIMIT: usize = 64 * 1024;

/// Opens an `amqp://` connection like [`open_connection`] and also returns
/// the broker's advertised `max-frame-size` (from its Open frame): the
/// largest frame this client may send.
///
/// fe2o3-amqp 0.18.2 does not expose the negotiated value, so the TCP stream
/// is wrapped in an [`OpenObserver`] that reads it off the wire. `amqps://`
/// is not supported here (fe2o3 layers TLS inside the connection, so the
/// plaintext Open is not visible to a stream wrapper).
pub async fn open_connection_observed(
    params: &BrokerParams,
    container_id: String,
) -> Result<(ConnectionHandle<()>, u32), OpenError> {
    let url = &params.url;
    if url.scheme() != "amqp" {
        return Err(OpenError::InvalidScheme);
    }
    let addrs = url.socket_addrs(|| Some(5672))?;
    let stream = TcpStream::connect(&*addrs).await?;
    let observed = Arc::new(OnceLock::new());
    let stream = OpenObserver::new(stream, observed.clone());
    let host = url.host_str();
    let mut builder = Connection::builder()
        .container_id(container_id)
        .scheme("amqp")
        .hostname(host)
        .sasl_hostname(host)
        .idle_time_out(IDLE_TIMEOUT.as_millis() as u32);
    if let Some(c) = &params.credentials {
        builder = builder.sasl_profile(SaslProfile::Plain {
            username: c.user.clone(),
            password: c.password.clone(),
        });
    }
    let connection = builder.open_with_stream(stream).await?;
    // The connection is open only after the peer's Open was read, so the
    // observer has always seen it; a peer that sent none could not get here.
    let max_frame = observed.get().copied().ok_or(OpenError::IllegalState)?;
    Ok((connection, max_frame))
}

/// Transparent stream wrapper that reads the peer's AMQP Open frame as it
/// passes and records its `max-frame-size` (default `u32::MAX` when absent).
/// Scanning stops after the Open (or [`OPEN_SCAN_LIMIT`] bytes); after that
/// it is a plain pass-through.
#[derive(Debug)]
pub struct OpenObserver<S> {
    inner: S,
    seen: Vec<u8>,
    pos: usize,
    done: bool,
    out: Arc<OnceLock<u32>>,
}

impl<S> OpenObserver<S> {
    pub fn new(inner: S, out: Arc<OnceLock<u32>>) -> Self {
        Self {
            inner,
            seen: Vec::new(),
            pos: 0,
            done: false,
            out,
        }
    }

    fn observe(&mut self, bytes: &[u8]) {
        if self.done {
            return;
        }
        self.seen.extend_from_slice(bytes);
        loop {
            let rest = &self.seen[self.pos..];
            if rest.len() < 8 {
                break;
            }
            if rest.starts_with(b"AMQP") {
                self.pos += 8; // protocol header (AMQP or SASL layer)
                continue;
            }
            let size = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
            if size < 8 || rest.len() < size {
                break;
            }
            let (doff, frame_type) = (rest[4] as usize * 4, rest[5]);
            // Type 0 = AMQP frame (1 = SASL); the first one is the Open.
            if frame_type == 0 && doff <= size {
                let _ = self
                    .out
                    .set(open_max_frame_size(&rest[doff..size]).unwrap_or(0));
                self.finish();
                return;
            }
            self.pos += size;
        }
        if self.seen.len() > OPEN_SCAN_LIMIT {
            self.finish();
        }
    }

    fn finish(&mut self) {
        self.done = true;
        self.seen = Vec::new();
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for OpenObserver<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && !self.done {
            let new = buf.filled()[before..].to_vec();
            self.observe(&new);
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for OpenObserver<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// `max-frame-size` (field 2) of an encoded Open performative (frame body);
/// `None` if `body` is not a decodable Open. Only the first three list fields
/// are read: container-id (string), hostname (string or null), max-frame-size
/// (uint or null; null/absent = `u32::MAX`).
fn open_max_frame_size(body: &[u8]) -> Option<u32> {
    // Described type: 0x00, descriptor smallulong 0x53 0x10 (open).
    let rest = body.strip_prefix(&[0x00, 0x53, 0x10])?;
    let (count, mut fields) = match *rest.first()? {
        0x45 => (0, &rest[1..]),                          // list0
        0xc0 => (*rest.get(2)? as usize, rest.get(3..)?), // list8: size, count
        0xd0 => (
            u32::from_be_bytes(rest.get(5..9)?.try_into().ok()?) as usize,
            rest.get(9..)?,
        ),
        _ => return None,
    };
    // Skip container-id and hostname.
    for i in 0..2 {
        if i >= count {
            return Some(DEFAULT_REMOTE_MAX_FRAME_SIZE);
        }
        let len = match *fields.first()? {
            0x40 => 0,
            0xa1 => 1 + *fields.get(1)? as usize,
            0xb1 => 4 + u32::from_be_bytes(fields.get(1..5)?.try_into().ok()?) as usize,
            _ => return None,
        };
        fields = fields.get(1 + len..)?;
    }
    if count < 3 {
        return Some(DEFAULT_REMOTE_MAX_FRAME_SIZE);
    }
    match *fields.first()? {
        0x40 => Some(DEFAULT_REMOTE_MAX_FRAME_SIZE),
        0x43 => Some(0),
        0x52 => fields.get(1).map(|&b| u32::from(b)),
        0x70 => Some(u32::from_be_bytes(fields.get(1..5)?.try_into().ok()?)),
        _ => None,
    }
}

/// Whether an open error can never succeed by retrying (bad credentials, bad
/// URL/scheme/TLS setup) — as opposed to a transport error worth a retry.
pub fn open_error_is_permanent(e: &OpenError) -> bool {
    matches!(
        e,
        OpenError::SaslError { .. }
            | OpenError::UrlError(_)
            | OpenError::InvalidDomain
            | OpenError::InvalidScheme
            | OpenError::TlsConnectorNotFound
            | OpenError::InvalidSchemeForSaslExternal
    )
}

/// Receiver source on the ANYCAST queue `queue`.
pub fn queue_source(queue: &str) -> Source {
    Source::builder()
        .address(queue.to_string())
        .capabilities(vec![Symbol::from(QUEUE_CAPABILITY)])
        .build()
}

/// Sender target on the ANYCAST queue `queue`.
pub fn queue_target(queue: &str) -> Target {
    Target::builder()
        .address(queue.to_string())
        .capabilities(vec![Symbol::from(QUEUE_CAPABILITY)])
        .build()
}

/// The message for one record: a single `Data` section holding the record
/// bytes verbatim (what [`body_bytes`] decodes back), header `durable` set
/// unless `durable` is false.
pub fn record_message(record: Vec<u8>, durable: bool) -> Message<Data> {
    Message::builder()
        .header(Header::builder().durable(durable).build())
        .data(Binary::from(record))
        .build()
}

/// The record bytes of an AMQP body: `Data` sections (concatenated) or an
/// `AmqpValue` string (a JMS `TextMessage`) or binary. Anything else yields
/// an empty body, which the consumer dead-letters as malformed.
pub fn body_bytes(body: Body<Value>) -> Vec<u8> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn published_record_decodes_to_the_same_bytes() {
        // The publisher's encoding must be exactly what the consumer decodes.
        let line = "{\"DATA_SOURCE\":\"T\",\"RECORD_ID\":\"1\",\"NAME_FULL\":\"Zoë\"}";
        let message = record_message(line.as_bytes().to_vec(), true);
        let body: Body<Value> = Body::Data(vec![message.body].into());
        assert_eq!(body_bytes(body), line.as_bytes());
    }

    #[test]
    fn durable_flag_maps_to_the_header() {
        let durable = record_message(b"x".to_vec(), true);
        assert!(durable.header.expect("header").durable);
        let transient = record_message(b"x".to_vec(), false);
        assert!(!transient.header.expect("header").durable);
    }

    #[test]
    fn queue_termini_carry_the_queue_capability() {
        let cap = Some(vec![Symbol::from(QUEUE_CAPABILITY)].into());
        let s = queue_source("a::q");
        assert_eq!(s.capabilities, cap);
        let t = queue_target("a::q");
        assert_eq!(t.capabilities, cap);
        assert_eq!(t.address.as_deref(), Some("a::q"));
    }

    #[test]
    fn url_credentials_are_decoded_and_stripped() {
        let (url, c) = parse_broker_url("amqp://us%40er:p%3Ass@host:5673", None, None).unwrap();
        assert_eq!(url.as_str(), "amqp://host:5673");
        let c = c.expect("credentials from URL");
        assert_eq!((c.user.as_str(), c.password.as_str()), ("us@er", "p:ss"));
    }

    #[test]
    fn user_and_password_override_url_userinfo() {
        let (_, c) = parse_broker_url("amqp://a:b@h", Some("u"), None).unwrap();
        assert_eq!(
            c,
            Some(Credentials {
                user: "u".into(),
                password: "b".into()
            })
        );
        let (_, c) = parse_broker_url("amqps://a:b@h", None, Some("p")).unwrap();
        assert_eq!(c.map(|c| c.password), Some("p".into()));
        let (_, c) = parse_broker_url("amqp://h", Some("u"), Some("p")).unwrap();
        assert_eq!(c.map(|c| c.user), Some("u".into()));
    }

    #[test]
    fn anonymous_without_any_credentials() {
        let (url, c) = parse_broker_url("amqp://localhost:5672", None, None).unwrap();
        assert_eq!(
            (url.host_str(), url.port(), c),
            (Some("localhost"), Some(5672), None)
        );
    }

    #[test]
    fn password_without_user_is_an_error() {
        let err = parse_broker_url("amqp://h", None, Some("p")).unwrap_err();
        assert!(err.contains("without a user"), "{err}");
        assert!(parse_broker_url("amqp://:p@h", None, None).is_err());
    }

    #[test]
    fn url_scheme_and_host_are_validated() {
        assert!(
            parse_broker_url("http://h", None, None)
                .unwrap_err()
                .contains("amqp://")
        );
        assert!(parse_broker_url("not a url", None, None).is_err());
        assert!(parse_broker_url("amqp://", None, None).is_err());
    }

    #[test]
    fn credentials_debug_hides_the_password() {
        let c = Credentials {
            user: "u".into(),
            password: "secret".into(),
        };
        assert!(!format!("{c:?}").contains("secret"));
    }

    #[test]
    fn queue_accepts_plain_and_fqqn() {
        assert_eq!(validate_queue("q").unwrap(), "q");
        assert_eq!(validate_queue("addr::q").unwrap(), "addr::q");
        for bad in ["::q", "addr::", "a::b::c"] {
            assert!(validate_queue(bad).is_err(), "{bad}");
        }
    }

    /// Real Artemis 2.57 Open frame body (captured on the wire, truncated
    /// after the fields read), acceptor default max frame 128 KiB.
    const ARTEMIS_OPEN_128K: &str = "005310c0c90aa12437333365626533372d633562352d313166312d383835302d3865\
        3765653566393764393640700002000060ffff70000075304040e0";

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn open_max_frame_size_decodes_artemis_open() {
        let body = hex(ARTEMIS_OPEN_128K);
        assert_eq!(open_max_frame_size(&body), Some(131_072));
        let mut big = body.clone();
        let at = big
            .windows(5)
            .position(|w| w == [0x70, 0, 2, 0, 0])
            .unwrap();
        big[at + 1..at + 5].copy_from_slice(&268_435_456u32.to_be_bytes());
        assert_eq!(open_max_frame_size(&big), Some(268_435_456));
    }

    #[test]
    fn open_max_frame_size_defaults_and_rejects() {
        // list8 with only container-id: max-frame-size absent = u32::MAX.
        let short = [0x00, 0x53, 0x10, 0xc0, 0x04, 0x01, 0xa1, 0x01, b'c'];
        assert_eq!(open_max_frame_size(&short), Some(u32::MAX));
        // Explicit null.
        let null = [
            0x00, 0x53, 0x10, 0xc0, 0x06, 0x03, 0xa1, 0x01, b'c', 0x40, 0x40,
        ];
        assert_eq!(open_max_frame_size(&null), Some(u32::MAX));
        // Not an Open (a Begin).
        assert_eq!(open_max_frame_size(&[0x00, 0x53, 0x11, 0x45]), None);
    }

    #[test]
    fn observer_finds_the_open_across_split_reads() {
        // AMQP SASL header, a SASL frame, AMQP header, the Open frame, split
        // at every byte boundary.
        let open_body = hex(ARTEMIS_OPEN_128K);
        let mut wire = b"AMQP\x03\x01\x00\x00".to_vec();
        wire.extend_from_slice(&[0, 0, 0, 12, 2, 1, 0, 0, 0x00, 0x53, 0x44, 0x45]);
        wire.extend_from_slice(b"AMQP\x00\x01\x00\x00");
        let size = (8 + open_body.len()) as u32;
        wire.extend_from_slice(&size.to_be_bytes());
        wire.extend_from_slice(&[2, 0, 0, 0]);
        wire.extend_from_slice(&open_body);
        let out = Arc::new(OnceLock::new());
        let mut obs = OpenObserver::new((), out.clone());
        for b in &wire {
            obs.observe(std::slice::from_ref(b));
        }
        assert_eq!(out.get(), Some(&131_072));
        assert!(obs.done && obs.seen.is_empty(), "scan stops after the Open");
    }

    #[test]
    fn auth_and_url_open_errors_are_permanent_io_is_not() {
        assert!(open_error_is_permanent(&OpenError::InvalidScheme));
        let io = OpenError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(!open_error_is_permanent(&io));
    }
}
