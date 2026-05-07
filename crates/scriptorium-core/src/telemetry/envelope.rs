#![allow(clippy::doc_markdown)]

//! OTel-shaped envelope types for Scriptorium telemetry.
//!
//! This module is the single source of truth for the in-process
//! representation of log records and spans. Every other telemetry module
//! (schema, store, tracing layer, CLI `log emit`) consumes these types.
//!
//! The types are deliberately modeled to mirror the OpenTelemetry data
//! models — without taking a runtime dependency on the `opentelemetry-sdk`
//! crate — so that rows written to SQLite can be exported to any OTLP
//! collector later without a schema migration.
//!
//! Spec references:
//!
//! - Logs data model (severity numbers):
//!   <https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber>
//! - Trace API (TraceId + SpanId):
//!   <https://opentelemetry.io/docs/specs/otel/trace/api/#retrieving-the-traceid-and-spanid>
//! - SpanKind:
//!   <https://opentelemetry.io/docs/specs/otel/trace/api/#spankind>
//! - Status:
//!   <https://opentelemetry.io/docs/specs/otel/trace/api/#set-status>

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};

/// Canonical deterministic attribute map.
///
/// [`BTreeMap`] guarantees sorted-key iteration, which makes JSON
/// serialization canonical — important for deduplication via SHA-256 hash
/// and for stable diffs in tests.
pub type Attributes = BTreeMap<String, serde_json::Value>;

// ── Source ───────────────────────────────────────────────────────────────

/// Origin of a telemetry record. Closed enum: runtime mutation is
/// impossible, so downstream queries can rely on a finite domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Hook,
    Cli,
    Mcp,
    Core,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Hook => "hook",
            Self::Cli => "cli",
            Self::Mcp => "mcp",
            Self::Core => "core",
        })
    }
}

/// Parse error for [`Source::from_str`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseSourceError(pub String);

impl fmt::Display for ParseSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid Source: {:?}", self.0)
    }
}

impl std::error::Error for ParseSourceError {}

impl FromStr for Source {
    type Err = ParseSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hook" => Ok(Self::Hook),
            "cli" => Ok(Self::Cli),
            "mcp" => Ok(Self::Mcp),
            "core" => Ok(Self::Core),
            other => Err(ParseSourceError(other.to_string())),
        }
    }
}

// ── SpanKind ─────────────────────────────────────────────────────────────

/// OTel [`SpanKind`] values, rendered uppercase per the spec strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl fmt::Display for SpanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Internal => "INTERNAL",
            Self::Server => "SERVER",
            Self::Client => "CLIENT",
            Self::Producer => "PRODUCER",
            Self::Consumer => "CONSUMER",
        })
    }
}

// ── Status ───────────────────────────────────────────────────────────────

/// OTel span [`Status`]. `Error` carries an optional description.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "message")]
pub enum Status {
    #[default]
    Unset,
    Ok,
    Error(String),
}

impl Status {
    /// OTel-spec status code string: one of `"UNSET"`, `"OK"`, `"ERROR"`.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unset => "UNSET",
            Self::Ok => "OK",
            Self::Error(_) => "ERROR",
        }
    }

    /// Human-readable message. `None` for `Unset`/`Ok`.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Error(m) => Some(m.as_str()),
            _ => None,
        }
    }
}

// ── SeverityNumber ───────────────────────────────────────────────────────

/// OTel severity number (1-24). Values outside that range render as
/// `"UNKNOWN"`.
///
/// See <https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeverityNumber(pub u8);

impl SeverityNumber {
    /// Canonical TRACE severity (range 1-4).
    pub const TRACE: Self = Self(1);
    /// Canonical DEBUG severity (range 5-8).
    pub const DEBUG: Self = Self(5);
    /// Canonical INFO severity (range 9-12).
    pub const INFO: Self = Self(9);
    /// Canonical WARN severity (range 13-16).
    pub const WARN: Self = Self(13);
    /// Canonical ERROR severity (range 17-20).
    pub const ERROR: Self = Self(17);
    /// Canonical FATAL severity (range 21-24).
    pub const FATAL: Self = Self(21);

    /// Map a `tracing::Level` to the canonical severity number for its
    /// tier. `TRACE` → 1, `DEBUG` → 5, `INFO` → 9, `WARN` → 13, `ERROR` → 17.
    #[must_use]
    pub fn from_tracing_level(level: &tracing::Level) -> Self {
        match *level {
            tracing::Level::TRACE => Self::TRACE,
            tracing::Level::DEBUG => Self::DEBUG,
            tracing::Level::INFO => Self::INFO,
            tracing::Level::WARN => Self::WARN,
            tracing::Level::ERROR => Self::ERROR,
        }
    }

    /// OTel severity text for this number. Returns `"UNKNOWN"` outside 1-24.
    #[must_use]
    pub fn to_text(self) -> &'static str {
        match self.0 {
            1..=4 => "TRACE",
            5..=8 => "DEBUG",
            9..=12 => "INFO",
            13..=16 => "WARN",
            17..=20 => "ERROR",
            21..=24 => "FATAL",
            _ => "UNKNOWN",
        }
    }
}

impl From<u8> for SeverityNumber {
    fn from(n: u8) -> Self {
        Self(n)
    }
}

impl fmt::Display for SeverityNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── ID types ─────────────────────────────────────────────────────────────

/// Error returned when parsing a [`TraceId`] or [`SpanId`] from hex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseIdError {
    /// Wrong hex string length (expected 32 for TraceId, 16 for SpanId).
    InvalidLength { expected: usize, got: usize },
    /// Non-hexadecimal character encountered.
    InvalidHex(char),
}

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, got } => write!(
                f,
                "invalid id length: expected {expected} hex chars, got {got}"
            ),
            Self::InvalidHex(c) => write!(f, "invalid hex character: {c:?}"),
        }
    }
}

impl std::error::Error for ParseIdError {}

fn decode_hex_into(input: &str, out: &mut [u8]) -> Result<(), ParseIdError> {
    let expected = out.len() * 2;
    if input.len() != expected {
        return Err(ParseIdError::InvalidLength {
            expected,
            got: input.len(),
        });
    }
    let bytes = input.as_bytes();
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = hex_nibble(bytes[i * 2])?;
        let lo = hex_nibble(bytes[i * 2 + 1])?;
        *slot = (hi << 4) | lo;
    }
    Ok(())
}

fn hex_nibble(b: u8) -> Result<u8, ParseIdError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        other => Err(ParseIdError::InvalidHex(other as char)),
    }
}

/// 128-bit OTel trace identifier. Rendered as 32 lowercase hex chars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    /// All-zero trace id (invalid per OTel spec; useful as a sentinel).
    pub const ZERO: Self = Self([0u8; 16]);

    /// Generate a new trace id from the operating system's CSPRNG.
    #[must_use]
    pub fn new_random() -> Self {
        let mut buf = [0u8; 16];
        OsRng.fill_bytes(&mut buf);
        Self(buf)
    }

    /// Returns `true` if every byte is zero (the invalid sentinel value).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|b| *b == 0)
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for TraceId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0u8; 16];
        decode_hex_into(s, &mut out)?;
        Ok(Self(out))
    }
}

/// 64-bit OTel span identifier. Rendered as 16 lowercase hex chars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpanId(pub [u8; 8]);

impl SpanId {
    /// All-zero span id (invalid per OTel spec; useful as a sentinel).
    pub const ZERO: Self = Self([0u8; 8]);

    /// Generate a new span id from the operating system's CSPRNG.
    #[must_use]
    pub fn new_random() -> Self {
        let mut buf = [0u8; 8];
        OsRng.fill_bytes(&mut buf);
        Self(buf)
    }

    /// Returns `true` if every byte is zero (the invalid sentinel value).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|b| *b == 0)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for SpanId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0u8; 8];
        decode_hex_into(s, &mut out)?;
        Ok(Self(out))
    }
}

// ── Payload caps ─────────────────────────────────────────────────────────

/// Size limits for log/span payload strings. Enforced by the payload
/// module (T8) before rows are written to SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadCap {
    /// Maximum byte length of `LogRecord::body` after UTF-8 encoding.
    pub max_body_bytes: usize,
    /// Maximum byte length of a single attribute value after JSON
    /// encoding.
    pub max_attr_value_bytes: usize,
}

/// Default payload cap: 8 KiB body, 4 KiB attr value.
pub const DEFAULT_PAYLOAD_CAP: PayloadCap = PayloadCap {
    max_body_bytes: 8192,
    max_attr_value_bytes: 4096,
};

/// Env var that overrides [`PayloadCap::max_body_bytes`].
pub const ENV_MAX_BODY: &str = "SCRIPTORIUM_TELEMETRY_MAX_BODY";
/// Env var that overrides [`PayloadCap::max_attr_value_bytes`].
pub const ENV_MAX_ATTR: &str = "SCRIPTORIUM_TELEMETRY_MAX_ATTR";

/// Load a [`PayloadCap`] from environment variables, falling back to
/// [`DEFAULT_PAYLOAD_CAP`] for any missing or unparseable value.
#[must_use]
pub fn payload_cap_from_env() -> PayloadCap {
    let max_body_bytes = std::env::var(ENV_MAX_BODY)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_PAYLOAD_CAP.max_body_bytes);
    let max_attr_value_bytes = std::env::var(ENV_MAX_ATTR)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_PAYLOAD_CAP.max_attr_value_bytes);
    PayloadCap {
        max_body_bytes,
        max_attr_value_bytes,
    }
}

// ── Time helper ──────────────────────────────────────────────────────────

fn now_unix_nanos() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

// ── LogRecord ────────────────────────────────────────────────────────────

/// OTel-shaped log record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogRecord {
    /// Event time, nanoseconds since the Unix epoch.
    pub time_unix_nano: i64,
    /// Time the record was observed by the collector, nanoseconds since
    /// the Unix epoch. Usually equal to `time_unix_nano` at emit time.
    pub observed_time_unix_nano: i64,
    /// OTel severity number (1-24).
    pub severity_number: SeverityNumber,
    /// Severity text (`"INFO"`, `"ERROR"`, etc.). `&'static str` because
    /// the set is closed per OTel spec. On deserialize, the value is
    /// always recomputed from `severity_number` — any incoming string is
    /// ignored.
    pub severity_text: &'static str,
    /// Human-readable log body.
    pub body: String,
    /// Associated trace id, if emitted from within a span context.
    #[serde(default)]
    pub trace_id: Option<TraceId>,
    /// Associated span id, if emitted from within a span context.
    #[serde(default)]
    pub span_id: Option<SpanId>,
    /// Structured attributes. Uses [`BTreeMap`] for deterministic
    /// serialization.
    #[serde(default)]
    pub attributes: Attributes,
    /// Record origin.
    pub source: Source,
    /// Foreign key to `telemetry_resource.id`. Caller fills this in after
    /// constructing the record; the `0` default is a placeholder.
    #[serde(default)]
    pub resource_id: i64,
}

#[derive(Deserialize)]
struct LogRecordWire {
    time_unix_nano: i64,
    observed_time_unix_nano: i64,
    severity_number: SeverityNumber,
    #[serde(default)]
    #[allow(dead_code)]
    severity_text: Option<String>,
    body: String,
    #[serde(default)]
    trace_id: Option<TraceId>,
    #[serde(default)]
    span_id: Option<SpanId>,
    #[serde(default)]
    attributes: Attributes,
    source: Source,
    #[serde(default)]
    resource_id: i64,
}

impl From<LogRecordWire> for LogRecord {
    fn from(w: LogRecordWire) -> Self {
        Self {
            time_unix_nano: w.time_unix_nano,
            observed_time_unix_nano: w.observed_time_unix_nano,
            severity_number: w.severity_number,
            severity_text: w.severity_number.to_text(),
            body: w.body,
            trace_id: w.trace_id,
            span_id: w.span_id,
            attributes: w.attributes,
            source: w.source,
            resource_id: w.resource_id,
        }
    }
}

impl<'de> Deserialize<'de> for LogRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        LogRecordWire::deserialize(deserializer).map(Into::into)
    }
}

impl LogRecord {
    /// Construct an INFO-severity record with `body` and `source`.
    /// Timestamps are set to "now", trace/span ids and attributes are
    /// empty, and `resource_id` is `0` (the caller fills it in).
    #[must_use]
    pub fn info(body: impl Into<String>, source: Source) -> Self {
        Self::with_severity(body, SeverityNumber::INFO, source)
    }

    /// Construct a record at an explicit severity.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the current wall clock is on or after the Unix
    /// epoch; in practice this is always true.
    #[must_use]
    pub fn with_severity(
        body: impl Into<String>,
        severity: SeverityNumber,
        source: Source,
    ) -> Self {
        let now = now_unix_nanos();
        debug_assert!(now >= 0, "wall clock is before Unix epoch");
        Self {
            time_unix_nano: now,
            observed_time_unix_nano: now,
            severity_number: severity,
            severity_text: severity.to_text(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            attributes: Attributes::new(),
            source,
            resource_id: 0,
        }
    }
}

// ── Span ─────────────────────────────────────────────────────────────────

/// OTel span event (structured log attached to a span at a point in time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub time_unix_nano: i64,
    #[serde(default)]
    pub attributes: Attributes,
}

/// OTel-shaped span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub span_id: SpanId,
    pub trace_id: TraceId,
    #[serde(default)]
    pub parent_span_id: Option<SpanId>,
    pub name: String,
    pub kind: SpanKind,
    pub start_time_unix_nano: i64,
    #[serde(default)]
    pub end_time_unix_nano: Option<i64>,
    pub status: Status,
    #[serde(default)]
    pub attributes: Attributes,
    #[serde(default)]
    pub events: Vec<SpanEvent>,
    pub source: Source,
    #[serde(default)]
    pub resource_id: i64,
}

impl Span {
    /// Start a new root span. Generates fresh `trace_id` and `span_id`
    /// from the OS CSPRNG, sets `start_time_unix_nano` to "now", and
    /// leaves `end_time_unix_nano = None` and `status = Unset`.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the current wall clock is on or after the Unix
    /// epoch; in practice this is always true.
    #[must_use]
    pub fn start(name: impl Into<String>, kind: SpanKind, source: Source) -> Self {
        let now = now_unix_nanos();
        debug_assert!(now >= 0, "wall clock is before Unix epoch");
        Self {
            span_id: SpanId::new_random(),
            trace_id: TraceId::new_random(),
            parent_span_id: None,
            name: name.into(),
            kind,
            start_time_unix_nano: now,
            end_time_unix_nano: None,
            status: Status::Unset,
            attributes: Attributes::new(),
            events: Vec::new(),
            source,
            resource_id: 0,
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes env-var tests so they don't race each other.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn id_roundtrip() {
        let tid = TraceId::new_random();
        let rendered = tid.to_string();
        assert_eq!(rendered.len(), 32, "TraceId hex should be 32 chars");
        assert!(
            rendered
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "TraceId hex must be lowercase: {rendered}"
        );
        let parsed: TraceId = rendered.parse().expect("parse TraceId");
        assert_eq!(tid.0, parsed.0);

        let sid = SpanId::new_random();
        let rendered = sid.to_string();
        assert_eq!(rendered.len(), 16, "SpanId hex should be 16 chars");
        let parsed: SpanId = rendered.parse().expect("parse SpanId");
        assert_eq!(sid.0, parsed.0);
    }

    #[test]
    fn trace_id_parse_normalizes_case() {
        let upper = "ABCDEF0123456789ABCDEF0123456789";
        let parsed: TraceId = upper.parse().expect("parse upper hex");
        assert_eq!(parsed.to_string(), upper.to_ascii_lowercase());
    }

    #[test]
    fn trace_id_parse_invalid() {
        assert!(matches!(
            "abc".parse::<TraceId>(),
            Err(ParseIdError::InvalidLength {
                expected: 32,
                got: 3
            })
        ));
        let too_short = "a".repeat(31);
        assert!(matches!(
            too_short.parse::<TraceId>(),
            Err(ParseIdError::InvalidLength {
                expected: 32,
                got: 31
            })
        ));
        let too_long = "a".repeat(33);
        assert!(matches!(
            too_long.parse::<TraceId>(),
            Err(ParseIdError::InvalidLength {
                expected: 32,
                got: 33
            })
        ));
        let non_hex = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        assert!(matches!(
            non_hex.parse::<TraceId>(),
            Err(ParseIdError::InvalidHex(_))
        ));
    }

    #[test]
    fn span_id_parse_invalid() {
        assert!(matches!(
            "abc".parse::<SpanId>(),
            Err(ParseIdError::InvalidLength {
                expected: 16,
                got: 3
            })
        ));
        let non_hex = "zzzzzzzzzzzzzzzz";
        assert!(matches!(
            non_hex.parse::<SpanId>(),
            Err(ParseIdError::InvalidHex(_))
        ));
    }

    #[test]
    fn trace_id_is_zero() {
        assert!(TraceId::ZERO.is_zero());
        assert!(!TraceId::new_random().is_zero() || TraceId::new_random().is_zero());
    }

    #[test]
    fn severity_mapping() {
        let table = [
            (0u8, "UNKNOWN"),
            (1, "TRACE"),
            (4, "TRACE"),
            (5, "DEBUG"),
            (8, "DEBUG"),
            (9, "INFO"),
            (10, "INFO"),
            (12, "INFO"),
            (13, "WARN"),
            (16, "WARN"),
            (17, "ERROR"),
            (20, "ERROR"),
            (21, "FATAL"),
            (24, "FATAL"),
            (25, "UNKNOWN"),
            (255, "UNKNOWN"),
        ];
        for (n, expected) in table {
            assert_eq!(
                SeverityNumber::from(n).to_text(),
                expected,
                "SeverityNumber({n}).to_text()"
            );
        }
    }

    #[test]
    fn severity_from_tracing_level() {
        assert_eq!(
            SeverityNumber::from_tracing_level(&tracing::Level::TRACE),
            SeverityNumber::TRACE
        );
        assert_eq!(
            SeverityNumber::from_tracing_level(&tracing::Level::DEBUG),
            SeverityNumber::DEBUG
        );
        assert_eq!(
            SeverityNumber::from_tracing_level(&tracing::Level::INFO),
            SeverityNumber::INFO
        );
        assert_eq!(
            SeverityNumber::from_tracing_level(&tracing::Level::WARN),
            SeverityNumber::WARN
        );
        assert_eq!(
            SeverityNumber::from_tracing_level(&tracing::Level::ERROR),
            SeverityNumber::ERROR
        );
    }

    #[test]
    fn env_cap() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev_body = std::env::var(ENV_MAX_BODY).ok();
        let prev_attr = std::env::var(ENV_MAX_ATTR).ok();

        std::env::set_var(ENV_MAX_BODY, "1024");
        std::env::set_var(ENV_MAX_ATTR, "512");
        let cap = payload_cap_from_env();
        assert_eq!(cap.max_body_bytes, 1024);
        assert_eq!(cap.max_attr_value_bytes, 512);

        std::env::set_var(ENV_MAX_BODY, "not-a-number");
        let cap = payload_cap_from_env();
        assert_eq!(
            cap.max_body_bytes, DEFAULT_PAYLOAD_CAP.max_body_bytes,
            "invalid value should fall back to default"
        );

        match prev_body {
            Some(v) => std::env::set_var(ENV_MAX_BODY, v),
            None => std::env::remove_var(ENV_MAX_BODY),
        }
        match prev_attr {
            Some(v) => std::env::set_var(ENV_MAX_ATTR, v),
            None => std::env::remove_var(ENV_MAX_ATTR),
        }
    }

    #[test]
    fn env_cap_default_when_unset() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev_body = std::env::var(ENV_MAX_BODY).ok();
        let prev_attr = std::env::var(ENV_MAX_ATTR).ok();
        std::env::remove_var(ENV_MAX_BODY);
        std::env::remove_var(ENV_MAX_ATTR);

        let cap = payload_cap_from_env();
        assert_eq!(cap, DEFAULT_PAYLOAD_CAP);

        match prev_body {
            Some(v) => std::env::set_var(ENV_MAX_BODY, v),
            None => std::env::remove_var(ENV_MAX_BODY),
        }
        match prev_attr {
            Some(v) => std::env::set_var(ENV_MAX_ATTR, v),
            None => std::env::remove_var(ENV_MAX_ATTR),
        }
    }

    #[test]
    fn source_display() {
        assert_eq!(Source::Hook.to_string(), "hook");
        assert_eq!(Source::Cli.to_string(), "cli");
        assert_eq!(Source::Mcp.to_string(), "mcp");
        assert_eq!(Source::Core.to_string(), "core");
    }

    #[test]
    fn source_from_str() {
        assert_eq!("hook".parse::<Source>().unwrap(), Source::Hook);
        assert_eq!("cli".parse::<Source>().unwrap(), Source::Cli);
        assert_eq!("mcp".parse::<Source>().unwrap(), Source::Mcp);
        assert_eq!("core".parse::<Source>().unwrap(), Source::Core);
        assert!("HOOK".parse::<Source>().is_err());
        assert!("unknown".parse::<Source>().is_err());
    }

    #[test]
    fn span_kind_display() {
        assert_eq!(SpanKind::Internal.to_string(), "INTERNAL");
        assert_eq!(SpanKind::Server.to_string(), "SERVER");
        assert_eq!(SpanKind::Client.to_string(), "CLIENT");
        assert_eq!(SpanKind::Producer.to_string(), "PRODUCER");
        assert_eq!(SpanKind::Consumer.to_string(), "CONSUMER");
    }

    #[test]
    fn status_code() {
        assert_eq!(Status::Unset.code(), "UNSET");
        assert_eq!(Status::Ok.code(), "OK");
        assert_eq!(Status::Error("boom".into()).code(), "ERROR");
        assert_eq!(Status::Error("boom".into()).message(), Some("boom"));
        assert_eq!(Status::Ok.message(), None);
        assert_eq!(Status::Unset.message(), None);
    }

    #[test]
    fn log_record_info_constructor() {
        let rec = LogRecord::info("hello", Source::Cli);
        assert_eq!(rec.severity_number, SeverityNumber::INFO);
        assert_eq!(rec.severity_text, "INFO");
        assert_eq!(rec.body, "hello");
        assert_eq!(rec.source, Source::Cli);
        assert!(rec.trace_id.is_none());
        assert!(rec.span_id.is_none());
        assert!(rec.attributes.is_empty());
        assert_eq!(rec.resource_id, 0);
        assert!(rec.time_unix_nano > 0);
        assert_eq!(rec.time_unix_nano, rec.observed_time_unix_nano);
    }

    #[test]
    fn span_start_constructor() {
        let s = Span::start("op", SpanKind::Internal, Source::Core);
        assert_eq!(s.name, "op");
        assert_eq!(s.kind, SpanKind::Internal);
        assert_eq!(s.source, Source::Core);
        assert_eq!(s.status, Status::Unset);
        assert!(s.parent_span_id.is_none());
        assert!(s.end_time_unix_nano.is_none());
        assert!(s.attributes.is_empty());
        assert!(s.events.is_empty());
        assert_eq!(s.resource_id, 0);
        assert!(!s.trace_id.is_zero());
        assert!(!s.span_id.is_zero());
        assert!(s.start_time_unix_nano > 0);
    }

    #[test]
    fn attributes_are_deterministically_ordered() {
        let mut attrs = Attributes::new();
        attrs.insert("zeta".into(), serde_json::json!(1));
        attrs.insert("alpha".into(), serde_json::json!(2));
        attrs.insert("mu".into(), serde_json::json!(3));
        let rendered = serde_json::to_string(&attrs).unwrap();
        assert_eq!(rendered, r#"{"alpha":2,"mu":3,"zeta":1}"#);
    }

    #[test]
    fn log_record_serde_roundtrip() {
        let mut rec = LogRecord::info("body", Source::Hook);
        rec.attributes.insert("k".into(), serde_json::json!("v"));
        let json = serde_json::to_string(&rec).unwrap();
        let parsed: LogRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(rec, parsed);
    }
}
