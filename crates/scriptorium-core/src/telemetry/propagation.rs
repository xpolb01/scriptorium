#![allow(clippy::doc_markdown)]

//! W3C Trace Context propagation for scriptorium.
//!
//! This module implements the W3C Trace Context v1 `traceparent` header
//! format together with the `SCRIPTORIUM_*` env-var propagation contract
//! used for cross-process correlation of logs and spans.
//!
//! Spec: <https://www.w3.org/TR/trace-context/#traceparent-header>
//!
//! ## Env-var contract
//!
//! - [`ENV_TRACEPARENT`] (`SCRIPTORIUM_TRACEPARENT`) — W3C `traceparent`
//!   string, exactly `00-{32 hex trace_id}-{16 hex span_id}-{2 hex flags}`.
//! - [`ENV_SESSION_ID`]  (`SCRIPTORIUM_SESSION_ID`)  — opaque session id
//!   (free-form string, passed through unchanged).
//! - [`ENV_TURN_ID`]     (`SCRIPTORIUM_TURN_ID`)     — opaque turn id
//!   scoped to a session.
//!
//! ## Manual developer workflow (not for hooks)
//!
//! ```bash
//! # Seed a trace root in your current shell — correlates subsequent CLI invocations.
//! export SCRIPTORIUM_TRACEPARENT=$(scriptorium trace new-root)
//! scriptorium doctor        # inherits the same trace
//! scriptorium lint          # ditto
//! ```
//!
//! **Hooks MUST NOT call `scriptorium trace new-root`** — per T15/T16,
//! hooks rely on `scriptorium log emit` / `scriptorium span start`
//! internally generating a root when `SCRIPTORIUM_TRACEPARENT` is absent.
//! This keeps the per-hook subprocess budget at 1.
//!
//! ## Out of scope (v1)
//!
//! - `tracestate` header — intentionally not parsed or emitted.
//! - OpenTelemetry SDK — we roll our own minimal W3C parser so the core
//!   crate stays SDK-free (plan guardrail).

use std::collections::HashMap;
use std::process::Command;
use std::str::FromStr;

use crate::telemetry::envelope::{ParseIdError, SpanId, TraceId};

/// Env var carrying the W3C `traceparent` string across processes.
pub const ENV_TRACEPARENT: &str = "SCRIPTORIUM_TRACEPARENT";
/// Env var carrying an opaque session identifier.
pub const ENV_SESSION_ID: &str = "SCRIPTORIUM_SESSION_ID";
/// Env var carrying an opaque turn identifier scoped to a session.
pub const ENV_TURN_ID: &str = "SCRIPTORIUM_TURN_ID";

/// W3C `traceparent` string length, in bytes. `2 + 1 + 32 + 1 + 16 + 1 + 2 = 55`.
const TRACEPARENT_LEN: usize = 55;

/// Default `trace_flags` value applied to a freshly minted root context.
/// Bit 0 set = "sampled" per W3C § 3.2.2.6.
const TRACE_FLAGS_SAMPLED: u8 = 0x01;

/// Cross-process trace context: W3C trace identity + scriptorium-specific
/// session/turn identifiers.
///
/// A context inherited via [`TraceContext::from_env`] represents the span
/// that the parent process was executing when it spawned us. When this
/// process emits its own child spans, those children should treat
/// [`TraceContext::span_id`] as their `parent_span_id`.
///
/// See the module-level docs for the env-var contract and the W3C spec
/// reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// 128-bit W3C trace id (shared by every span in the trace).
    pub trace_id: TraceId,
    /// 64-bit W3C span id for the current span.
    pub span_id: SpanId,
    /// The parent span id inside *this process*, set by [`TraceContext::child`].
    /// `None` on contexts that come from [`TraceContext::new_root`] or
    /// [`TraceContext::from_env`] (the parent is remote, not tracked here).
    pub parent_span_id: Option<SpanId>,
    /// W3C `trace-flags` byte. Only bit 0 ("sampled") is defined in v1.
    pub trace_flags: u8,
    /// Opaque session identifier propagated via [`ENV_SESSION_ID`].
    pub session_id: Option<String>,
    /// Opaque turn identifier propagated via [`ENV_TURN_ID`].
    pub turn_id: Option<String>,
}

/// Errors returned by [`TraceContext::parse_traceparent`] and the
/// [`FromStr`] impl of [`TraceContext`].
#[derive(Debug, thiserror::Error)]
pub enum TraceparentParseError {
    /// Header length is not exactly 55 bytes.
    #[error("invalid length (expected 55, got {0})")]
    InvalidLength(usize),
    /// Version byte is anything other than `"00"`. Future versions and
    /// the reserved `"ff"` value are both rejected.
    #[error("unsupported version byte (only '00' is supported)")]
    UnsupportedVersion,
    /// One or more of the three hyphen delimiters is missing or
    /// misplaced.
    #[error("invalid format: expected 4 hyphen-separated parts")]
    InvalidFormat,
    /// `trace_id` field failed hex decoding.
    #[error("invalid trace_id: {0}")]
    InvalidTraceId(String),
    /// `span_id` field failed hex decoding.
    #[error("invalid span_id: {0}")]
    InvalidSpanId(String),
    /// `flags` field is not a 2-char hex byte.
    #[error("invalid flags: {0}")]
    InvalidFlags(String),
    /// `trace_id` is all zeros (invalid per W3C § 3.2.2.5).
    #[error("trace_id must not be all zeros")]
    ZeroTraceId,
    /// `span_id` is all zeros (invalid per W3C § 3.2.2.5).
    #[error("span_id must not be all zeros")]
    ZeroSpanId,
}

impl TraceContext {
    /// Create a fresh root context. `trace_id` and `span_id` come from
    /// the OS CSPRNG (via [`TraceId::new_random`] / [`SpanId::new_random`]),
    /// `parent_span_id` is `None`, and `trace_flags` is set to "sampled"
    /// (`0x01`).
    #[must_use]
    pub fn new_root(session_id: Option<String>, turn_id: Option<String>) -> Self {
        Self {
            trace_id: TraceId::new_random(),
            span_id: SpanId::new_random(),
            parent_span_id: None,
            trace_flags: TRACE_FLAGS_SAMPLED,
            session_id,
            turn_id,
        }
    }

    /// Create a child context that reuses the same `trace_id` and
    /// promotes the current `span_id` to `parent_span_id`. The child gets
    /// a fresh `span_id` from the OS CSPRNG; `trace_flags`, `session_id`,
    /// and `turn_id` are inherited unchanged.
    ///
    /// The `_new_span_name` argument is reserved for future OTel
    /// `Span::name` wiring; it is currently unused.
    #[must_use]
    pub fn child(&self, _new_span_name: &str) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::new_random(),
            parent_span_id: Some(self.span_id),
            trace_flags: self.trace_flags,
            session_id: self.session_id.clone(),
            turn_id: self.turn_id.clone(),
        }
    }

    /// Strict W3C `traceparent` parser.
    ///
    /// Validates (in order):
    ///
    /// 1. Exact length of 55 bytes.
    /// 2. Hyphen delimiters at byte offsets 2, 35, 52.
    /// 3. Version field equals `"00"` (rejects `"ff"` and all future versions).
    /// 4. `trace_id` decodes to 16 bytes of hex **and** is not all zeros.
    /// 5. `span_id` decodes to 8 bytes of hex **and** is not all zeros.
    /// 6. `flags` decodes to a single byte of hex (0x00-0xFF).
    ///
    /// Uppercase hex inputs are normalized to lowercase before parsing.
    ///
    /// # Errors
    ///
    /// Returns one of the [`TraceparentParseError`] variants documented
    /// there; the first failing check wins.
    pub fn parse_traceparent(raw: &str) -> Result<(TraceId, SpanId, u8), TraceparentParseError> {
        if raw.len() != TRACEPARENT_LEN {
            return Err(TraceparentParseError::InvalidLength(raw.len()));
        }
        // Normalize hex case before any further parsing. ASCII-only op so
        // non-ASCII inputs (which can't be valid traceparent anyway) are
        // left untouched and fail the delimiter/hex checks below.
        let lower = raw.to_ascii_lowercase();
        let bytes = lower.as_bytes();
        if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
            return Err(TraceparentParseError::InvalidFormat);
        }
        let version = &lower[0..2];
        if version != "00" {
            return Err(TraceparentParseError::UnsupportedVersion);
        }
        let trace_hex = &lower[3..35];
        let span_hex = &lower[36..52];
        let flags_hex = &lower[53..55];

        let trace_id = TraceId::from_str(trace_hex)
            .map_err(|e: ParseIdError| TraceparentParseError::InvalidTraceId(e.to_string()))?;
        if trace_id.is_zero() {
            return Err(TraceparentParseError::ZeroTraceId);
        }
        let span_id = SpanId::from_str(span_hex)
            .map_err(|e: ParseIdError| TraceparentParseError::InvalidSpanId(e.to_string()))?;
        if span_id.is_zero() {
            return Err(TraceparentParseError::ZeroSpanId);
        }
        let flags = u8::from_str_radix(flags_hex, 16)
            .map_err(|e| TraceparentParseError::InvalidFlags(e.to_string()))?;
        Ok((trace_id, span_id, flags))
    }

    /// Emit a W3C `traceparent` header string for this context.
    ///
    /// The output always starts with `"00-"` (v1) and always uses
    /// lowercase hex. It is the identity inverse of
    /// [`TraceContext::parse_traceparent`] on the `(trace_id, span_id, flags)`
    /// tuple.
    #[must_use]
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Load a context from process environment.
    ///
    /// Returns `None` if [`ENV_TRACEPARENT`] is absent or malformed — the
    /// caller should then fall back to [`TraceContext::new_root`].
    /// [`ENV_SESSION_ID`] / [`ENV_TURN_ID`] are optional and pass through
    /// unchanged.
    ///
    /// `parent_span_id` on the returned context is always `None`: the
    /// "parent" on the remote side IS our `span_id` for this process,
    /// and becomes `parent_span_id` only when we mint a child via
    /// [`TraceContext::child`].
    #[must_use]
    pub fn from_env() -> Option<TraceContext> {
        let raw = std::env::var(ENV_TRACEPARENT).ok()?;
        let (trace_id, span_id, trace_flags) = Self::parse_traceparent(&raw).ok()?;
        let session_id = std::env::var(ENV_SESSION_ID).ok();
        let turn_id = std::env::var(ENV_TURN_ID).ok();
        Some(Self {
            trace_id,
            span_id,
            parent_span_id: None,
            trace_flags,
            session_id,
            turn_id,
        })
    }

    /// Export this context to the given [`Command`]'s environment.
    ///
    /// [`ENV_TRACEPARENT`] is always set. [`ENV_SESSION_ID`] /
    /// [`ENV_TURN_ID`] are set when [`Self::session_id`] / [`Self::turn_id`]
    /// are `Some`; otherwise they are **explicitly removed** from the
    /// child's environment so stale values from the parent process cannot
    /// leak through.
    pub fn export_env(&self, cmd: &mut Command) {
        cmd.env(ENV_TRACEPARENT, self.to_traceparent());
        if let Some(v) = &self.session_id {
            cmd.env(ENV_SESSION_ID, v);
        } else {
            cmd.env_remove(ENV_SESSION_ID);
        }
        if let Some(v) = &self.turn_id {
            cmd.env(ENV_TURN_ID, v);
        } else {
            cmd.env_remove(ENV_TURN_ID);
        }
    }

    /// Export to a `HashMap` for programmatic use (tests, bash env
    /// emission, MCP session forwarding). Keys are omitted when their
    /// corresponding field is `None`.
    #[must_use]
    pub fn as_env_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(ENV_TRACEPARENT.to_string(), self.to_traceparent());
        if let Some(v) = &self.session_id {
            map.insert(ENV_SESSION_ID.to_string(), v.clone());
        }
        if let Some(v) = &self.turn_id {
            map.insert(ENV_TURN_ID.to_string(), v.clone());
        }
        map
    }
}

impl FromStr for TraceContext {
    type Err = TraceparentParseError;

    /// Parse a `TraceContext` from a `traceparent` header string alone.
    /// `session_id` / `turn_id` default to `None`; `parent_span_id` is
    /// always `None` (see [`TraceContext::from_env`] for rationale).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (trace_id, span_id, trace_flags) = Self::parse_traceparent(s)?;
        Ok(Self {
            trace_id,
            span_id,
            parent_span_id: None,
            trace_flags,
            session_id: None,
            turn_id: None,
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes env-var tests so they don't race each other (process
    /// env is a global mutable singleton).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Canonical example from W3C § 3.2.2.
    const VALID: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    // ── parse_traceparent ─────────────────────────────────────────────

    #[test]
    fn parse_valid() {
        let (tid, sid, flags) = TraceContext::parse_traceparent(VALID).expect("parse valid");
        assert_eq!(tid.to_string(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(sid.to_string(), "b7ad6b7169203331");
        assert_eq!(flags, 0x01);
    }

    #[test]
    fn parse_valid_lowercase_normalization() {
        let upper = "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01";
        let (tid, sid, flags) = TraceContext::parse_traceparent(upper).expect("parse upper");
        assert_eq!(tid.to_string(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(sid.to_string(), "b7ad6b7169203331");
        assert_eq!(flags, 0x01);
    }

    #[test]
    fn parse_invalid_length() {
        let short = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0";
        assert_eq!(short.len(), 54);
        assert!(matches!(
            TraceContext::parse_traceparent(short),
            Err(TraceparentParseError::InvalidLength(54))
        ));

        let long = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-011";
        assert_eq!(long.len(), 56);
        assert!(matches!(
            TraceContext::parse_traceparent(long),
            Err(TraceparentParseError::InvalidLength(56))
        ));
    }

    #[test]
    fn parse_invalid_version() {
        let bad = "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert_eq!(bad.len(), 55);
        assert!(matches!(
            TraceContext::parse_traceparent(bad),
            Err(TraceparentParseError::UnsupportedVersion)
        ));

        // Any other version byte is also rejected in v1.
        let bad2 = "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert!(matches!(
            TraceContext::parse_traceparent(bad2),
            Err(TraceparentParseError::UnsupportedVersion)
        ));
    }

    #[test]
    fn parse_invalid_format() {
        // Delimiter at pos 2 replaced with 'x'.
        let bad = "00x0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert_eq!(bad.len(), 55);
        assert!(matches!(
            TraceContext::parse_traceparent(bad),
            Err(TraceparentParseError::InvalidFormat)
        ));

        // Delimiter at pos 35 missing.
        let bad2 = "00-0af7651916cd43dd8448eb211c80319cxb7ad6b7169203331-01";
        assert_eq!(bad2.len(), 55);
        assert!(matches!(
            TraceContext::parse_traceparent(bad2),
            Err(TraceparentParseError::InvalidFormat)
        ));

        // Delimiter at pos 52 missing.
        let bad3 = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331x01";
        assert_eq!(bad3.len(), 55);
        assert!(matches!(
            TraceContext::parse_traceparent(bad3),
            Err(TraceparentParseError::InvalidFormat)
        ));
    }

    #[test]
    fn parse_invalid_trace_id_zeros() {
        let bad = "00-00000000000000000000000000000000-b7ad6b7169203331-01";
        assert!(matches!(
            TraceContext::parse_traceparent(bad),
            Err(TraceparentParseError::ZeroTraceId)
        ));
    }

    #[test]
    fn parse_invalid_span_id_zeros() {
        let bad = "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01";
        assert!(matches!(
            TraceContext::parse_traceparent(bad),
            Err(TraceparentParseError::ZeroSpanId)
        ));
    }

    #[test]
    fn parse_invalid_hex() {
        let bad = "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-b7ad6b7169203331-01";
        assert!(matches!(
            TraceContext::parse_traceparent(bad),
            Err(TraceparentParseError::InvalidTraceId(_))
        ));

        let bad_span = "00-0af7651916cd43dd8448eb211c80319c-zzzzzzzzzzzzzzzz-01";
        assert!(matches!(
            TraceContext::parse_traceparent(bad_span),
            Err(TraceparentParseError::InvalidSpanId(_))
        ));

        let bad_flags = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-zz";
        assert!(matches!(
            TraceContext::parse_traceparent(bad_flags),
            Err(TraceparentParseError::InvalidFlags(_))
        ));
    }

    #[test]
    fn roundtrip_to_traceparent() {
        let ctx: TraceContext = VALID.parse().expect("parse valid");
        assert_eq!(ctx.to_traceparent(), VALID);

        // Uppercase input normalizes on roundtrip.
        let upper = "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01";
        let ctx2: TraceContext = upper.parse().expect("parse upper");
        assert_eq!(ctx2.to_traceparent(), VALID);
    }

    // ── new_root / child ───────────────────────────────────────────────

    #[test]
    fn new_root_randomness() {
        let a = TraceContext::new_root(None, None);
        let b = TraceContext::new_root(None, None);
        assert_ne!(a.trace_id, b.trace_id, "two new_root calls must differ");
        assert_ne!(a.span_id, b.span_id);
        assert!(a.parent_span_id.is_none());
        assert_eq!(a.trace_flags, TRACE_FLAGS_SAMPLED);
        assert!(a.session_id.is_none());
        assert!(a.turn_id.is_none());
    }

    #[test]
    fn new_root_carries_session_and_turn() {
        let ctx = TraceContext::new_root(Some("sess".into()), Some("turn".into()));
        assert_eq!(ctx.session_id.as_deref(), Some("sess"));
        assert_eq!(ctx.turn_id.as_deref(), Some("turn"));
    }

    #[test]
    fn child_reuses_trace() {
        let root = TraceContext::new_root(Some("sess".into()), Some("turn".into()));
        let child = root.child("op");
        assert_eq!(child.trace_id, root.trace_id, "trace_id must be inherited");
        assert_eq!(
            child.parent_span_id,
            Some(root.span_id),
            "parent_span_id must point at root.span_id"
        );
        assert_ne!(child.span_id, root.span_id, "child span_id must be fresh");
        assert_eq!(child.trace_flags, root.trace_flags);
        assert_eq!(child.session_id, root.session_id);
        assert_eq!(child.turn_id, root.turn_id);
    }

    // ── env-var roundtrip ──────────────────────────────────────────────

    fn save_and_clear_env() -> (Option<String>, Option<String>, Option<String>) {
        let a = std::env::var(ENV_TRACEPARENT).ok();
        let b = std::env::var(ENV_SESSION_ID).ok();
        let c = std::env::var(ENV_TURN_ID).ok();
        std::env::remove_var(ENV_TRACEPARENT);
        std::env::remove_var(ENV_SESSION_ID);
        std::env::remove_var(ENV_TURN_ID);
        (a, b, c)
    }

    fn restore_env(prev: (Option<String>, Option<String>, Option<String>)) {
        for (k, v) in [
            (ENV_TRACEPARENT, prev.0),
            (ENV_SESSION_ID, prev.1),
            (ENV_TURN_ID, prev.2),
        ] {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn env_roundtrip() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = save_and_clear_env();

        std::env::set_var(ENV_TRACEPARENT, VALID);
        std::env::set_var(ENV_SESSION_ID, "sess-1");
        std::env::set_var(ENV_TURN_ID, "turn-7");

        let ctx = TraceContext::from_env().expect("from_env with valid traceparent");
        assert_eq!(ctx.to_traceparent(), VALID);
        assert_eq!(ctx.session_id.as_deref(), Some("sess-1"));
        assert_eq!(ctx.turn_id.as_deref(), Some("turn-7"));
        assert!(
            ctx.parent_span_id.is_none(),
            "from_env must not set parent_span_id"
        );

        // Clearing traceparent makes from_env return None even if
        // session/turn are still set.
        std::env::remove_var(ENV_TRACEPARENT);
        assert!(TraceContext::from_env().is_none());

        restore_env(prev);
    }

    #[test]
    fn from_env_invalid_returns_none() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = save_and_clear_env();

        std::env::set_var(ENV_TRACEPARENT, "not-a-valid-traceparent");
        assert!(
            TraceContext::from_env().is_none(),
            "invalid traceparent must yield None (graceful fallback)"
        );

        // Empty string → None.
        std::env::set_var(ENV_TRACEPARENT, "");
        assert!(TraceContext::from_env().is_none());

        restore_env(prev);
    }

    // ── export_env ─────────────────────────────────────────────────────

    #[test]
    fn export_env_sets_all_three() {
        let ctx = TraceContext {
            trace_id: TraceId::from_str("0af7651916cd43dd8448eb211c80319c").unwrap(),
            span_id: SpanId::from_str("b7ad6b7169203331").unwrap(),
            parent_span_id: None,
            trace_flags: 0x01,
            session_id: Some("sess".into()),
            turn_id: Some("turn".into()),
        };
        let mut cmd = Command::new("/bin/true");
        ctx.export_env(&mut cmd);

        let envs: HashMap<std::ffi::OsString, std::ffi::OsString> = cmd
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_os_string(), v.to_os_string())))
            .collect();

        assert_eq!(
            envs.get(std::ffi::OsStr::new(ENV_TRACEPARENT))
                .map(std::ffi::OsString::as_os_str),
            Some(std::ffi::OsStr::new(VALID))
        );
        assert_eq!(
            envs.get(std::ffi::OsStr::new(ENV_SESSION_ID))
                .map(std::ffi::OsString::as_os_str),
            Some(std::ffi::OsStr::new("sess"))
        );
        assert_eq!(
            envs.get(std::ffi::OsStr::new(ENV_TURN_ID))
                .map(std::ffi::OsString::as_os_str),
            Some(std::ffi::OsStr::new("turn"))
        );
    }

    #[test]
    fn export_env_removes_missing_session_and_turn() {
        // When session_id / turn_id are None, the child's env must have
        // those names explicitly removed so stale parent values don't
        // leak through.
        let ctx = TraceContext::new_root(None, None);
        let mut cmd = Command::new("/bin/true");
        ctx.export_env(&mut cmd);

        let mut saw_session_removed = false;
        let mut saw_turn_removed = false;
        for (k, v) in cmd.get_envs() {
            if k == std::ffi::OsStr::new(ENV_SESSION_ID) && v.is_none() {
                saw_session_removed = true;
            }
            if k == std::ffi::OsStr::new(ENV_TURN_ID) && v.is_none() {
                saw_turn_removed = true;
            }
        }
        assert!(
            saw_session_removed,
            "session_id=None must translate to env_remove on the child"
        );
        assert!(
            saw_turn_removed,
            "turn_id=None must translate to env_remove on the child"
        );
    }

    #[test]
    fn as_env_map_omits_none_fields() {
        let ctx = TraceContext::new_root(None, None);
        let map = ctx.as_env_map();
        assert!(map.contains_key(ENV_TRACEPARENT));
        assert!(
            !map.contains_key(ENV_SESSION_ID),
            "None session_id must be omitted from as_env_map"
        );
        assert!(!map.contains_key(ENV_TURN_ID));

        let ctx2 = TraceContext::new_root(Some("s".into()), Some("t".into()));
        let map2 = ctx2.as_env_map();
        assert_eq!(map2.get(ENV_SESSION_ID).map(String::as_str), Some("s"));
        assert_eq!(map2.get(ENV_TURN_ID).map(String::as_str), Some("t"));
    }
}
