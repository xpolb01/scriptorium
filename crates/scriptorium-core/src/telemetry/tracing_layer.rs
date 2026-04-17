#![allow(clippy::doc_markdown)]

//! `tracing::Layer` → SQLite bridge for OTel-shaped telemetry.
//!
//! [`TelemetrySqliteLayer`] implements `tracing_subscriber::Layer` so
//! every `tracing::info!`, `tracing::span!`, etc. from anywhere in the
//! process is captured into the OTel-shaped [`TelemetryStore`] as either
//! a [`LogRecord`] (events) or a [`Span`] (spans).
//!
//! **Mapping contract**:
//!
//! - `tracing::Level` → [`SeverityNumber`] via canonical OTel tiers
//!   (`TRACE`=1, `DEBUG`=5, `INFO`=9, `WARN`=13, `ERROR`=17).
//! - The `message` field → [`LogRecord::body`].
//! - `otel.kind` field (values `"server"`/`"client"`/...) → [`SpanKind`]
//!   override — matches the convention used by `tracing-opentelemetry`.
//! - `otel.status` field (values `"ok"`/`"error"`) → [`Status`] override.
//! - `error = <msg>` field implies [`Status::Error`] when no explicit
//!   `otel.status` was set.
//! - Every other field is collected into [`Attributes`].
//! - `tracing::field::Empty` placeholders never register a recorded
//!   value and are therefore skipped entirely (not stored as the literal
//!   string `"<empty>"`).
//!
//! **Trace identity**:
//!
//! - If [`TraceContext::from_env`] returns `Some` at layer init, every
//!   root span (one with no in-process tracing parent) inherits that
//!   trace_id and uses the env-provided span_id as its `parent_span_id`.
//! - Otherwise, a fresh [`TraceId::new_random`] is minted per root span
//!   and `parent_span_id` is `None`.
//! - Nested spans share the root's trace_id and point at their
//!   in-process parent's span_id.
//!
//! **Payload caps** ([`cap_body`] / [`cap_attributes`]) are applied
//! before every store write; truncation metadata is attached as
//! `telemetry.truncated` / `telemetry.truncated_fields` attributes.
//!
//! **Panic safety**: every Layer method is panic-free — all `Result` /
//! `Option` unwrap paths are guarded, and every [`TelemetryStore`] call
//! is `let _ =` ignored (writes are best-effort by design).
//!
//! [`TelemetryStore`]: crate::telemetry::store::TelemetryStore
//! [`LogRecord`]: crate::telemetry::envelope::LogRecord
//! [`Span`]: crate::telemetry::envelope::Span
//! [`SeverityNumber`]: crate::telemetry::envelope::SeverityNumber
//! [`SpanKind`]: crate::telemetry::envelope::SpanKind
//! [`Status`]: crate::telemetry::envelope::Status
//! [`Attributes`]: crate::telemetry::envelope::Attributes
//! [`TraceContext::from_env`]: crate::telemetry::propagation::TraceContext::from_env
//! [`TraceId::new_random`]: crate::telemetry::envelope::TraceId::new_random
//! [`cap_body`]: crate::telemetry::payload::cap_body
//! [`cap_attributes`]: crate::telemetry::payload::cap_attributes

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes as SpanAttrs, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::telemetry::envelope::{
    Attributes, LogRecord, PayloadCap, SeverityNumber, Source, Span as TelemetrySpan, SpanId,
    SpanKind, Status, TraceId, DEFAULT_PAYLOAD_CAP,
};
use crate::telemetry::payload::{add_truncation_attrs, cap_attributes, cap_body};
use crate::telemetry::propagation::TraceContext;
use crate::telemetry::resource::Resource;
use crate::telemetry::store::TelemetryStore;

/// Return the current wall-clock time in nanoseconds since the Unix
/// epoch. Panic-free: negative durations fall back to `0`, and
/// out-of-range `i64` values clamp to [`i64::MAX`].
fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// Per-span state stored in span extensions. Captured by
/// `on_new_span`, mutated by `on_record` / `on_event`, consumed by
/// `on_close`.
#[derive(Debug, Clone)]
struct SpanState {
    span_id: SpanId,
    trace_id: TraceId,
    attributes: Attributes,
    otel_status_override: Option<Status>,
}

/// Scans the fields of a span or event once, producing:
/// - `message`: value of the special `message` field (becomes
///   [`LogRecord::body`] for events).
/// - `attributes`: every other field as a JSON value.
/// - `otel_kind_override`: set when a string field `otel.kind` parses to
///   a [`SpanKind`].
/// - `otel_status_override`: set when a string field `otel.status` is
///   `"ok"` / `"error"`, OR (fallback) when an `error` field carries a
///   message.
#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    attributes: Attributes,
    otel_kind_override: Option<SpanKind>,
    otel_status_override: Option<Status>,
    saw_error_field: Option<String>,
}

impl FieldVisitor {
    fn into_parts(mut self) -> (Option<String>, Attributes, Option<SpanKind>, Option<Status>) {
        // The `error` field promotes to Status::Error(msg) only if no
        // explicit otel.status override was set.
        if self.otel_status_override.is_none() {
            if let Some(msg) = self.saw_error_field.take() {
                self.otel_status_override = Some(Status::Error(msg));
            }
        }
        (
            self.message,
            self.attributes,
            self.otel_kind_override,
            self.otel_status_override,
        )
    }

    fn store_string_field(&mut self, name: &str, value: &str) {
        match name {
            "message" => {
                self.message = Some(value.to_string());
            }
            "otel.kind" => {
                self.otel_kind_override = parse_span_kind(value);
            }
            "otel.status" => {
                self.otel_status_override = match value {
                    "ok" => Some(Status::Ok),
                    "error" => Some(Status::Error(String::new())),
                    _ => None,
                };
            }
            "error" => {
                self.saw_error_field = Some(value.to_string());
                self.attributes.insert(
                    name.to_string(),
                    serde_json::Value::String(value.to_string()),
                );
            }
            _ => {
                self.attributes.insert(
                    name.to_string(),
                    serde_json::Value::String(value.to_string()),
                );
            }
        }
    }
}

/// Parse an OTel-style span kind string. Case-insensitive; unknown
/// values yield `None`.
fn parse_span_kind(s: &str) -> Option<SpanKind> {
    match s.to_ascii_lowercase().as_str() {
        "internal" => Some(SpanKind::Internal),
        "server" => Some(SpanKind::Server),
        "client" => Some(SpanKind::Client),
        "producer" => Some(SpanKind::Producer),
        "consumer" => Some(SpanKind::Consumer),
        _ => None,
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.store_string_field(field.name(), value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
            return;
        }
        self.attributes.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
            return;
        }
        self.attributes.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
            return;
        }
        let num =
            serde_json::Number::from_f64(value).unwrap_or_else(|| serde_json::Number::from(0));
        self.attributes
            .insert(field.name().to_string(), serde_json::Value::Number(num));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
            return;
        }
        self.attributes
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{value:?}");
        self.store_string_field(field.name(), &formatted);
    }
}

/// `tracing::Layer` that writes OTel-shaped rows into a
/// [`TelemetryStore`].
///
/// Cheap to clone via [`Arc`]-wrapped store. Holds no locks across event
/// emission paths — each store write opens its own short transaction.
#[derive(Debug)]
pub struct TelemetrySqliteLayer {
    store: Arc<TelemetryStore>,
    resource_id: i64,
    default_source: Source,
    payload_cap: PayloadCap,
    root_context: Option<TraceContext>,
}

impl TelemetrySqliteLayer {
    /// Construct a layer wired to `store`. Detects and upserts the
    /// process [`Resource`], caches its `resource_id`, and captures any
    /// inherited [`TraceContext`] from env at init time.
    ///
    /// Store writes during init are best-effort; if resource lookup
    /// fails, `resource_id` falls back to `0` (downstream writes will
    /// then fail the FK check and be dropped via the
    /// [`InsertOutcome::Dropped`] path — this is preferable to crashing
    /// on init).
    ///
    /// [`InsertOutcome::Dropped`]: crate::telemetry::store::InsertOutcome::Dropped
    #[must_use]
    pub fn init(
        store: Arc<TelemetryStore>,
        default_source: Source,
        vault_path: Option<&Path>,
    ) -> Self {
        let resource = Resource::detect(default_source, vault_path);
        let _ = store.insert_resource(&resource);
        let resource_id = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap_or(0);
        let root_context = TraceContext::from_env();
        Self {
            store,
            resource_id,
            default_source,
            payload_cap: DEFAULT_PAYLOAD_CAP,
            root_context,
        }
    }

    /// Override the default [`PayloadCap`]. Builder-style.
    #[must_use]
    pub fn with_payload_cap(mut self, cap: PayloadCap) -> Self {
        self.payload_cap = cap;
        self
    }
}

impl<S> Layer<S> for TelemetrySqliteLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &SpanAttrs<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();

        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let (_message, attributes, otel_kind_override, otel_status_override) = visitor.into_parts();

        // Look up in-process parent (if any) and inherit its trace_id.
        let parent_info: Option<(TraceId, SpanId)> = ctx.span(id).and_then(|span_ref| {
            span_ref.parent().and_then(|parent_ref| {
                parent_ref
                    .extensions()
                    .get::<SpanState>()
                    .map(|p| (p.trace_id, p.span_id))
            })
        });

        let (trace_id, parent_span_id) = match parent_info {
            Some((tid, pid)) => (tid, Some(pid)),
            None => match &self.root_context {
                Some(c) => (c.trace_id, Some(c.span_id)),
                None => (TraceId::new_random(), None),
            },
        };

        let span_id = SpanId::new_random();
        let kind = otel_kind_override.unwrap_or(SpanKind::Internal);

        let span = TelemetrySpan {
            span_id,
            trace_id,
            parent_span_id,
            name: metadata.name().to_string(),
            kind,
            start_time_unix_nano: now_ns(),
            end_time_unix_nano: None,
            status: Status::Unset,
            attributes: attributes.clone(),
            events: Vec::new(),
            source: self.default_source,
            resource_id: self.resource_id,
        };

        let _ = self.store.insert_span_start(&span);

        if let Some(span_ref) = ctx.span(id) {
            span_ref.extensions_mut().insert(SpanState {
                span_id,
                trace_id,
                attributes,
                otel_status_override,
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span_ref) = ctx.span(id) else {
            return;
        };
        let mut extensions = span_ref.extensions_mut();
        let Some(state) = extensions.get_mut::<SpanState>() else {
            return;
        };

        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        let (_message, attributes, _kind, status_override) = visitor.into_parts();

        for (k, v) in attributes {
            state.attributes.insert(k, v);
        }
        if status_override.is_some() {
            state.otel_status_override = status_override;
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let severity = SeverityNumber::from_tracing_level(metadata.level());

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let (message, attributes, _kind, status_override) = visitor.into_parts();

        let body_raw = message.unwrap_or_default();
        let (body, body_meta) = cap_body(&body_raw, &self.payload_cap);
        let (mut attributes_capped, mut attr_metas) = cap_attributes(attributes, &self.payload_cap);
        let mut metas = Vec::new();
        if let Some(m) = body_meta {
            metas.push(m);
        }
        metas.append(&mut attr_metas);
        add_truncation_attrs(&mut attributes_capped, &metas);

        // Resolve trace/span ids: prefer the span this event is attached
        // to; fall back to the env-provided root context; else None.
        let mut trace_id_opt: Option<TraceId> = None;
        let mut span_id_opt: Option<SpanId> = None;

        if let Some(span_ref) = ctx.event_span(event) {
            let exts = span_ref.extensions();
            if let Some(st) = exts.get::<SpanState>() {
                trace_id_opt = Some(st.trace_id);
                span_id_opt = Some(st.span_id);
            }
        }

        if trace_id_opt.is_none() {
            if let Some(c) = &self.root_context {
                trace_id_opt = Some(c.trace_id);
                span_id_opt = Some(c.span_id);
            }
        }

        // If the event carries otel.status / error, propagate it to the
        // enclosing span's state so on_close can apply it.
        if let Some(status) = status_override {
            if let Some(span_ref) = ctx.event_span(event) {
                let mut exts = span_ref.extensions_mut();
                if let Some(st) = exts.get_mut::<SpanState>() {
                    st.otel_status_override = Some(status);
                }
            }
        }

        let now = now_ns();
        let log = LogRecord {
            time_unix_nano: now,
            observed_time_unix_nano: now,
            severity_number: severity,
            severity_text: severity.to_text(),
            body,
            trace_id: trace_id_opt,
            span_id: span_id_opt,
            attributes: attributes_capped,
            source: self.default_source,
            resource_id: self.resource_id,
        };
        let _ = self.store.insert_log(&log);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span_ref) = ctx.span(&id) else {
            return;
        };

        // Pull everything we need out of the extensions guard, then drop
        // the guard before calling into the store (which may do I/O).
        let (span_id, status, attributes) = {
            let exts = span_ref.extensions();
            let Some(state) = exts.get::<SpanState>() else {
                return;
            };
            (
                state.span_id,
                state.otel_status_override.clone().unwrap_or(Status::Ok),
                state.attributes.clone(),
            )
        };

        let _ = self
            .store
            .update_span_end(span_id, now_ns(), status, attributes);
    }
}

/// Install the telemetry layer plus a stderr `fmt` layer as the
/// **thread-local default** subscriber. Returns a
/// [`tracing::dispatcher::DefaultGuard`]; drop it to restore the
/// previous default.
///
/// Note: this uses `tracing::subscriber::set_default` (scoped) rather
/// than `set_global_default` so multiple installs in a test binary or
/// repeated `main` calls don't panic. Callers who need a process-wide
/// install should compose the layer stack themselves and call
/// `set_global_default`.
pub fn install_global(
    store: Arc<TelemetryStore>,
    default_source: Source,
    vault_path: Option<&Path>,
) -> tracing::dispatcher::DefaultGuard {
    use tracing_subscriber::layer::SubscriberExt;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false);
    let tele_layer = TelemetrySqliteLayer::init(store, default_source, vault_path);
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(tele_layer);
    tracing::subscriber::set_default(subscriber)
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::envelope::ENV_MAX_BODY;
    use crate::telemetry::propagation::ENV_TRACEPARENT;
    use crate::telemetry::store::{LogFilters, SpanFilters};
    use std::sync::Mutex;
    use tracing::{debug, error, info, info_span, trace, warn};
    use tracing_subscriber::layer::SubscriberExt;

    /// Serializes env-var-touching tests so they don't race each other.
    /// Layer tests also acquire this lock because `init` reads env.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn make_store() -> (tempfile::TempDir, Arc<TelemetryStore>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("t.sqlite");
        let store = Arc::new(TelemetryStore::open(&path).expect("open store"));
        (dir, store)
    }

    /// Clear `SCRIPTORIUM_TRACEPARENT` for tests that assume no root
    /// context. Returns the previous value for restoration.
    fn clear_env_traceparent() -> Option<String> {
        let prev = std::env::var(ENV_TRACEPARENT).ok();
        std::env::remove_var(ENV_TRACEPARENT);
        prev
    }

    fn restore_env_traceparent(prev: Option<String>) {
        match prev {
            Some(v) => std::env::set_var(ENV_TRACEPARENT, v),
            None => std::env::remove_var(ENV_TRACEPARENT),
        }
    }

    #[test]
    fn single_event_captured_as_log() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info!("hello");
            });
        }

        let (rows, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let found: Vec<_> = rows.iter().filter(|r| r.body == "hello").collect();
        assert_eq!(found.len(), 1, "expected 1 row with body=hello");
        let row = found[0];
        assert_eq!(row.severity_number, 9);
        assert_eq!(row.severity_text.as_deref(), Some("INFO"));
        assert_eq!(row.source, "cli");
        assert!(row.trace_id.is_none());
        assert!(row.span_id.is_none());

        restore_env_traceparent(prev);
    }

    #[test]
    fn level_severity_mapping() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                trace!("t-msg");
                debug!("d-msg");
                info!("i-msg");
                warn!("w-msg");
                error!("e-msg");
            });
        }

        let (rows, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let severity_for = |body: &str| -> Option<u8> {
            rows.iter()
                .find(|r| r.body == body)
                .map(|r| r.severity_number)
        };
        assert_eq!(severity_for("t-msg"), Some(1), "TRACE → 1");
        assert_eq!(severity_for("d-msg"), Some(5), "DEBUG → 5");
        assert_eq!(severity_for("i-msg"), Some(9), "INFO → 9");
        assert_eq!(severity_for("w-msg"), Some(13), "WARN → 13");
        assert_eq!(severity_for("e-msg"), Some(17), "ERROR → 17");

        restore_env_traceparent(prev);
    }

    #[test]
    fn nested_spans_parent_child() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                let parent = info_span!("parent");
                parent.in_scope(|| {
                    let child = info_span!("child");
                    child.in_scope(|| {
                        info!("inner");
                    });
                });
            });
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let parent_row = spans
            .iter()
            .find(|s| s.name == "parent")
            .expect("parent span written");
        let child_row = spans
            .iter()
            .find(|s| s.name == "child")
            .expect("child span written");

        assert_eq!(
            child_row.parent_span_id.as_deref(),
            Some(parent_row.span_id.as_str()),
            "child.parent_span_id must equal parent.span_id"
        );
        assert_eq!(
            child_row.trace_id, parent_row.trace_id,
            "child and parent must share trace_id"
        );

        let (logs, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let inner = logs
            .iter()
            .find(|r| r.body == "inner")
            .expect("inner log written");
        assert_eq!(
            inner.span_id.as_deref(),
            Some(child_row.span_id.as_str()),
            "log emitted inside child span must carry child.span_id"
        );
        assert_eq!(
            inner.trace_id.as_deref(),
            Some(child_row.trace_id.as_str()),
            "log emitted inside child span must carry shared trace_id"
        );

        restore_env_traceparent(prev);
    }

    #[test]
    fn env_trace_parent_inherited() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::var(ENV_TRACEPARENT).ok();

        let valid = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        std::env::set_var(ENV_TRACEPARENT, valid);

        let (_dir, store) = make_store();
        {
            // Layer init reads env HERE — must happen after set_var.
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info_span!("root").in_scope(|| {
                    info!("ev");
                });
            });
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let root_row = spans
            .iter()
            .find(|s| s.name == "root")
            .expect("root span written");
        assert_eq!(
            root_row.trace_id, "0af7651916cd43dd8448eb211c80319c",
            "top-level span must inherit env trace_id"
        );
        assert_eq!(
            root_row.parent_span_id.as_deref(),
            Some("b7ad6b7169203331"),
            "top-level span's parent must be env span_id"
        );

        restore_env_traceparent(prev);
    }

    #[test]
    fn otel_kind_override_sets_span_kind() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                let _s = info_span!("op", otel.kind = "server").entered();
            });
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let op = spans.iter().find(|s| s.name == "op").expect("span written");
        assert_eq!(op.kind, "SERVER", "otel.kind=server → SpanKind::Server");

        restore_env_traceparent(prev);
    }

    #[test]
    fn otel_status_error_from_event_field() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info_span!("op").in_scope(|| {
                    error!(error = "boom", "failure");
                });
            });
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let op = spans.iter().find(|s| s.name == "op").expect("span written");
        assert_eq!(op.status_code, "ERROR");
        assert_eq!(op.status_message.as_deref(), Some("boom"));

        restore_env_traceparent(prev);
    }

    #[test]
    fn otel_status_ok_default_when_no_error() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info_span!("clean").in_scope(|| {
                    info!("no-error");
                });
            });
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let op = spans
            .iter()
            .find(|s| s.name == "clean")
            .expect("span written");
        assert_eq!(
            op.status_code, "OK",
            "span closed without error → Status::Ok"
        );
        assert!(op.status_message.is_none());

        restore_env_traceparent(prev);
    }

    #[test]
    fn payload_cap_applies_to_body() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();
        // Default cap is 8 KiB body.
        std::env::remove_var(ENV_MAX_BODY);

        let (_dir, store) = make_store();
        let big_body: String = "x".repeat(16 * 1024);
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info!("{}", big_body);
            });
        }

        let (rows, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let truncated = rows
            .iter()
            .find(|r| r.body.starts_with("xxxx"))
            .expect("log inserted");
        assert!(
            truncated.body.len() <= 8192,
            "body must be capped to ≤ 8 KiB; got {} bytes",
            truncated.body.len()
        );
        assert_eq!(
            truncated
                .attributes
                .get("telemetry.truncated")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            serde_json::Value::Bool(true),
            "truncation marker attribute must be set"
        );

        restore_env_traceparent(prev);
    }

    #[test]
    fn fields_captured_as_attributes() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info!(user = "alice", count = 42i64, active = true, "msg");
            });
        }

        let (rows, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let row = rows.iter().find(|r| r.body == "msg").expect("log inserted");
        assert_eq!(
            row.attributes.get("user"),
            Some(&serde_json::Value::String("alice".into()))
        );
        assert_eq!(
            row.attributes.get("count"),
            Some(&serde_json::json!(42)),
            "integer field should be captured as JSON number"
        );
        assert_eq!(
            row.attributes.get("active"),
            Some(&serde_json::Value::Bool(true))
        );

        restore_env_traceparent(prev);
    }

    #[test]
    fn empty_field_skipped() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::with_default(subscriber, || {
                info!(skipme = tracing::field::Empty, "msg-empty");
            });
        }

        let (rows, _) = store
            .query_logs(LogFilters::default(), None, 100)
            .expect("query logs");
        let row = rows
            .iter()
            .find(|r| r.body == "msg-empty")
            .expect("log inserted");
        assert!(
            row.attributes.get("skipme").is_none(),
            "tracing::field::Empty must not be recorded as an attribute; got {:?}",
            row.attributes.get("skipme")
        );

        restore_env_traceparent(prev);
    }

    #[test]
    fn dangling_span_end_time_null_then_set_on_close() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = clear_env_traceparent();

        let (_dir, store) = make_store();
        let stored_span_id: String;
        {
            let layer = TelemetrySqliteLayer::init(Arc::clone(&store), Source::Cli, None);
            let subscriber = tracing_subscriber::registry().with(layer);
            let _sub_guard = tracing::subscriber::set_default(subscriber);

            let span = info_span!("dangling");
            // on_new_span has fired; on_close has NOT.
            let (spans, _) = store
                .query_spans(SpanFilters::default(), None, 100)
                .expect("query spans");
            let row = spans
                .iter()
                .find(|s| s.name == "dangling")
                .expect("span inserted");
            assert!(
                row.end_time_unix_nano.is_none(),
                "end_time should be NULL before on_close"
            );
            stored_span_id = row.span_id.clone();

            drop(span);
            // on_close fires now.
        }

        let (spans, _) = store
            .query_spans(SpanFilters::default(), None, 100)
            .expect("query spans");
        let row = spans
            .iter()
            .find(|s| s.span_id == stored_span_id)
            .expect("span still there");
        assert!(
            row.end_time_unix_nano.is_some(),
            "end_time should be set after on_close"
        );

        restore_env_traceparent(prev);
    }
}
