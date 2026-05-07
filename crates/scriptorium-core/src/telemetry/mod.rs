#![allow(clippy::doc_markdown)]

//! Scriptorium telemetry subsystem.
//!
//! We implement OTel data models in-process without the `opentelemetry-sdk`
//! crate (`tracing::Layer` + custom SQLite bridge). The shape of logs and
//! spans conforms to the OpenTelemetry Logs and Trace specifications so
//! that emitted rows can be exported to any OTLP collector later without a
//! schema migration.
//!
//! Submodules:
//!
//! - [`envelope`] — OTel-shaped record types ([`LogRecord`], [`Span`],
//!   [`TraceId`], [`SpanId`], [`PayloadCap`], severity). This is the
//!   contract for every other telemetry module.
//!
//! Downstream modules (schema, propagation, resource, store, payload,
//! layer) will land in later tasks (T2, T4, T5, T7, T8, T9) and build on
//! the types defined here.

pub mod envelope;
pub mod migration;
pub mod payload;
pub mod propagation;
pub mod resource;
pub mod schema;
pub mod store;
pub mod tracing_layer;

pub use envelope::{
    payload_cap_from_env, Attributes, LogRecord, PayloadCap, SeverityNumber, Source, Span,
    SpanEvent, SpanId, SpanKind, Status, TraceId, DEFAULT_PAYLOAD_CAP,
};
pub use migration::{backfill_hook_events, BackfillReport};
pub use payload::{add_truncation_attrs, cap_attributes, cap_body, cap_bytes, TruncationMeta};
pub use propagation::{TraceContext, ENV_SESSION_ID, ENV_TRACEPARENT, ENV_TURN_ID};
pub use resource::Resource;
pub use store::{
    Cursor, DropReason, InsertOutcome, LogFilters, LogRow, SpanFilters, SpanRow, TelemetryStats,
    TelemetryStore, TimelineEntry, TimelineResult, TraceTree, GLOBAL_STATS,
};
pub use tracing_layer::{install_global, TelemetrySqliteLayer};
