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
pub mod resource;
pub mod schema;

pub use envelope::{
    payload_cap_from_env, Attributes, LogRecord, PayloadCap, SeverityNumber, Source, Span,
    SpanEvent, SpanId, SpanKind, Status, TraceId, DEFAULT_PAYLOAD_CAP,
};
pub use resource::Resource;
