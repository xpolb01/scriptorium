//! Social media data import pipelines.
//!
//! Each submodule handles one platform's export format, converting raw JSON
//! into markdown source files that [`crate::bulk_ingest`] can process.

pub mod facebook;
