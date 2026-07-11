//! Answer- and curation-quality evaluation (LLM-as-judge).
//!
//! The retrieval benchmarks in [`crate::bench`] measure whether search
//! finds the right pages. This module measures the half that retrieval
//! metrics cannot see: whether *text* — an answer, or a curated wiki
//! page — is actually supported by its sources.
//!
//! Three checks, all built on one judge engine:
//!
//! | Check | Subject | Context |
//! |-------|---------|---------|
//! | Faithfulness | any answer text | the retrieved pages |
//! | Curation audit | a wiki page body | its interned `sources/` files |
//! | Citation correctness | each cited claim | the specific cited page |
//!
//! The judge decomposes the subject into atomic factual claims and
//! verifies each strictly against the provided context (RAGAS/TruLens
//! faithfulness pattern, reimplemented natively). Scores are
//! `supported / total`. Everything runs on the already-configured chat
//! provider — no new service, model, or API.

pub mod context_precision;
pub mod faithfulness;

pub use context_precision::{judge_context_precision, ContextPrecisionReport};
pub use faithfulness::{
    citation_audit, curation_audit, judge_faithfulness, ClaimVerdict, FaithfulnessReport,
};
