//! `orphan` rule: flags pages with zero inbound wikilinks from any other page.
//!
//! Orphans are the default sign that a page is disconnected from the rest of
//! the vault. They are reported as warnings (not errors) because fresh pages
//! are legitimately orphans until someone links to them — [`VaultTx::validate`]
//! does not block commits on orphan warnings.

use super::{LintIssue, Severity};
use crate::vault::LinkGraph;

pub const RULE: &str = "orphan";

pub fn check(graph: &LinkGraph) -> Vec<LintIssue> {
    graph
        .orphans()
        .map(|info| {
            LintIssue {
                severity: Severity::Warning,
                rule: RULE.into(),
                page: None,
                path: None,
                message: format!("page `{}` has no inbound links", info.title),
            }
            .at(info.id, info.path.clone())
        })
        .collect()
}
