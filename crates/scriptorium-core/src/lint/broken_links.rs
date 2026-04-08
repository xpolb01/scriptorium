//! `broken_link` rule: flags wikilinks whose target has no matching page,
//! and `ambiguous_link` for targets that match more than one page.

use super::{LintIssue, Severity};
use crate::vault::LinkGraph;

pub const BROKEN: &str = "broken_link";
pub const AMBIGUOUS: &str = "ambiguous_link";

pub fn check(graph: &LinkGraph) -> Vec<LintIssue> {
    let mut issues = Vec::new();
    for (owner, link_ref) in graph.broken_links() {
        let Some(info) = graph.page_info(owner) else {
            continue;
        };
        issues.push(
            LintIssue {
                severity: Severity::Error,
                rule: BROKEN.into(),
                page: None,
                path: None,
                message: format!(
                    "broken wikilink `[[{}]]` — no page with stem `{}` exists",
                    link_ref.link.target, link_ref.link.target
                ),
            }
            .at(owner, info.path.clone()),
        );
    }
    for (owner, link_ref) in graph.ambiguous_links() {
        let Some(info) = graph.page_info(owner) else {
            continue;
        };
        issues.push(
            LintIssue {
                severity: Severity::Warning,
                rule: AMBIGUOUS.into(),
                page: None,
                path: None,
                message: format!(
                    "ambiguous wikilink `[[{}]]` — multiple pages share this stem",
                    link_ref.link.target
                ),
            }
            .at(owner, info.path.clone()),
        );
    }
    issues
}
