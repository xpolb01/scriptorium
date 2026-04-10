//! Mechanical lint rules — things we can check without calling an LLM.
//!
//! Every rule produces a `Vec<LintIssue>`. [`run`] wires them together over a
//! fresh [`Vault`] scan and [`LinkGraph`]. Higher-level callers (CLI, MCP,
//! `VaultTx` validation) consume the [`LintReport`] and decide what to do:
//!
//! - CLI `scriptorium lint` prints the issues
//! - `VaultTx::validate` blocks a commit if any [`Severity::Error`] is present
//!
//! LLM-assisted rules (`stale`, `contradictions`, `missing cross-references`)
//! are deferred to v2. The pattern for adding them is the same as the
//! mechanical rules: one file per rule, each returning a `Vec<LintIssue>`.

pub mod broken_links;
pub mod fix;
pub mod frontmatter;
pub mod missing_tags;
pub mod orphans;
pub mod page_structure;
pub mod stale;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::vault::{LinkGraph, PageId, Vault};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintIssue {
    pub severity: Severity,
    /// Short stable id for the rule, e.g. `broken_link`, `orphan`,
    /// `frontmatter.missing_title`. Stable so users can suppress by id.
    pub rule: String,
    /// The page the issue belongs to, if any. `None` means a vault-level issue.
    pub page: Option<PageId>,
    /// Relative path of the page the issue belongs to, duplicated from
    /// `page` for convenient display without a graph lookup.
    pub path: Option<Utf8PathBuf>,
    pub message: String,
}

impl LintIssue {
    pub fn error(rule: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            rule: rule.into(),
            page: None,
            path: None,
            message: message.into(),
        }
    }

    pub fn warning(rule: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            rule: rule.into(),
            page: None,
            path: None,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn at(mut self, page: PageId, path: Utf8PathBuf) -> Self {
        self.page = Some(page);
        self.path = Some(path);
        self
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LintReport {
    pub issues: Vec<LintIssue>,
}

impl LintReport {
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn has_errors(&self) -> bool {
        self.issues
            .iter()
            .any(|i| matches!(i.severity, Severity::Error))
    }

    pub fn count_by_severity(&self, sev: Severity) -> usize {
        self.issues.iter().filter(|i| i.severity == sev).count()
    }

    pub fn errors(&self) -> impl Iterator<Item = &LintIssue> {
        self.issues
            .iter()
            .filter(|i| matches!(i.severity, Severity::Error))
    }
}

/// Run every mechanical rule against a vault and return the combined report.
///
/// The vault is scanned once and the [`LinkGraph`] is built once, then each
/// rule inspects the shared inputs. Rule order is fixed so the report is
/// deterministic.
pub fn run(vault: &Vault) -> Result<LintReport> {
    let scan = vault.scan()?;
    let graph = LinkGraph::build(&scan.pages);
    let mut issues = Vec::new();
    // Per-file scan errors (e.g. missing frontmatter) become lint errors.
    for err in &scan.errors {
        issues.push(LintIssue {
            severity: Severity::Error,
            rule: "scan".into(),
            page: None,
            path: camino::Utf8PathBuf::from_path_buf(err.path.clone()).ok(),
            message: err.message.clone(),
        });
    }
    issues.extend(frontmatter::check(&scan.pages));
    issues.extend(broken_links::check(&graph));
    issues.extend(orphans::check(&graph));
    issues.extend(stale::check(vault, &scan.pages));
    issues.extend(missing_tags::check(&scan.pages));
    issues.extend(page_structure::check(&scan.pages));
    // Stable sort by severity then path then rule so diffs of `lint` output
    // between runs are clean.
    issues.sort_by(|a, b| {
        a.severity
            .cmp(&b.severity)
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.rule.cmp(&b.rule))
            .then_with(|| a.message.cmp(&b.message))
    });
    Ok(LintReport { issues })
}

impl PartialOrd for Severity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Severity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn rank(s: Severity) -> u8 {
            match s {
                Severity::Error => 0,
                Severity::Warning => 1,
                Severity::Info => 2,
            }
        }
        rank(*self).cmp(&rank(*other))
    }
}
