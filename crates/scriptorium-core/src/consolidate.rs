//! Vault consolidation: find near-duplicate pages and merge them.
//!
//! A growing curated vault accumulates overlapping pages — the same
//! concept ingested twice under different names, or two sources curated
//! into parallel pages. Mechanical lint can't see this (the pages are
//! structurally fine); this module can:
//!
//! 1. **Detect** — embed a fixed-length prefix of every page body in one
//!    batched call, compute pairwise cosine similarity, and union pairs
//!    above the threshold into groups.
//! 2. **Merge** (`apply` mode) — for each group, an LLM writes one merged
//!    body onto the *survivor* (the longest page); the other pages become
//!    explicit redirect stubs (`Merged into [[survivor]].`) so their IDs,
//!    inbound wikilinks, and history stay valid. One commit per run;
//!    `scriptorium undo` reverts it.
//!
//! Dry-run (report only) is the default — merging is an LLM judgement
//! call and deserves a human glance first.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{Error, Result};
use crate::llm::{CompletionRequest, LlmProvider, Message, Role};
use crate::vault::{Page, Vault};

/// How many characters of each page body participate in similarity.
const PREFIX_CHARS: usize = 1500;

/// One group of near-duplicate pages (page stems, plus the minimum
/// pairwise similarity that holds the group together).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DupGroup {
    pub stems: Vec<String>,
    pub min_similarity: f32,
}

/// Outcome of a consolidate run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidateReport {
    pub groups: Vec<DupGroup>,
    /// Number of pages merged away (turned into redirect stubs).
    pub merged: usize,
    /// Commit id when `apply` ran and something merged.
    pub commit_id: Option<String>,
}

/// Find groups of near-duplicate pages by embedding a prefix of each body.
pub async fn find_duplicate_groups(
    vault: &Vault,
    embed_provider: &dyn LlmProvider,
    threshold: f32,
) -> Result<Vec<DupGroup>> {
    let scan = vault.scan()?;
    let pages: Vec<&Page> = scan
        .pages
        .iter()
        .filter(|p| p.path.starts_with("wiki/") && !p.body.trim().is_empty())
        .collect();
    if pages.len() < 2 {
        return Ok(Vec::new());
    }
    let texts: Vec<String> = pages
        .iter()
        .map(|p| p.body.chars().take(PREFIX_CHARS).collect())
        .collect();
    let vectors = embed_provider
        .embed(&texts)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("embed pages for consolidate: {e}")))?;
    if vectors.len() != pages.len() {
        return Err(Error::Other(anyhow::anyhow!(
            "provider returned {} vectors for {} pages",
            vectors.len(),
            pages.len()
        )));
    }

    // Union-find over pairs above the threshold.
    let n = pages.len();
    let mut parent: Vec<usize> = (0..n).collect();
    let mut pair_sim: Vec<(usize, usize, f32)> = Vec::new();
    for i in 0..n {
        for j in (i + 1)..n {
            let sim = cosine(&vectors[i], &vectors[j]);
            if sim >= threshold {
                pair_sim.push((i, j, sim));
                let (ri, rj) = (uf_find(&mut parent, i), uf_find(&mut parent, j));
                if ri != rj {
                    parent[ri] = rj;
                }
            }
        }
    }

    let mut groups: std::collections::HashMap<usize, (Vec<usize>, f32)> =
        std::collections::HashMap::new();
    for (i, j, sim) in pair_sim {
        let root = uf_find(&mut parent, i);
        let entry = groups.entry(root).or_insert_with(|| (Vec::new(), 1.0));
        for m in [i, j] {
            if !entry.0.contains(&m) {
                entry.0.push(m);
            }
        }
        entry.1 = entry.1.min(sim);
    }

    let mut out: Vec<DupGroup> = groups
        .into_values()
        .map(|(members, min_similarity)| {
            let mut stems: Vec<String> = members
                .iter()
                .filter_map(|&m| pages[m].path.file_stem().map(ToString::to_string))
                .collect();
            stems.sort();
            DupGroup {
                stems,
                min_similarity,
            }
        })
        .collect();
    out.sort_by(|a, b| b.min_similarity.total_cmp(&a.min_similarity));
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct MergedBody {
    merged_body: String,
}

/// Consolidate the vault: detect near-duplicate groups and, in `apply`
/// mode, merge each group into its longest member.
pub async fn consolidate(
    vault: &Vault,
    chat: &dyn LlmProvider,
    embed_provider: &dyn LlmProvider,
    threshold: f32,
    apply: bool,
) -> Result<ConsolidateReport> {
    let groups = find_duplicate_groups(vault, embed_provider, threshold).await?;
    if !apply || groups.is_empty() {
        return Ok(ConsolidateReport {
            groups,
            merged: 0,
            commit_id: None,
        });
    }

    let scan = vault.scan()?;
    let mut tx = vault.begin();
    let mut merged = 0usize;
    for group in &groups {
        let mut members: Vec<&Page> = scan
            .pages
            .iter()
            .filter(|p| {
                p.path
                    .file_stem()
                    .is_some_and(|s| group.stems.iter().any(|g| g == s))
            })
            .collect();
        if members.len() < 2 {
            continue;
        }
        // Survivor: the longest body (most curated content to preserve).
        members.sort_by_key(|p| std::cmp::Reverse(p.body.len()));
        let survivor = members[0];
        let others = &members[1..];

        let merged_body = merge_bodies(chat, survivor, others).await?;
        let survivor_stem = survivor.path.file_stem().unwrap_or_default().to_string();

        // Survivor: merged body; union of sources, tags, aliases (plus the
        // absorbed pages' titles/stems as aliases so lookups keep working).
        let mut fm = survivor.frontmatter.clone();
        for other in others {
            for s in &other.frontmatter.sources {
                if !fm.sources.contains(s) {
                    fm.sources.push(s.clone());
                }
            }
            for t in &other.frontmatter.tags {
                if !fm.tags.contains(t) {
                    fm.tags.push(t.clone());
                }
            }
            for a in &other.frontmatter.aliases {
                if !fm.aliases.contains(a) {
                    fm.aliases.push(a.clone());
                }
            }
            if !fm.aliases.contains(&other.frontmatter.title) {
                fm.aliases.push(other.frontmatter.title.clone());
            }
        }
        fm.updated = chrono::Utc::now();
        tx.write_page(&Page {
            path: survivor.path.clone(),
            frontmatter: fm,
            body: merged_body,
        })?;

        for other in others {
            let mut ofm = other.frontmatter.clone();
            ofm.updated = chrono::Utc::now();
            let stub = format!(
                "# {}\n\nMerged into [[{}]].\n",
                other.frontmatter.title, survivor_stem
            );
            tx.write_page(&Page {
                path: other.path.clone(),
                frontmatter: ofm,
                body: stub,
            })?;
            merged += 1;
        }
    }

    if merged == 0 {
        return Ok(ConsolidateReport {
            groups,
            merged: 0,
            commit_id: None,
        });
    }
    let commit_id = tx.commit(&format!(
        "[consolidate] merge {merged} near-duplicate page(s)"
    ))?;
    Ok(ConsolidateReport {
        groups,
        merged,
        commit_id: Some(commit_id),
    })
}

/// One LLM call merging the group's bodies into the survivor's voice.
async fn merge_bodies(chat: &dyn LlmProvider, survivor: &Page, others: &[&Page]) -> Result<String> {
    let schema = json!({
        "type": "object",
        "properties": {
            "merged_body": {"type": "string", "description": "The complete merged markdown body for the surviving page."}
        },
        "required": ["merged_body"],
        "additionalProperties": false
    });
    let mut listing = format!(
        "PRIMARY PAGE ({}):\n{}\n",
        survivor.frontmatter.title, survivor.body
    );
    for other in others {
        use std::fmt::Write as _;
        let _ = write!(
            listing,
            "\nDUPLICATE PAGE ({}):\n{}\n",
            other.frontmatter.title, other.body
        );
    }
    let req = CompletionRequest {
        system: "You merge near-duplicate wiki pages. Produce ONE merged \
                 markdown body for the primary page that preserves every \
                 distinct fact, citation, and [[wikilink]] from all versions, \
                 without duplicating shared material. Keep the primary page's \
                 structure and voice. Do not include frontmatter. Return JSON."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: listing,
        }],
        max_tokens: 16_384,
        temperature: Some(0.0),
        response_schema: Some(schema),
    };
    let resp = chat
        .complete(req)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("merge llm: {e}")))?;
    let payload = crate::llm::extract_json_payload(&resp.text);
    let parsed: MergedBody = serde_json::from_str(&payload)
        .map_err(|e| Error::Other(anyhow::anyhow!("merge response parse: {e}")))?;
    if parsed.merged_body.trim().is_empty() {
        return Err(Error::Other(anyhow::anyhow!("merge produced empty body")));
    }
    Ok(parsed.merged_body)
}

/// Path-compressing union-find lookup.
fn uf_find(parent: &mut Vec<usize>, i: usize) -> usize {
    if parent[i] != i {
        let root = uf_find(parent, parent[i]);
        parent[i] = root;
    }
    parent[i]
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denom = na * nb;
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_basics() {
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
        assert!(cosine(&[], &[]).abs() < f32::EPSILON);
    }
}
