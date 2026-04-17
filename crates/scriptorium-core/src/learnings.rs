//! Self-learning journal: append-only JSONL that captures mistakes,
//! patterns, and user corrections.
//!
//! Learnings are injected into LLM prompts as context so the system
//! improves over time. The journal lives at `.scriptorium/learnings.jsonl`
//! and is gitignored (local to each machine, not vault content).
//!
//! **Design references**:
//! - `GStack`'s `SELF_LEARNING_V0.md` — JSONL schema, confidence decay,
//!   dedup at read time
//! - Prometheus-local's `learning_service.py` — capture/retrieve/feedback
//!   loop, auto-validation after repeated success

use std::io::{BufRead, Write};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::vault::Vault;

const LEARNINGS_FILE: &str = "learnings.jsonl";

/// Learning type — what kind of insight this is.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LearningType {
    /// A successful approach that worked well.
    Pattern,
    /// Something to avoid — a known failure mode.
    Pitfall,
    /// A user preference for how things should be done.
    Preference,
    /// A user-corrected mistake (highest confidence, no decay).
    Correction,
    /// A codebase-specific fact or convention.
    DomainKnowledge,
}

/// How the learning was acquired.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LearningSource {
    /// Observed by the agent during operation.
    Observed,
    /// Explicitly stated by the user.
    UserStated,
    /// Inferred from patterns (lowest initial confidence).
    Inferred,
}

/// One learning entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Learning {
    /// When this learning was captured.
    pub ts: DateTime<Utc>,
    /// Which skill produced this learning (e.g. "ingest", "query").
    pub skill: String,
    /// What kind of insight.
    #[serde(rename = "type")]
    pub learning_type: LearningType,
    /// Short kebab-case key for dedup. Same `(key, type)` = latest wins.
    pub key: String,
    /// The actual insight — what was learned.
    pub insight: String,
    /// Confidence 1-10. Decays over time for observed/inferred.
    pub confidence: u8,
    /// How this learning was acquired.
    pub source: LearningSource,
    /// Tags for retrieval matching.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Related file paths (optional context).
    #[serde(default)]
    pub files: Vec<String>,
}

/// Append a learning to the journal.
pub fn capture(vault: &Vault, learning: &Learning) -> Result<()> {
    let path = vault.meta_dir().join(LEARNINGS_FILE);
    let json = serde_json::to_string(learning)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize learning: {e}")))?;

    // Ensure the directory exists.
    if let Some(parent) = path.as_std_path().parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.to_path_buf(), e))?;
    }

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.as_std_path())
        .map_err(|e| Error::io(path.clone().into_std_path_buf(), e))?;
    writeln!(file, "{json}").map_err(|e| Error::io(path.into_std_path_buf(), e))?;
    Ok(())
}

/// Retrieve learnings relevant to the given tags, sorted by effective
/// confidence descending. Deduplicates by `(key, type)` keeping the
/// latest entry. Applies confidence decay for observed/inferred sources.
pub fn retrieve(vault: &Vault, tags: &[&str], limit: usize) -> Result<Vec<Learning>> {
    let all = load_deduped(vault)?;
    let now = Utc::now();

    let mut scored: Vec<(Learning, f32)> = all
        .into_iter()
        .map(|l| {
            let eff = effective_confidence(&l, now);
            (l, eff)
        })
        .filter(|(_, eff)| *eff > 0.0)
        .collect();

    // If tags are provided, boost entries that match.
    if tags.is_empty() {
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        scored.sort_by(|a, b| {
            let a_match = tag_match_score(&a.0, tags);
            let b_match = tag_match_score(&b.0, tags);
            let a_score = a.1 + a_match;
            let b_score = b.1 + b_match;
            b_score
                .partial_cmp(&a_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    Ok(scored.into_iter().take(limit).map(|(l, _)| l).collect())
}

/// Search learnings by keyword in the `insight` and `key` fields.
pub fn search(vault: &Vault, query: &str) -> Result<Vec<Learning>> {
    let all = load_deduped(vault)?;
    let q = query.to_lowercase();
    Ok(all
        .into_iter()
        .filter(|l| {
            l.insight.to_lowercase().contains(&q)
                || l.key.to_lowercase().contains(&q)
                || l.skill.to_lowercase().contains(&q)
        })
        .collect())
}

/// List the N most recent learnings (by timestamp).
pub fn list_recent(vault: &Vault, limit: usize) -> Result<Vec<Learning>> {
    let mut all = load_deduped(vault)?;
    all.sort_by(|a, b| b.ts.cmp(&a.ts));
    all.truncate(limit);
    Ok(all)
}

/// Remove learnings whose effective confidence has decayed to 0.
/// Returns the number pruned.
pub fn prune_stale(vault: &Vault) -> Result<usize> {
    let path = vault.meta_dir().join(LEARNINGS_FILE);
    let all = load_all(vault)?;
    let now = Utc::now();

    let (keep, pruned): (Vec<_>, Vec<_>) = all
        .into_iter()
        .partition(|l| effective_confidence(l, now) > 0.0);

    let pruned_count = pruned.len();
    if pruned_count > 0 {
        // Rewrite the file with only the surviving entries.
        let mut file = std::fs::File::create(path.as_std_path())
            .map_err(|e| Error::io(path.clone().into_std_path_buf(), e))?;
        for l in &keep {
            let json = serde_json::to_string(l)
                .map_err(|e| Error::Other(anyhow::anyhow!("serialize: {e}")))?;
            writeln!(file, "{json}").map_err(|e| Error::io(path.clone().into_std_path_buf(), e))?;
        }
    }
    Ok(pruned_count)
}

/// Format learnings as a prompt section for injection into LLM system prompts.
pub fn format_for_prompt(learnings: &[Learning]) -> String {
    use std::fmt::Write;

    if learnings.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n\n## Prior learnings (from past sessions)\n\n");
    for l in learnings {
        let type_str = match l.learning_type {
            LearningType::Pattern => "pattern",
            LearningType::Pitfall => "PITFALL",
            LearningType::Preference => "preference",
            LearningType::Correction => "CORRECTION",
            LearningType::DomainKnowledge => "domain",
        };
        let _ = writeln!(
            out,
            "- [{type_str}] {insight} (confidence: {conf})",
            insight = l.insight,
            conf = l.confidence,
        );
    }
    out
}

// ── Internal helpers ─────────────────────────────────────────────────────

/// Load all entries from the JSONL file.
fn load_all(vault: &Vault) -> Result<Vec<Learning>> {
    let path = vault.meta_dir().join(LEARNINGS_FILE);
    let file = match std::fs::File::open(path.as_std_path()) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::io(path.into_std_path_buf(), e)),
    };
    let reader = std::io::BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| Error::io(path.clone().into_std_path_buf(), e))?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(learning) = serde_json::from_str::<Learning>(&line) {
            entries.push(learning);
        }
        // Skip malformed lines silently — the journal is append-only and
        // may accumulate minor corruption over time.
    }
    Ok(entries)
}

/// Load and deduplicate: for each `(key, type)` pair, keep only the latest.
fn load_deduped(vault: &Vault) -> Result<Vec<Learning>> {
    let all = load_all(vault)?;
    let mut map: std::collections::HashMap<(String, String), Learning> =
        std::collections::HashMap::new();
    for l in all {
        let type_key = format!("{:?}", l.learning_type).to_lowercase();
        let dedup_key = (l.key.clone(), type_key);
        map.entry(dedup_key)
            .and_modify(|existing| {
                if l.ts > existing.ts {
                    *existing = l.clone();
                }
            })
            .or_insert(l);
    }
    Ok(map.into_values().collect())
}

/// Public wrapper for use by doctor/maintain modules.
pub fn effective_confidence_pub(l: &Learning, now: DateTime<Utc>) -> f32 {
    effective_confidence(l, now)
}

/// Compute effective confidence after decay.
/// - `user_stated` and `correction` sources never decay.
/// - `observed` and `inferred` decay 1 point per 30 days.
fn effective_confidence(l: &Learning, now: DateTime<Utc>) -> f32 {
    let base = f32::from(l.confidence);
    match l.source {
        LearningSource::UserStated => base,
        LearningSource::Observed | LearningSource::Inferred => {
            let age_days = (now - l.ts).num_days().max(0);
            #[allow(clippy::cast_precision_loss)]
            let decay = (age_days as f32) / 30.0;
            (base - decay).max(0.0)
        }
    }
}

/// Score how well a learning's tags match the requested tags.
fn tag_match_score(l: &Learning, tags: &[&str]) -> f32 {
    let mut score = 0.0f32;
    for tag in tags {
        if l.tags.iter().any(|t| t == tag) {
            score += 3.0;
        }
        if l.skill == *tag {
            score += 2.0;
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_learning(
        key: &str,
        lt: LearningType,
        confidence: u8,
        source: LearningSource,
    ) -> Learning {
        Learning {
            ts: Utc::now(),
            skill: "test".into(),
            learning_type: lt,
            key: key.into(),
            insight: format!("insight about {key}"),
            confidence,
            source,
            tags: vec!["test".into()],
            files: vec![],
        }
    }

    #[test]
    fn capture_appends_to_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        std::fs::create_dir_all(dir.path().join(".scriptorium")).unwrap();
        let vault = crate::vault::Vault::open(dir.path()).unwrap();

        let l = make_learning(
            "test-key",
            LearningType::Pattern,
            7,
            LearningSource::Observed,
        );
        capture(&vault, &l).unwrap();
        capture(&vault, &l).unwrap();

        let all = load_all(&vault).unwrap();
        assert_eq!(all.len(), 2, "append-only: two writes = two entries");
    }

    #[test]
    fn retrieve_dedup_by_key_type() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        std::fs::create_dir_all(dir.path().join(".scriptorium")).unwrap();
        let vault = crate::vault::Vault::open(dir.path()).unwrap();

        let mut l1 = make_learning(
            "dup-key",
            LearningType::Pitfall,
            5,
            LearningSource::Observed,
        );
        l1.ts = Utc::now() - Duration::hours(1);
        l1.insight = "old insight".into();
        capture(&vault, &l1).unwrap();

        let l2 = make_learning(
            "dup-key",
            LearningType::Pitfall,
            8,
            LearningSource::Observed,
        );
        capture(&vault, &l2).unwrap();

        let results = retrieve(&vault, &[], 10).unwrap();
        assert_eq!(results.len(), 1, "dedup should keep only latest");
        assert_eq!(results[0].confidence, 8, "latest entry should win");
    }

    #[test]
    fn retrieve_filters_by_tags() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        std::fs::create_dir_all(dir.path().join(".scriptorium")).unwrap();
        let vault = crate::vault::Vault::open(dir.path()).unwrap();

        let mut l1 = make_learning("tagged", LearningType::Pattern, 7, LearningSource::Observed);
        l1.tags = vec!["llm".into(), "ingest".into()];
        capture(&vault, &l1).unwrap();

        let mut l2 = make_learning(
            "untagged",
            LearningType::Pattern,
            9,
            LearningSource::Observed,
        );
        l2.tags = vec!["unrelated".into()];
        capture(&vault, &l2).unwrap();

        let results = retrieve(&vault, &["ingest"], 10).unwrap();
        // Both should be returned, but the tagged one should rank higher
        // despite lower base confidence due to tag match bonus.
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "tagged", "tag-matched should rank first");
    }

    #[test]
    fn confidence_decay_applied() {
        let mut l = make_learning("old", LearningType::Pitfall, 5, LearningSource::Observed);
        // Set timestamp to 90 days ago.
        l.ts = Utc::now() - Duration::days(90);
        let eff = effective_confidence(&l, Utc::now());
        // 5 - (90/30) = 5 - 3 = 2
        assert!((eff - 2.0).abs() < 0.1, "expected ~2.0, got {eff}");
    }

    #[test]
    fn user_stated_does_not_decay() {
        let mut l = make_learning(
            "pref",
            LearningType::Preference,
            8,
            LearningSource::UserStated,
        );
        l.ts = Utc::now() - Duration::days(365);
        let eff = effective_confidence(&l, Utc::now());
        assert!((eff - 8.0).abs() < 0.01, "user-stated should not decay");
    }

    #[test]
    fn search_matches_insight_text() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        std::fs::create_dir_all(dir.path().join(".scriptorium")).unwrap();
        let vault = crate::vault::Vault::open(dir.path()).unwrap();

        let mut l = make_learning(
            "search-test",
            LearningType::Pattern,
            7,
            LearningSource::Observed,
        );
        l.insight = "Claude sometimes omits log_entry on large ingests".into();
        capture(&vault, &l).unwrap();

        let results = search(&vault, "log_entry").unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].insight.contains("log_entry"));

        let results = search(&vault, "nonexistent").unwrap();
        assert!(results.is_empty());
    }
}
