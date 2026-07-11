//! Chat-export importers: Claude and `ChatGPT` conversation exports →
//! markdown source files.
//!
//! Both vendors ship a `conversations.json` in their data exports. This
//! module converts each conversation into one markdown transcript under
//! the vault's `sources/` tree (default `sources/chats/`), ready for the
//! normal ingest/enqueue pipeline — the conversations become curated,
//! cited wiki pages like any other source.
//!
//! Format detection is per-conversation-shape, not filename: Claude
//! exports carry `chat_messages`, `ChatGPT` exports carry `mapping`.

use std::fmt::Write as _;

use camino::Utf8PathBuf;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::vault::Vault;

/// Outcome of an import run.
#[derive(Debug, Clone)]
pub struct ImportReport {
    /// Vault-relative paths written.
    pub written: Vec<Utf8PathBuf>,
    /// Conversations skipped (empty or unparseable).
    pub skipped: usize,
}

/// Convert every conversation in `export_json` into a markdown file under
/// `out_dir` (vault-relative). Existing files with the same name are left
/// alone (idempotent re-import).
pub fn import_chat_export(vault: &Vault, export_json: &str, out_dir: &str) -> Result<ImportReport> {
    let root: Value = serde_json::from_str(export_json)
        .map_err(|e| Error::Other(anyhow::anyhow!("chat export parse: {e}")))?;
    let conversations = match &root {
        Value::Array(items) => items.clone(),
        Value::Object(_) => vec![root.clone()],
        _ => {
            return Err(Error::Other(anyhow::anyhow!(
                "chat export must be a JSON array of conversations"
            )))
        }
    };

    let dir = vault.root().join(out_dir);
    std::fs::create_dir_all(dir.as_std_path())
        .map_err(|e| Error::io(dir.clone().into_std_path_buf(), e))?;

    let mut written = Vec::new();
    let mut skipped = 0usize;
    for convo in &conversations {
        match render_conversation(convo) {
            Some((slug, markdown)) => {
                let rel = Utf8PathBuf::from(format!("{out_dir}/{slug}.md"));
                let abs = vault.root().join(&rel);
                if abs.as_std_path().exists() {
                    skipped += 1;
                    continue;
                }
                std::fs::write(abs.as_std_path(), markdown)
                    .map_err(|e| Error::io(abs.into_std_path_buf(), e))?;
                written.push(rel);
            }
            None => skipped += 1,
        }
    }
    Ok(ImportReport { written, skipped })
}

/// Render one conversation to `(file_slug, markdown)`. Returns `None` for
/// conversations with no usable messages.
fn render_conversation(convo: &Value) -> Option<(String, String)> {
    let title = convo
        .get("name")
        .or_else(|| convo.get("title"))
        .and_then(Value::as_str)
        .filter(|t| !t.trim().is_empty())
        .unwrap_or("untitled-conversation");
    let date = convo
        .get("created_at")
        .or_else(|| convo.get("create_time"))
        .map_or_else(|| "undated".into(), date_prefix);

    let turns = if convo.get("chat_messages").is_some() {
        claude_turns(convo)
    } else if convo.get("mapping").is_some() {
        chatgpt_turns(convo)
    } else {
        Vec::new()
    };
    if turns.is_empty() {
        return None;
    }

    let mut md = format!("# {title}\n\n> Imported chat transcript ({date})\n");
    for (role, text) in &turns {
        let _ = write!(md, "\n## {role}\n\n{text}\n");
    }
    let slug = crate::url_fetch::slug_from_title(title);
    let slug = if slug.is_empty() {
        "conversation"
    } else {
        &slug
    };
    Some((format!("{date}-{slug}"), md))
}

/// Claude export: `chat_messages: [{sender, text|content[], created_at}]`.
fn claude_turns(convo: &Value) -> Vec<(String, String)> {
    let Some(messages) = convo.get("chat_messages").and_then(Value::as_array) else {
        return Vec::new();
    };
    messages
        .iter()
        .filter_map(|m| {
            let role = match m.get("sender").and_then(Value::as_str) {
                Some("human") => "Human",
                Some("assistant") => "Assistant",
                _ => return None,
            };
            let text = m
                .get("text")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    // Newer exports: content: [{type: "text", text}]
                    m.get("content").and_then(Value::as_array).map(|parts| {
                        parts
                            .iter()
                            .filter_map(|p| p.get("text").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                })
                .filter(|t| !t.trim().is_empty())?;
            Some((role.to_string(), text))
        })
        .collect()
}

/// `ChatGPT` export: `mapping: {id → {message: {author.role, content.parts,
/// create_time}}}`. Messages are sorted by `create_time`.
fn chatgpt_turns(convo: &Value) -> Vec<(String, String)> {
    let Some(mapping) = convo.get("mapping").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut msgs: Vec<(f64, String, String)> = mapping
        .values()
        .filter_map(|node| {
            let m = node.get("message")?;
            let role = match m.pointer("/author/role").and_then(Value::as_str) {
                Some("user") => "Human",
                Some("assistant") => "Assistant",
                _ => return None,
            };
            let text = m
                .pointer("/content/parts")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .filter(|t| !t.trim().is_empty())?;
            let ts = m
                .get("create_time")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            Some((ts, role.to_string(), text))
        })
        .collect();
    msgs.sort_by(|a, b| a.0.total_cmp(&b.0));
    msgs.into_iter().map(|(_, r, t)| (r, t)).collect()
}

/// Best-effort YYYY-MM-DD prefix from an ISO string or a unix timestamp.
fn date_prefix(v: &Value) -> String {
    if let Some(s) = v.as_str() {
        if s.len() >= 10 {
            return s[..10].to_string();
        }
    }
    if let Some(ts) = v.as_f64() {
        #[allow(clippy::cast_possible_truncation)]
        if let Some(dt) = chrono::DateTime::from_timestamp(ts as i64, 0) {
            return dt.format("%Y-%m-%d").to_string();
        }
    }
    "undated".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_export_renders_turns() {
        let convo = serde_json::json!({
            "name": "Test Chat",
            "created_at": "2026-07-01T10:00:00Z",
            "chat_messages": [
                {"sender": "human", "text": "hello there"},
                {"sender": "assistant", "text": "hi!"}
            ]
        });
        let (slug, md) = render_conversation(&convo).unwrap();
        assert_eq!(slug, "2026-07-01-test-chat");
        assert!(md.contains("## Human\n\nhello there"));
        assert!(md.contains("## Assistant\n\nhi!"));
    }

    #[test]
    fn chatgpt_export_sorts_by_time() {
        let convo = serde_json::json!({
            "title": "GPT Chat",
            "create_time": 1_751_364_000.0,
            "mapping": {
                "b": {"message": {"author": {"role": "assistant"},
                        "content": {"parts": ["answer"]}, "create_time": 2.0}},
                "a": {"message": {"author": {"role": "user"},
                        "content": {"parts": ["question"]}, "create_time": 1.0}}
            }
        });
        let (_, md) = render_conversation(&convo).unwrap();
        let q = md.find("question").unwrap();
        let a = md.find("answer").unwrap();
        assert!(q < a, "user turn must precede assistant turn");
    }

    #[test]
    fn empty_conversation_is_skipped() {
        let convo = serde_json::json!({"name": "Empty", "chat_messages": []});
        assert!(render_conversation(&convo).is_none());
    }
}
