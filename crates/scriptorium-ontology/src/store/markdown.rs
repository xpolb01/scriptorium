//! Markdown notes as instances: one `.md` file with YAML frontmatter per
//! entity, read from the git snapshot (so, like YAML records, any revision
//! can be served).
//!
//! - The **type** is fixed (`type = "Person"`) or read from a frontmatter
//!   field (`type_field = "type"`).
//! - The **key** is the file stem, or a frontmatter field
//!   (`key = "frontmatter:id"`, e.g. scriptorium's page ULIDs).
//! - **Fields** are the frontmatter keys. A string that is exactly a
//!   `[[wikilink]]` names another note and is replaced by that note's key,
//!   so Obsidian-style relations (`authors: ["[[vaswani]]"]`) just work.
//! - `body_field` receives the markdown body; `links_field` receives the
//!   keys of the notes the body links to, which makes backlinks an ordinary
//!   `@inverse(of: "<links_field>")`.
//!
//! The whole directory is loaded once per store and indexed in memory.
//! Files that cannot be loaded are skipped and reported by `check`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use regex::Regex;

use super::{EntityStore, ListQuery, Record};
use crate::error::{Error, Result};
use crate::layout::{join, normalize_dir};
use crate::manifest::MarkdownConfig;
use crate::snapshot::Snapshot;

const FRONTMATTER_KEY: &str = "frontmatter:";

/// Where a note's key comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeySource {
    Stem,
    Frontmatter(String),
}

/// Where a note's entity type comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TypeSource {
    Fixed(String),
    Field(String),
}

/// A note that loaded, and where it came from.
#[derive(Debug, Clone)]
pub struct Note {
    pub path: String,
    pub type_name: String,
    pub key: String,
    pub record: Arc<Record>,
}

/// A note that did not load, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Default)]
struct Loaded {
    /// `(type, key)` → note, in type then key order.
    notes: BTreeMap<(String, String), Note>,
    problems: Vec<Problem>,
}

/// Referenced key → keys of the notes of one type whose field references it.
type ReferenceIndex = HashMap<String, Vec<String>>;
type ReferenceIndexes = HashMap<(String, String), Arc<OnceLock<Arc<ReferenceIndex>>>>;

pub struct MarkdownStore {
    snapshot: Arc<dyn Snapshot>,
    sub: String,
    dir: String,
    types: TypeSource,
    key: KeySource,
    body_field: Option<String>,
    links_field: Option<String>,
    /// Entity type → key field name, so every record carries its own key.
    key_fields: BTreeMap<String, String>,
    loaded: OnceLock<std::result::Result<Loaded, String>>,
    references: Mutex<ReferenceIndexes>,
}

impl MarkdownStore {
    pub fn new(
        snapshot: Arc<dyn Snapshot>,
        sub: &str,
        config: &MarkdownConfig,
        key_fields: BTreeMap<String, String>,
    ) -> Result<Self> {
        let invalid = |message: &str| Error::Store {
            store: format!("markdown:{sub}"),
            message: message.to_string(),
        };
        let types = match (&config.type_name, &config.type_field) {
            (Some(name), None) => TypeSource::Fixed(name.clone()),
            (None, Some(field)) => TypeSource::Field(field.clone()),
            _ => return Err(invalid("set exactly one of `type` and `type_field`")),
        };
        let key = if config.key == "stem" {
            KeySource::Stem
        } else if let Some(field) = config.key.strip_prefix(FRONTMATTER_KEY) {
            if field.is_empty() {
                return Err(invalid("`key = \"frontmatter:\"` needs a field name"));
            }
            KeySource::Frontmatter(field.to_string())
        } else {
            return Err(invalid("`key` must be `stem` or `frontmatter:<field>`"));
        };
        let dir = normalize_dir(&config.dir);
        if dir.split('/').any(|part| part == "..") || dir.starts_with('/') {
            return Err(invalid("`dir` must be a path inside the repository"));
        }
        Ok(Self {
            snapshot,
            sub: sub.to_string(),
            dir,
            types,
            key,
            body_field: config.body_field.clone(),
            links_field: config.links_field.clone(),
            key_fields,
            loaded: OnceLock::new(),
            references: Mutex::new(HashMap::new()),
        })
    }

    fn loaded(&self) -> Result<&Loaded> {
        self.loaded
            .get_or_init(|| self.load().map_err(|e| e.to_string()))
            .as_ref()
            .map_err(|message| Error::Store {
                store: self.describe(),
                message: message.clone(),
            })
    }

    /// Every note that loaded, and every file that did not.
    pub fn notes(&self) -> Result<(Vec<Note>, Vec<Problem>)> {
        let loaded = self.loaded()?;
        Ok((
            loaded.notes.values().cloned().collect(),
            loaded.problems.clone(),
        ))
    }

    fn load(&self) -> Result<Loaded> {
        let mut files = Vec::new();
        self.walk(&self.dir, &mut files)?;

        // Pass 1: frontmatter, type, key.
        let mut parsed = Vec::new();
        let mut loaded = Loaded::default();
        for path in files {
            match self.parse_file(&path)? {
                Ok(note) => parsed.push(note),
                Err(message) => loaded.problems.push(Problem { path, message }),
            }
        }

        // Stems resolve `[[wikilinks]]` to keys. A stem shared by two notes
        // is ambiguous, so links to it resolve to nothing.
        let mut stems: HashMap<String, Option<String>> = HashMap::new();
        for note in &parsed {
            stems
                .entry(note.stem.to_lowercase())
                .and_modify(|key| *key = None)
                .or_insert_with(|| Some(note.key.clone()));
        }
        let resolve = |target: &str| -> Option<String> {
            stems.get(&target.to_lowercase()).cloned().flatten()
        };

        // Pass 2: resolve links, attach body and key.
        for ParsedNote {
            path,
            stem: _,
            type_name,
            key,
            mut record,
            body,
        } in parsed
        {
            for value in record.values_mut() {
                resolve_wikilink_values(value, &resolve);
            }
            if let Some(field) = &self.links_field {
                let mut links: Vec<serde_json::Value> = Vec::new();
                for target in wikilink_targets(&body) {
                    if let Some(key) = resolve(&target) {
                        let key = serde_json::Value::String(key);
                        if !links.contains(&key) {
                            links.push(key);
                        }
                    }
                }
                record.insert(field.clone(), serde_json::Value::Array(links));
            }
            if let Some(field) = &self.body_field {
                record.insert(field.clone(), serde_json::Value::String(body));
            }
            if let Some(key_field) = self.key_fields.get(&type_name) {
                record.insert(key_field.clone(), serde_json::Value::String(key.clone()));
            }
            let id = (type_name.clone(), key.clone());
            if let Some(existing) = loaded.notes.get(&id) {
                loaded.problems.push(Problem {
                    path: path.clone(),
                    message: format!(
                        "{type_name}:{key} is also defined by {}; keys must be unique",
                        existing.path
                    ),
                });
                continue;
            }
            loaded.notes.insert(
                id,
                Note {
                    path,
                    type_name,
                    key,
                    record: Arc::new(record),
                },
            );
        }
        Ok(loaded)
    }

    fn walk(&self, dir: &str, out: &mut Vec<String>) -> Result<()> {
        for entry in self.snapshot.list(dir)? {
            // Obsidian keeps its own state in dot-directories.
            if entry.name.starts_with('.') {
                continue;
            }
            let path = join(dir, &entry.name);
            if entry.is_dir {
                self.walk(&path, out)?;
            } else if is_markdown(&entry.name) {
                out.push(path);
            }
        }
        Ok(())
    }

    /// `Err` is a per-file problem; the outer `Result` is a read failure.
    fn parse_file(&self, path: &str) -> Result<std::result::Result<ParsedNote, String>> {
        let Some(text) = self.snapshot.read_string(path)? else {
            return Ok(Err("file disappeared while loading".into()));
        };
        let stem = std::path::Path::new(path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_string();
        let (record, body) = match split_frontmatter(&text) {
            Ok(split) => split,
            Err(message) => return Ok(Err(message)),
        };
        let type_name = match &self.types {
            TypeSource::Fixed(name) => name.clone(),
            TypeSource::Field(field) => match record.get(field).and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => return Ok(Err(format!("no `{field}` field naming the entity type"))),
            },
        };
        let key = match &self.key {
            KeySource::Stem => stem.clone(),
            KeySource::Frontmatter(field) => match record.get(field) {
                Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
                Some(serde_json::Value::Number(n)) => n.to_string(),
                _ => return Ok(Err(format!("no `{field}` field to use as the key"))),
            },
        };
        Ok(Ok(ParsedNote {
            path: path.to_string(),
            stem,
            type_name,
            key,
            record,
            body,
        }))
    }

    fn references_of(&self, type_name: &str, field: &str) -> Result<Arc<ReferenceIndex>> {
        let loaded = self.loaded()?;
        let slot = Arc::clone(
            self.references
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry((type_name.to_string(), field.to_string()))
                .or_default(),
        );
        Ok(Arc::clone(slot.get_or_init(|| {
            let mut index = ReferenceIndex::new();
            for note in loaded.notes.values().filter(|n| n.type_name == type_name) {
                let mut add = |value: &serde_json::Value| {
                    let target = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => return,
                    };
                    let sources = index.entry(target).or_default();
                    if sources.last() != Some(&note.key) {
                        sources.push(note.key.clone());
                    }
                };
                match note.record.get(field) {
                    Some(serde_json::Value::Array(items)) => items.iter().for_each(&mut add),
                    Some(value) => add(value),
                    None => {}
                }
            }
            Arc::new(index)
        })))
    }
}

fn is_markdown(name: &str) -> bool {
    std::path::Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md"))
}

struct ParsedNote {
    path: String,
    stem: String,
    type_name: String,
    key: String,
    record: Record,
    body: String,
}

#[async_trait]
impl EntityStore for MarkdownStore {
    fn describe(&self) -> String {
        format!("markdown:{}@{}", self.dir, self.snapshot.label())
    }

    async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
        Ok(self
            .loaded()?
            .notes
            .get(&(type_name.to_string(), key.to_string()))
            .map(|note| Arc::clone(&note.record)))
    }

    async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>> {
        let loaded = self.loaded()?;
        let limit = query.limit.unwrap_or(usize::MAX);
        if let Some((field, key)) = &query.references {
            let index = self.references_of(type_name, field)?;
            let keys = index.get(key).map(Vec::as_slice).unwrap_or_default();
            return Ok(keys
                .iter()
                .filter_map(|k| loaded.notes.get(&(type_name.to_string(), k.clone())))
                .take(limit)
                .map(|note| Arc::clone(&note.record))
                .collect());
        }
        Ok(loaded
            .notes
            .range((type_name.to_string(), String::new())..)
            .take_while(|((t, _), _)| t == type_name)
            .map(|(_, note)| Arc::clone(&note.record))
            .take(limit)
            .collect())
    }

    fn prepare_references(&self, type_name: &str, field: &str) -> Result<()> {
        self.references_of(type_name, field).map(|_| ())
    }

    async fn ready(&self) -> Result<()> {
        let loaded = self.loaded()?;
        if !loaded.problems.is_empty() {
            tracing::warn!(
                sub_ontology = %self.sub,
                skipped = loaded.problems.len(),
                "some markdown notes were skipped; run `scriptorium-ontology check`"
            );
        }
        Ok(())
    }
}

/// Split `---` YAML frontmatter from the body. A note without frontmatter
/// is an empty record with the whole text as its body.
fn split_frontmatter(text: &str) -> std::result::Result<(Record, String), String> {
    // gray_matter reads an unclosed block as no frontmatter at all, so its
    // fields would become body text.
    let mut lines = text.lines();
    if lines.next().is_some_and(|first| first.trim_end() == "---")
        && !lines.any(|line| line.trim_end() == "---")
    {
        return Err(
            "frontmatter is not valid YAML: `---` opens it, but no `---` line closes it".into(),
        );
    }
    let matter = gray_matter::Matter::<gray_matter::engine::YAML>::new();
    let parsed = matter.parse(text);
    let body = parsed.content.trim_start_matches('\n').to_string();
    let Some(data) = parsed.data else {
        if text.trim_start().starts_with("---") && !parsed.matter.trim().is_empty() {
            return Err("frontmatter is not valid YAML".into());
        }
        return Ok((Record::new(), body));
    };
    match data.deserialize::<serde_json::Value>() {
        Ok(serde_json::Value::Object(map)) => Ok((map, body)),
        // gray_matter reports every YAML error, a key given twice included,
        // as null frontmatter: only matter that says nothing is empty.
        Ok(serde_json::Value::Null) if !says_nothing(&parsed.matter) => {
            Err("frontmatter is not valid YAML (a syntax error, or a key given twice)".into())
        }
        Ok(serde_json::Value::Null) => Ok((Record::new(), body)),
        Ok(_) => Err("frontmatter must be a mapping of field names to values".into()),
        Err(e) => Err(format!("frontmatter could not be read: {e}")),
    }
}

/// Whether YAML `matter` holds nothing: blank, only comments, or `~`/`null`.
fn says_nothing(matter: &str) -> bool {
    let content: Vec<&str> = matter
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    matches!(content.as_slice(), [] | ["~" | "null" | "Null" | "NULL"])
}

fn wikilink_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // [[target]], [[target|alias]], [[target#heading]]. An `!` before
        // it makes an embed, which callers test for themselves: consuming
        // the character before `[[` would swallow adjacent links.
        Regex::new(r"\[\[([^\[\]|#]+)(?:#[^\[\]|]*)?(?:\|[^\[\]]*)?\]\]")
            .expect("valid wikilink regex")
    })
}

/// The link target of a string that is exactly one `[[wikilink]]`.
fn whole_wikilink(value: &str) -> Option<&str> {
    let inner = value.trim().strip_prefix("[[")?.strip_suffix("]]")?;
    if inner.contains("[[") || inner.contains("]]") {
        return None;
    }
    let target = inner.split(['|', '#']).next()?.trim();
    (!target.is_empty()).then_some(target)
}

fn resolve_wikilink_values(
    value: &mut serde_json::Value,
    resolve: &impl Fn(&str) -> Option<String>,
) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(target) = whole_wikilink(s) {
                // An unresolvable link keeps its target as a (dangling) key.
                *s = resolve(target).unwrap_or_else(|| target.to_string());
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                resolve_wikilink_values(item, resolve);
            }
        }
        _ => {}
    }
}

/// `[[wikilink]]` targets in a markdown body, in order, skipping fenced code
/// blocks and inline code.
fn wikilink_targets(body: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut fence: Option<&str> = None;
    for line in body.lines() {
        let trimmed = line.trim_start();
        let marker = ["```", "~~~"].into_iter().find(|m| trimmed.starts_with(m));
        match (fence, marker) {
            (None, Some(m)) => {
                fence = Some(m);
                continue;
            }
            (Some(open), Some(m)) if open == m => {
                fence = None;
                continue;
            }
            (Some(_), _) => continue,
            (None, None) => {}
        }
        let without_code = strip_inline_code(line);
        for capture in wikilink_regex().captures_iter(&without_code) {
            let start = capture.get(0).map_or(0, |m| m.start());
            if without_code[..start].ends_with('!') {
                continue;
            }
            targets.push(capture[1].trim().to_string());
        }
    }
    targets
}

fn strip_inline_code(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_code = false;
    for c in line.chars() {
        if c == '`' {
            in_code = !in_code;
            out.push(' ');
        } else if !in_code {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};
    use serde_json::json;

    fn config(extra: &str) -> MarkdownConfig {
        toml::from_str(&format!("dir = \"notes\"\n{extra}")).unwrap()
    }

    fn store(dir: &std::path::Path, extra: &str, keys: &[(&str, &str)]) -> MarkdownStore {
        MarkdownStore::new(
            Arc::new(GitSnapshot::open(dir, "HEAD").unwrap()),
            "people",
            &config(extra),
            keys.iter()
                .map(|(t, k)| ((*t).to_string(), (*k).to_string()))
                .collect(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn notes_with_fixed_type_stem_keys_and_wikilink_relations() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("notes/vaswani.md", "---\nname: Ashish Vaswani\nfriends: [\"[[Shazeer|Noam]]\"]\n---\nWorked with [[shazeer]] and [[missing]].\n"),
                ("notes/sub/shazeer.md", "---\nname: Noam Shazeer\n---\nSee `[[not-a-link]]` and ![[embed]].\n\n```\n[[in-code]]\n```\n"),
                ("notes/.obsidian/workspace.md", "ignored"),
                ("notes/readme.txt", "not markdown"),
            ],
            "init",
        );
        let s = store(
            dir.path(),
            "type = \"Person\"\nbody_field = \"body\"\nlinks_field = \"links\"",
            &[("Person", "id")],
        );
        let v = s.get("Person", "vaswani").await.unwrap().unwrap();
        assert_eq!(v["id"], json!("vaswani"));
        assert_eq!(
            v["friends"],
            json!(["shazeer"]),
            "wikilink resolved to a key, case-insensitively"
        );
        assert_eq!(
            v["links"],
            json!(["shazeer"]),
            "unresolvable [[missing]] dropped"
        );
        assert!(v["body"].as_str().unwrap().starts_with("Worked with"));
        let sh = s.get("Person", "shazeer").await.unwrap().unwrap();
        assert_eq!(
            sh["links"],
            json!([]),
            "code, inline code, and embeds are not links"
        );

        let all = s.list("Person", &ListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 2);
        let linking = s
            .list(
                "Person",
                &ListQuery {
                    references: Some(("links".into(), "shazeer".into())),
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(linking.len(), 1, "backlinks via the reverse index");
        assert!(s.notes().unwrap().1.is_empty());
    }

    #[tokio::test]
    async fn type_field_and_frontmatter_keys_with_problems_reported() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                (
                    "notes/a.md",
                    "---\nid: 01AAA\ntype: Paper\ntitle: A\ncites: [\"[[b]]\"]\n---\n",
                ),
                ("notes/b.md", "---\nid: 01BBB\ntype: Paper\ntitle: B\n---\n"),
                ("notes/no-type.md", "---\nid: 01CCC\n---\n"),
                ("notes/no-id.md", "---\ntype: Paper\n---\n"),
                ("notes/dup.md", "---\nid: 01AAA\ntype: Paper\n---\n"),
                ("notes/bad.md", "---\ntitle: [unclosed\n---\n"),
                (
                    "notes/true.md",
                    "---\nid: 01DDD\ntype: Paper\ntitle: True\n---\n",
                ),
            ],
            "init",
        );
        let s = store(
            dir.path(),
            "type_field = \"type\"\nkey = \"frontmatter:id\"",
            &[("Paper", "id")],
        );
        let a = s.get("Paper", "01AAA").await.unwrap().unwrap();
        assert_eq!(
            a["cites"],
            json!(["01BBB"]),
            "wikilink by stem → frontmatter key"
        );
        let t = s.get("Paper", "01DDD").await.unwrap().unwrap();
        assert_eq!(
            t["title"],
            json!("True"),
            "same YAML reading as scriptorium pages"
        );
        let (_, problems) = s.notes().unwrap();
        let mut paths: Vec<&str> = problems.iter().map(|p| p.path.as_str()).collect();
        paths.sort_unstable();
        assert_eq!(
            paths,
            [
                "notes/bad.md",
                "notes/dup.md",
                "notes/no-id.md",
                "notes/no-type.md"
            ],
            "{problems:?}"
        );
    }

    #[tokio::test]
    async fn malformed_frontmatter_is_a_problem_not_an_empty_note() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("notes/twice.md", "---\ntitle: A\ntitle: B\n---\n"),
                (
                    "notes/nested.md",
                    "---\npages:\n  first: 1\n  first: 2\n---\n",
                ),
                ("notes/syntax.md", "---\ntitle: [unclosed\n---\n"),
                ("notes/open.md", "---\ntitle: Open\nno closing line\n"),
                ("notes/null.md", "---\n~\n---\nJust a body.\n"),
                ("notes/comment.md", "---\n# nothing yet\n---\n"),
            ],
            "init",
        );
        let s = store(dir.path(), "type = \"Topic\"", &[("Topic", "id")]);
        let (notes, problems) = s.notes().unwrap();
        let mut bad: Vec<&str> = problems.iter().map(|p| p.path.as_str()).collect();
        bad.sort_unstable();
        assert_eq!(
            bad,
            [
                "notes/nested.md",
                "notes/open.md",
                "notes/syntax.md",
                "notes/twice.md"
            ],
            "{problems:?}"
        );
        assert!(
            problems
                .iter()
                .all(|p| p.message.contains("not valid YAML")),
            "{problems:?}"
        );
        let mut empty: Vec<&str> = notes.iter().map(|n| n.key.as_str()).collect();
        empty.sort_unstable();
        assert_eq!(empty, ["comment", "null"]);
    }

    #[test]
    fn config_errors() {
        let snap: Arc<dyn Snapshot> = Arc::new(crate::snapshot::WorktreeSnapshot::new("/tmp"));
        for extra in [
            "",                                 // neither type nor type_field
            "type = \"A\"\ntype_field = \"t\"", // both
            "type = \"A\"\nkey = \"filename\"", // unknown key source
            "type = \"A\"\nkey = \"frontmatter:\"",
        ] {
            assert!(
                MarkdownStore::new(Arc::clone(&snap), "x", &config(extra), BTreeMap::new())
                    .is_err(),
                "{extra}"
            );
        }
        let escaping: MarkdownConfig =
            toml::from_str("dir = \"../outside\"\ntype = \"A\"").unwrap();
        assert!(MarkdownStore::new(snap, "x", &escaping, BTreeMap::new()).is_err());
    }

    #[test]
    fn wikilink_parsing() {
        assert_eq!(whole_wikilink("[[a|b]]"), Some("a"));
        assert_eq!(whole_wikilink(" [[a#h]] "), Some("a"));
        assert_eq!(whole_wikilink("see [[a]]"), None);
        assert_eq!(whole_wikilink("[[a]] and [[b]]"), None);
        assert_eq!(
            wikilink_targets("[[a]] x [[b|B]] `[[c]]` ![[d]] [[e#s]]"),
            ["a", "b", "e"]
        );
        assert_eq!(
            wikilink_targets("[[first]][[second]] ![[embed]][[after-embed]]"),
            ["first", "second", "after-embed"]
        );
    }
}
