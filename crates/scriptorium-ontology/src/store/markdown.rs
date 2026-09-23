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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use regex::Regex;

use super::{EntityStore, ListQuery, Record};
use crate::error::{Error, Result};
use crate::layout::{join, resolve_within};
use crate::manifest::MarkdownConfig;
use crate::registry::SubOntology;
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

/// A validated `[storage]` for markdown, independent of any snapshot.
struct Settings {
    sub: String,
    dir: String,
    types: TypeSource,
    key: KeySource,
    body_field: Option<String>,
    links_field: Option<String>,
    schema: NoteTypes,
}

/// What a markdown store needs to know of its sub-ontology's schema.
#[derive(Debug, Clone, Default)]
pub struct NoteTypes {
    /// Entity type → key field name, so every record carries its own key.
    pub key_fields: BTreeMap<String, String>,
    /// Entity type → the entity type its `links_field` lists: a body link
    /// to a note of another type is prose, not that relation.
    pub link_targets: BTreeMap<String, String>,
    /// Entity type → the fields its schema declares. Only these hold
    /// relations; other frontmatter (Obsidian's `aliases`, …) is ignored.
    pub fields: BTreeMap<String, BTreeSet<String>>,
}

impl NoteTypes {
    pub fn of(model: &crate::model::SubOntologyModel, config: &MarkdownConfig) -> Self {
        let key_fields = model
            .entities()
            .filter_map(|o| o.key_field().map(|k| (o.name.clone(), k.to_string())))
            .collect();
        let link_targets = config
            .links_field
            .as_ref()
            .map(|field| {
                model
                    .entities()
                    .filter_map(|o| {
                        let f = o.field(field)?;
                        matches!(
                            model.kind_of(&f.ty.named),
                            crate::model::TypeKind::Entity(_)
                        )
                        .then(|| (o.name.clone(), f.ty.named.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let fields = model
            .entities()
            .map(|o| {
                let names = o.fields.iter().map(|f| f.name.clone()).collect();
                (o.name.clone(), names)
            })
            .collect();
        Self {
            key_fields,
            link_targets,
            fields,
        }
    }
}

pub struct MarkdownStore {
    snapshot: Arc<dyn Snapshot>,
    settings: Settings,
    loaded: OnceLock<std::result::Result<Loaded, String>>,
    references: Mutex<ReferenceIndexes>,
}

/// Load every note of a markdown sub-ontology from `snapshot`, with the
/// files that did not load (what `check` reports).
pub fn load_notes(
    snapshot: &dyn Snapshot,
    sub: &SubOntology,
    config: &MarkdownConfig,
    schema: NoteTypes,
) -> Result<(Vec<Note>, Vec<Problem>)> {
    let settings = Settings::new(sub, config, schema)?;
    let loaded = settings.load(snapshot)?;
    Ok((loaded.notes.into_values().collect(), loaded.problems))
}

impl MarkdownStore {
    pub fn new(
        snapshot: Arc<dyn Snapshot>,
        sub: &SubOntology,
        config: &MarkdownConfig,
        schema: NoteTypes,
    ) -> Result<Self> {
        Ok(Self {
            snapshot,
            settings: Settings::new(sub, config, schema)?,
            loaded: OnceLock::new(),
            references: Mutex::new(HashMap::new()),
        })
    }

    fn loaded(&self) -> Result<&Loaded> {
        self.loaded
            .get_or_init(|| {
                self.settings
                    .load(self.snapshot.as_ref())
                    .map_err(|e| e.to_string())
            })
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

impl Settings {
    fn new(sub: &SubOntology, config: &MarkdownConfig, schema: NoteTypes) -> Result<Self> {
        let name = sub.name.as_str();
        let invalid = |message: &str| Error::Store {
            store: format!("markdown:{name}"),
            message: message.to_string(),
        };
        let types = match (&config.type_name, &config.type_field) {
            // Checked here, not only per note: with no notes yet, nothing
            // else would notice, and the entity would serve as empty.
            (Some(name), None) if !schema.key_fields.contains_key(name) => {
                return Err(invalid(&format!(
                    "`type = \"{name}\"` is not an entity with a single-field @key in this sub-ontology"
                )));
            }
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
        let Some(dir) = resolve_within(&sub.dir, &config.dir) else {
            return Err(invalid("`dir` must stay inside the repository"));
        };
        Ok(Self {
            sub: name.to_string(),
            dir,
            types,
            key,
            body_field: config.body_field.clone(),
            links_field: config.links_field.clone(),
            schema,
        })
    }

    fn load(&self, snapshot: &dyn Snapshot) -> Result<Loaded> {
        // Notes behind a symlink would be missing, not wrong: refuse it.
        if let Some(problem) = crate::snapshot::not_a_directory(snapshot, "", &self.dir, "notes")? {
            return Err(Error::Invalid {
                what: "markdown notes directory",
                path: self.dir.clone(),
                message: problem,
            });
        }
        let mut files = Vec::new();
        let mut loaded = Loaded::default();
        Self::walk(snapshot, &self.dir, &mut files, &mut loaded.problems)?;

        // Pass 1: frontmatter, type, key.
        let mut parsed = Vec::new();
        for path in files {
            match self.parse_file(snapshot, &path) {
                Ok(note) => parsed.push(note),
                Err(message) => loaded.problems.push(Problem { path, message }),
            }
        }

        // Stems resolve `[[wikilinks]]` to keys. A stem shared by two notes
        // is ambiguous: a body link to it resolves to nothing, and a
        // frontmatter relation to it is a problem.
        let mut stems: Stems = HashMap::new();
        for note in &parsed {
            stems
                .entry(note.stem.to_lowercase())
                .and_modify(|note| *note = None)
                .or_insert_with(|| Some((note.key.clone(), note.type_name.clone())));
        }

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
            // Undeclared frontmatter is ignored, as `check` ignores it, even
            // when it names an ambiguous stem.
            let declared = self.schema.fields.get(&type_name);
            let mut ambiguous = Vec::new();
            for (field, value) in &mut record {
                if declared.is_some_and(|fields| fields.contains(field)) {
                    resolve_wikilink_values(value, &stems, &mut ambiguous);
                }
            }
            if !ambiguous.is_empty() {
                loaded.problems.push(Problem {
                    path,
                    message: format!(
                        "[[{}]] names more than one note (by file name); rename one, or link by a unique name",
                        ambiguous.join("]], [[")
                    ),
                });
                continue;
            }
            if let Some(field) = &self.links_field {
                let wanted = self.schema.link_targets.get(&type_name);
                let mut links: Vec<serde_json::Value> = Vec::new();
                for target in wikilink_targets(&body) {
                    let Some(Some((key, linked))) = stems.get(&target.to_lowercase()) else {
                        continue;
                    };
                    if wanted.is_some_and(|wanted| wanted != linked) {
                        continue;
                    }
                    let key = serde_json::Value::String(key.clone());
                    if !links.contains(&key) {
                        links.push(key);
                    }
                }
                record.insert(field.clone(), serde_json::Value::Array(links));
            }
            if let Some(field) = &self.body_field {
                record.insert(field.clone(), serde_json::Value::String(body));
            }
            if let Some(key_field) = self.schema.key_fields.get(&type_name) {
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

    fn walk(
        snapshot: &dyn Snapshot,
        dir: &str,
        out: &mut Vec<String>,
        problems: &mut Vec<Problem>,
    ) -> Result<()> {
        for entry in snapshot.list(dir)? {
            // Obsidian keeps its own state in dot-directories.
            if entry.name.starts_with('.') {
                continue;
            }
            let path = join(dir, &entry.name);
            if entry.is_symlink {
                // Git stores the link, not what it names, so a linked
                // directory's notes would be missing without a word.
                problems.push(Problem {
                    path,
                    message: "is a symlink; git reads do not follow it, so what it links to is not read; replace it with the note or directory".into(),
                });
            } else if entry.is_dir {
                Self::walk(snapshot, &path, out, problems)?;
            } else if is_markdown(&entry.name) {
                out.push(path);
            }
        }
        Ok(())
    }

    /// `Err` is a problem with this file alone.
    fn parse_file(
        &self,
        snapshot: &dyn Snapshot,
        path: &str,
    ) -> std::result::Result<ParsedNote, String> {
        // One unreadable note is its own problem, not every note's.
        let text = match snapshot.read(path) {
            Ok(Some(bytes)) => match String::from_utf8(bytes) {
                Ok(text) => text,
                Err(_) => return Err("is not valid UTF-8".into()),
            },
            Ok(None) => return Err("file disappeared while loading".into()),
            Err(e) => return Err(e.to_string()),
        };
        let stem = std::path::Path::new(path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_string();
        let (record, body) = split_frontmatter(&text)?;
        let type_name = match &self.types {
            TypeSource::Fixed(name) => name.clone(),
            TypeSource::Field(field) => match record.get(field).and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => return Err(format!("no `{field}` field naming the entity type")),
            },
        };
        let key = match &self.key {
            KeySource::Stem => stem.clone(),
            KeySource::Frontmatter(field) => match record.get(field) {
                Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
                Some(serde_json::Value::Number(n)) => n.to_string(),
                _ => return Err(format!("no `{field}` field to use as the key")),
            },
        };
        if let Some(problem) = self.overridden_field(&type_name, &key, &record) {
            return Err(problem);
        }
        Ok(ParsedNote {
            path: path.to_string(),
            stem,
            type_name,
            key,
            record,
            body,
        })
    }
}

impl Settings {
    /// The store fills the key, body, and links fields itself: a note whose
    /// frontmatter states another value for one would be served with a value
    /// it does not say.
    fn overridden_field(&self, type_name: &str, key: &str, record: &Record) -> Option<String> {
        if let Some(key_field) = self.schema.key_fields.get(type_name) {
            if let Some(stated) = record.get(key_field) {
                let agrees = match stated {
                    serde_json::Value::String(s) => s == key,
                    serde_json::Value::Number(n) => n.to_string() == key,
                    _ => false,
                };
                if !agrees {
                    let from = match &self.key {
                        KeySource::Stem => "its file name".to_string(),
                        KeySource::Frontmatter(field) => format!("`{field}`"),
                    };
                    return Some(format!(
                        "`{key_field}: {stated}` disagrees with the note's key `{key}` (from {from}); remove `{key_field}` or make it `{key}`"
                    ));
                }
            }
        }
        let filled = [
            (&self.body_field, "the note's body"),
            (&self.links_field, "the notes its body links to"),
        ];
        filled.into_iter().find_map(|(field, with)| {
            let field = field.as_ref().filter(|f| record.contains_key(f.as_str()))?;
            Some(format!(
                "the frontmatter sets `{field}`, which this store fills with {with}; rename it, or configure another field in subgraph.toml"
            ))
        })
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
        format!("markdown:{}@{}", self.settings.dir, self.snapshot.label())
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
                sub_ontology = %self.settings.sub,
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

/// Replace each `[[wikilink]]` value with the key of the note it names by
/// stem. A link naming no note keeps its target as the key (another
/// sub-ontology's entity, or a dangling reference `check` reports); one
/// naming several notes is collected in `ambiguous`.
/// Lowercased file stem → the one note with it (key and type), or `None`
/// when several notes share it.
type Stems = HashMap<String, Option<(String, String)>>;

fn resolve_wikilink_values(
    value: &mut serde_json::Value,
    stems: &Stems,
    ambiguous: &mut Vec<String>,
) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(target) = whole_wikilink(s) {
                match stems.get(&target.to_lowercase()) {
                    Some(Some((key, _))) => s.clone_from(key),
                    Some(None) => ambiguous.push(target.to_string()),
                    None => *s = target.to_string(),
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                resolve_wikilink_values(item, stems, ambiguous);
            }
        }
        _ => {}
    }
}

/// `[[wikilink]]` targets in a markdown body, in order. The body is read as
/// `CommonMark`, as the vault reads its pages: links in code (spans, which
/// may run across lines, and fenced or indented blocks) and in raw HTML do
/// not count, nor do `![[embeds]]`.
fn wikilink_targets(body: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut at = 0;
    let end = std::iter::once(body.len()..body.len());
    for skipped in code_and_html(body).into_iter().chain(end) {
        let text = &body[at..skipped.start.max(at)];
        for capture in wikilink_regex().captures_iter(text) {
            let start = capture.get(0).map_or(0, |m| m.start());
            if text[..start].ends_with('!') {
                continue;
            }
            targets.push(capture[1].trim().to_string());
        }
        at = at.max(skipped.end);
    }
    targets
}

/// The byte ranges of `body` that are code or raw HTML, in order and
/// merged. `pulldown-cmark` splits `[[a]]` into brackets and a reference
/// link, so its text events cannot be searched for links themselves; only
/// the source between these ranges is.
fn code_and_html(body: &str) -> Vec<std::ops::Range<usize>> {
    use pulldown_cmark::{Event, Parser, Tag, TagEnd};
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut block = None;
    for (event, range) in Parser::new(body).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(_)) => block = Some(range.start),
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = block.take() {
                    ranges.push(start..range.end);
                }
            }
            Event::Code(_) | Event::Html(_) | Event::InlineHtml(_) => ranges.push(range),
            _ => {}
        }
    }
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<std::ops::Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};
    use serde_json::json;

    fn config(extra: &str) -> MarkdownConfig {
        toml::from_str(&format!("dir = \"../../notes\"\n{extra}")).unwrap()
    }

    /// A sub-ontology at `ontology/people` whose notes live in `notes/`.
    fn sub() -> SubOntology {
        SubOntology {
            name: "people".into(),
            dir: "ontology/people".into(),
            description: None,
            owners: vec![],
            sdl: String::new(),
            source: crate::registry::SubOntologySource::Local(
                crate::manifest::StorageConfig::Git {
                    data_dir: "data".into(),
                },
            ),
        }
    }

    /// A store over `dir` at HEAD for `types`: each entity type, its key
    /// field, and the fields its schema declares.
    fn store(dir: &std::path::Path, extra: &str, types: &[(&str, &str, &[&str])]) -> MarkdownStore {
        MarkdownStore::new(
            Arc::new(GitSnapshot::open(dir, "HEAD").unwrap()),
            &sub(),
            &config(extra),
            NoteTypes {
                key_fields: types
                    .iter()
                    .map(|(t, k, _)| ((*t).to_string(), (*k).to_string()))
                    .collect(),
                link_targets: BTreeMap::new(),
                fields: types
                    .iter()
                    .map(|(t, _, fields)| {
                        let names = fields.iter().map(ToString::to_string).collect();
                        ((*t).to_string(), names)
                    })
                    .collect(),
            },
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
            &[("Person", "id", &["name", "friends", "body", "links"])],
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
            &[("Paper", "id", &["title", "cites"])],
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
    async fn frontmatter_cannot_contradict_what_the_store_fills() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("notes/foo.md", "---\nid: bar\n---\n"),
                ("notes/same.md", "---\nid: same\n---\n"),
                ("notes/body.md", "---\nbody: stated\n---\nThe body.\n"),
                ("notes/links.md", "---\nlinks: [same]\n---\n"),
                ("notes/plain.md", "---\ntitle: Plain\n---\n"),
            ],
            "init",
        );
        let s = store(
            dir.path(),
            "type = \"Topic\"\nbody_field = \"body\"\nlinks_field = \"links\"",
            &[("Topic", "id", &["title", "body", "links"])],
        );
        let (notes, problems) = s.notes().unwrap();
        let mut bad: Vec<(&str, &str)> = problems
            .iter()
            .map(|p| (p.path.as_str(), p.message.as_str()))
            .collect();
        bad.sort_unstable();
        assert_eq!(bad.len(), 3, "{problems:?}");
        assert_eq!(bad[0].0, "notes/body.md");
        assert!(bad[0].1.contains("sets `body`"), "{}", bad[0].1);
        assert_eq!(bad[1].0, "notes/foo.md");
        assert!(
            bad[1]
                .1
                .contains("`id: \"bar\"` disagrees with the note's key `foo`"),
            "{}",
            bad[1].1
        );
        assert_eq!(bad[2].0, "notes/links.md");
        assert!(s.get("Topic", "foo").await.unwrap().is_none(), "not served");
        let mut served: Vec<&str> = notes.iter().map(|n| n.key.as_str()).collect();
        served.sort_unstable();
        assert_eq!(served, ["plain", "same"]);
    }

    #[tokio::test]
    async fn only_declared_fields_hold_relations() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("notes/a/foo.md", "---\nid: A\n---\n"),
                ("notes/b/foo.md", "---\nid: B\n---\n"),
                (
                    "notes/x.md",
                    "---\nid: X\naliases: [\"[[foo]]\"]\nrel: [\"[[y]]\"]\n---\n",
                ),
                ("notes/y.md", "---\nid: Y\n---\n"),
            ],
            "init",
        );
        let s = store(
            dir.path(),
            "type = \"Topic\"\nkey = \"frontmatter:id\"",
            &[("Topic", "id", &["rel"])],
        );
        // An ambiguous stem in metadata the schema never reads is not a
        // problem, and is left as it was.
        let x = s.get("Topic", "X").await.unwrap().expect("X loads");
        assert_eq!(x["rel"], json!(["Y"]));
        assert_eq!(x["aliases"], json!(["[[foo]]"]));
        let (_, problems) = s.notes().unwrap();
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[tokio::test]
    async fn ambiguous_frontmatter_links_are_problems_not_guesses() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("notes/a/foo.md", "---\nid: A\n---\n"),
                ("notes/b/foo.md", "---\nid: B\n---\n"),
                // Its key happens to be the ambiguous stem.
                ("notes/bar.md", "---\nid: foo\n---\n"),
                ("notes/x.md", "---\nid: X\nrel: [\"[[foo]]\"]\n---\n"),
                (
                    "notes/y.md",
                    "---\nid: Y\nrel: [\"[[bar]]\", \"[[elsewhere]]\"]\n---\n",
                ),
            ],
            "init",
        );
        let s = store(
            dir.path(),
            "type = \"Topic\"\nkey = \"frontmatter:id\"",
            &[("Topic", "id", &["rel"])],
        );
        let (_, problems) = s.notes().unwrap();
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert_eq!(problems[0].path, "notes/x.md");
        assert!(
            problems[0]
                .message
                .contains("[[foo]] names more than one note"),
            "{}",
            problems[0].message
        );
        assert!(s.get("Topic", "X").await.unwrap().is_none(), "not served");
        // A unique stem resolves to its key; an unknown one stays as written.
        let y = s.get("Topic", "Y").await.unwrap().unwrap();
        assert_eq!(y["rel"], json!(["foo", "elsewhere"]));
    }

    #[tokio::test]
    async fn body_links_keep_to_the_type_their_relation_lists() {
        const SDL: &str = r#"
type Topic @key(fields: "id") { id: ID! related: [Topic!]! }
type Person @key(fields: "id") { id: ID! }
type Query { topic(id: ID!): Topic }
"#;
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                (
                    "notes/topic.md",
                    "---\ntype: Topic\n---\nSee [[person]] and [[other]].\n",
                ),
                ("notes/other.md", "---\ntype: Topic\n---\n"),
                ("notes/person.md", "---\ntype: Person\n---\n"),
            ],
            "init",
        );
        let config = config("type_field = \"type\"\nlinks_field = \"related\"");
        let model = crate::model::SubOntologyModel::parse(SDL).unwrap();
        let schema = NoteTypes::of(&model, &config);
        assert_eq!(
            schema.link_targets,
            BTreeMap::from([("Topic".to_string(), "Topic".to_string())])
        );
        let s = MarkdownStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            &sub(),
            &config,
            schema,
        )
        .unwrap();
        let topic = s.get("Topic", "topic").await.unwrap().unwrap();
        assert_eq!(topic["related"], json!(["other"]), "not the Person note");
    }

    #[tokio::test]
    async fn an_unreadable_note_is_its_own_problem() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[("notes/fine.md", "---\ntitle: Fine\n---\n")],
            "init",
        );
        // A note that is not UTF-8, committed next to a fine one.
        let repo = git2::Repository::open(dir.path()).unwrap();
        std::fs::write(
            dir.path().join("notes/latin1.md"),
            b"---\ntitle: caf\xe9\n---\n",
        )
        .unwrap();
        let mut index = repo.index().unwrap();
        index
            .add_path(std::path::Path::new("notes/latin1.md"))
            .unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "latin1", &tree, &[&parent])
            .unwrap();
        let s = store(dir.path(), "type = \"Topic\"", &[("Topic", "id", &[])]);
        let (notes, problems) = s.notes().unwrap();
        assert_eq!(notes.len(), 1, "the fine note still loads");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert_eq!(problems[0].path, "notes/latin1.md");
        assert!(
            problems[0].message.contains("UTF-8"),
            "{}",
            problems[0].message
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
        let s = store(dir.path(), "type = \"Topic\"", &[("Topic", "id", &[])]);
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

    /// A schema with one entity type, `A`, keyed by `id`.
    fn types_a() -> NoteTypes {
        NoteTypes {
            key_fields: BTreeMap::from([("A".to_string(), "id".to_string())]),
            link_targets: BTreeMap::new(),
            fields: BTreeMap::new(),
        }
    }

    #[test]
    fn config_errors() {
        let snap: Arc<dyn Snapshot> = Arc::new(crate::snapshot::WorktreeSnapshot::new("/tmp"));
        let new = |config: &MarkdownConfig| {
            MarkdownStore::new(Arc::clone(&snap), &sub(), config, types_a()).map(|_| ())
        };
        assert!(new(&config("type = \"A\"")).is_ok());
        for extra in [
            "",                                 // neither type nor type_field
            "type = \"A\"\ntype_field = \"t\"", // both
            "type = \"A\"\nkey = \"filename\"", // unknown key source
            "type = \"A\"\nkey = \"frontmatter:\"",
        ] {
            assert!(new(&config(extra)).is_err(), "{extra}");
        }
        let escaping: MarkdownConfig =
            toml::from_str("dir = \"../../../outside\"\ntype = \"A\"").unwrap();
        assert!(new(&escaping).is_err());
        // A fixed type the schema lacks would serve as empty, notes or not.
        let err = new(&config("type = \"B\"")).unwrap_err().to_string();
        assert!(
            err.contains("`type = \"B\"` is not an entity with a single-field @key"),
            "{err}"
        );
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
        // A fence closes only on its own kind, at least as long.
        assert_eq!(
            wikilink_targets(
                "````\n```\n[[inside]]\n````\n[[after]]\n~~~\n[[tilde]]\n```\n[[still]]\n~~~\n"
            ),
            ["after"]
        );
        // Code spans are delimited by runs of equal length.
        assert_eq!(
            wikilink_targets("``[[a]]`` `[[b]]` ``x ` [[c]]`` [[d]] `[[e]]"),
            ["d", "e"]
        );
        // A code span runs across the lines of its paragraph, not past it.
        assert_eq!(
            wikilink_targets("a `code\n[[in-span]]` b [[after]]\n\n`open\n\n[[next]] `"),
            ["after", "next"]
        );
        // Indented code blocks and raw HTML are not prose either.
        assert_eq!(
            wikilink_targets("text\n\n    [[indented]]\n\n<div>\n[[html]]\n</div>\n\n[[prose]]"),
            ["prose"]
        );
    }

    /// Commit a symlink at `link` naming `target`: git stores the link.
    #[cfg(unix)]
    fn commit_link(root: &std::path::Path, link: &str, target: &str) {
        std::os::unix::fs::symlink(target, root.join(link)).unwrap();
        let repo = git2::Repository::open(root).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new(link)).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "link", &tree, &[&parent])
            .unwrap();
    }

    /// `root` at HEAD, and as its working tree.
    #[cfg(unix)]
    fn views(root: &std::path::Path) -> [Arc<dyn Snapshot>; 2] {
        [
            Arc::new(GitSnapshot::open(root, "HEAD").unwrap()),
            Arc::new(crate::snapshot::WorktreeSnapshot::new(root)),
        ]
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_among_the_notes_are_problems_not_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(
            root,
            &[("notes/a.md", "# A\n"), ("elsewhere/b.md", "# B\n")],
            "init",
        );
        commit_link(root, "notes/linked", "../elsewhere");
        commit_link(root, "notes/alias.md", "a.md");
        for view in views(root) {
            let s = MarkdownStore::new(view, &sub(), &config("type = \"A\""), types_a()).unwrap();
            let (notes, problems) = s.notes().unwrap();
            let keys: Vec<&str> = notes.iter().map(|n| n.key.as_str()).collect();
            assert_eq!(keys, ["a"]);
            let mut linked: Vec<&str> = problems
                .iter()
                .filter(|p| p.message.contains("symlink"))
                .map(|p| p.path.as_str())
                .collect();
            linked.sort_unstable();
            assert_eq!(linked, ["notes/alias.md", "notes/linked"], "{problems:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_notes_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(root, &[("real/a.md", "# A\n")], "init");
        commit_link(root, "notes", "real");
        for view in views(root) {
            let s = MarkdownStore::new(view, &sub(), &config("type = \"A\""), types_a()).unwrap();
            let err = s.notes().unwrap_err().to_string();
            assert!(err.contains("`notes` is a symlink"), "{err}");
        }
    }
}
