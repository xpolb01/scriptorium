//! `Page` — a single markdown file in the vault, with structured frontmatter.
//!
//! Every page in `wiki/` carries YAML frontmatter that the rest of the engine
//! treats as the canonical metadata. The most important field is `id`: a ULID
//! that is **the merge key**, not the filename. Pages can be renamed without
//! breaking links, embeddings, or patches because everything keys off the ID.
//!
//! Unknown frontmatter fields are preserved through round-trips so users can
//! mix Scriptorium-managed pages with their own Obsidian frontmatter
//! conventions (`cssclass`, `publish`, plugin-specific keys, etc.).

use std::collections::BTreeMap;
use std::path::Path;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use gray_matter::{engine::YAML, Matter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::error::{Error, Result};

/// Current schema version. Bump when frontmatter shape changes in a way that
/// requires migration.
pub const SCHEMA_VERSION: u32 = 1;

/// Stable identifier for a page. Survives renames; used as the merge key for
/// LLM patches and embeddings rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PageId(Ulid);

impl PageId {
    /// Generate a fresh, monotonic-time-ordered ID.
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Construct from a Crockford-base32 string (the ULID textual form).
    pub fn parse(s: &str) -> std::result::Result<Self, ulid::DecodeError> {
        Ulid::from_string(s).map(Self)
    }

    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for PageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PageId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Structured frontmatter for a wiki page.
///
/// Unknown fields are preserved in [`Self::extra`] so round-tripping a page
/// through `parse → serialize` is lossless even if the user has added their
/// own keys via Obsidian.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frontmatter {
    pub id: PageId,
    pub title: String,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Unknown / user-defined fields. Preserved on round-trip in a stable
    /// alphabetic order so diffs stay clean.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yml::Value>,
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

impl Frontmatter {
    /// Build a fresh frontmatter for a brand-new page. `created` and `updated`
    /// are both set to `now`.
    pub fn new(title: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: PageId::new(),
            title: title.into(),
            created: now,
            updated: now,
            sources: Vec::new(),
            tags: Vec::new(),
            aliases: Vec::new(),
            schema_version: SCHEMA_VERSION,
            extra: BTreeMap::new(),
        }
    }
}

/// A markdown page parsed from disk: location, frontmatter, body.
///
/// The `path` is **relative to the vault root** (e.g. `wiki/concepts/foo.md`).
/// Constructing a `Page` does not touch disk; use [`Page::read`] for that.
#[derive(Debug, Clone)]
pub struct Page {
    pub path: Utf8PathBuf,
    pub frontmatter: Frontmatter,
    pub body: String,
}

impl Page {
    /// Parse a page from in-memory markdown text. Errors if the frontmatter is
    /// missing, malformed, or fails to deserialize into [`Frontmatter`].
    ///
    /// The returned page's [`body`](Self::body) is normalized to the canonical
    /// form: no leading newlines, exactly one trailing newline (or empty if
    /// the body has no content). This makes the round-trip
    /// `parse(to_markdown(p)) == p` hold for any normalized page.
    pub fn parse(path: impl Into<Utf8PathBuf>, input: &str) -> Result<Self> {
        let path = path.into();
        let matter = Matter::<YAML>::new();
        // First pass: detect "no frontmatter at all" so we can produce a more
        // specific error than "schema mismatch".
        let raw = matter.parse(input);
        if raw.data.is_none() {
            return Err(Error::Frontmatter {
                path: path.into_std_path_buf(),
                message: "missing or malformed YAML frontmatter delimiters".into(),
            });
        }
        let parsed = matter
            .parse_with_struct::<Frontmatter>(input)
            .ok_or_else(|| Error::Frontmatter {
                path: path.clone().into_std_path_buf(),
                message: "frontmatter does not match required schema \
                          (id, title, created, updated)"
                    .into(),
            })?;
        Ok(Self {
            path,
            frontmatter: parsed.data,
            body: normalize_body(&parsed.content),
        })
    }

    /// Read and parse a page from disk. `path` is the absolute (or
    /// process-relative) location of the file; the resulting [`Page::path`] is
    /// computed relative to `vault_root`.
    pub fn read(vault_root: &Utf8Path, path: &Path) -> Result<Self> {
        let bytes = std::fs::read_to_string(path).map_err(|e| Error::io(path.to_path_buf(), e))?;
        let utf8 = Utf8Path::from_path(path).ok_or_else(|| Error::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 path"),
        })?;
        let rel = utf8
            .strip_prefix(vault_root)
            .map_err(|_| Error::PathEscape(path.to_path_buf()))?;
        Self::parse(rel.to_owned(), &bytes)
    }

    /// Render the page back to a string with `---`-delimited YAML frontmatter
    /// followed by the body. Round-trip property: `Page::parse(p.to_markdown())
    /// == p` for any normalized page.
    ///
    /// The body is normalized on the way out (see [`Self::parse`] for the
    /// canonical form), so calling `to_markdown` on a manually-constructed
    /// `Page` with non-canonical body whitespace produces the same output as
    /// the normalized equivalent.
    pub fn to_markdown(&self) -> Result<String> {
        let yaml = serde_yml::to_string(&self.frontmatter).map_err(|e| Error::Frontmatter {
            path: self.path.clone().into_std_path_buf(),
            message: format!("yaml serialize: {e}"),
        })?;
        let body = normalize_body(&self.body);
        Ok(format!("---\n{yaml}---\n{body}"))
    }

    /// Hex-encoded SHA-256 of the full rendered markdown (frontmatter + body).
    /// Used as the cache key for embeddings and as the stale-page detector for
    /// LLM patches.
    pub fn content_hash(&self) -> Result<String> {
        let rendered = self.to_markdown()?;
        let mut hasher = Sha256::new();
        hasher.update(rendered.as_bytes());
        Ok(hex(&hasher.finalize()))
    }
}

/// Canonical body form: no leading newlines, exactly one trailing newline if
/// non-empty. Used by both [`Page::parse`] and [`Page::to_markdown`] so the
/// round-trip is stable regardless of how the page was originally written.
fn normalize_body(body: &str) -> String {
    let trimmed = body.trim_matches('\n');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}\n")
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use pretty_assertions::assert_eq;

    fn fixed_frontmatter() -> Frontmatter {
        Frontmatter {
            id: PageId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            title: "Test Page".into(),
            created: Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap(),
            updated: Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap(),
            sources: vec!["sources/articles/foo.md".into()],
            tags: vec!["concept".into(), "draft".into()],
            aliases: vec!["alt".into()],
            schema_version: SCHEMA_VERSION,
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn parses_minimal_page() {
        let input = r"---
id: 01ARZ3NDEKTSV4RRFFQ69G5FAV
title: Hello
created: 2026-04-06T12:00:00Z
updated: 2026-04-06T12:00:00Z
---
# Hello

Body text.
";
        let page = Page::parse("wiki/hello.md", input).unwrap();
        assert_eq!(page.frontmatter.title, "Hello");
        assert_eq!(page.frontmatter.schema_version, SCHEMA_VERSION);
        assert!(page.body.contains("Body text"));
    }

    #[test]
    fn rejects_missing_frontmatter() {
        let input = "# No frontmatter\n\nJust a body.\n";
        let err = Page::parse("wiki/x.md", input).unwrap_err();
        match err {
            Error::Frontmatter { .. } => {}
            other => panic!("expected Frontmatter, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_preserves_known_fields() {
        let page = Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: fixed_frontmatter(),
            body: "# Heading\n\nSome body.\n".into(),
        };
        let rendered = page.to_markdown().unwrap();
        let reparsed = Page::parse(page.path.clone(), &rendered).unwrap();
        assert_eq!(reparsed.frontmatter.id, page.frontmatter.id);
        assert_eq!(reparsed.frontmatter.title, page.frontmatter.title);
        assert_eq!(reparsed.frontmatter.created, page.frontmatter.created);
        assert_eq!(reparsed.frontmatter.updated, page.frontmatter.updated);
        assert_eq!(reparsed.frontmatter.sources, page.frontmatter.sources);
        assert_eq!(reparsed.frontmatter.tags, page.frontmatter.tags);
        assert_eq!(reparsed.frontmatter.aliases, page.frontmatter.aliases);
        assert_eq!(
            reparsed.frontmatter.schema_version,
            page.frontmatter.schema_version
        );
        assert_eq!(reparsed.body, page.body);
    }

    #[test]
    fn round_trip_preserves_unknown_extra_fields() {
        let mut fm = fixed_frontmatter();
        fm.extra
            .insert("publish".into(), serde_yml::Value::Bool(true));
        fm.extra
            .insert("cssclass".into(), serde_yml::Value::String("dark".into()));
        let page = Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: fm,
            body: "body\n".into(),
        };
        let rendered = page.to_markdown().unwrap();
        let reparsed = Page::parse(page.path.clone(), &rendered).unwrap();
        assert_eq!(
            reparsed.frontmatter.extra.get("publish"),
            Some(&serde_yml::Value::Bool(true))
        );
        assert_eq!(
            reparsed.frontmatter.extra.get("cssclass"),
            Some(&serde_yml::Value::String("dark".into()))
        );
    }

    #[test]
    fn content_hash_is_stable() {
        let page = Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: fixed_frontmatter(),
            body: "body\n".into(),
        };
        let h1 = page.content_hash().unwrap();
        let h2 = page.content_hash().unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn content_hash_changes_when_body_changes() {
        let mut page = Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: fixed_frontmatter(),
            body: "body one\n".into(),
        };
        let h1 = page.content_hash().unwrap();
        page.body = "body two\n".into();
        let h2 = page.content_hash().unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn page_id_round_trips_through_string() {
        let id = PageId::new();
        let s = id.to_string();
        let parsed = PageId::parse(&s).unwrap();
        assert_eq!(id, parsed);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_frontmatter() -> impl Strategy<Value = Frontmatter> {
        (
            "[A-Za-z0-9 ]{1,40}",
            prop::collection::vec("[a-z]{1,8}", 0..4),
            prop::collection::vec("[a-z][a-z0-9-]{0,12}", 0..4),
            0i64..1_000_000_000,
        )
            .prop_map(|(title, tags, sources, secs)| {
                let created = chrono::DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
                Frontmatter {
                    id: PageId::new(),
                    title,
                    created,
                    updated: created,
                    sources,
                    tags,
                    aliases: vec![],
                    schema_version: SCHEMA_VERSION,
                    extra: BTreeMap::new(),
                }
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn parse_render_roundtrip(fm in arb_frontmatter(), body in "[a-zA-Z0-9 \n]{0,200}") {
            let page = Page {
                path: Utf8PathBuf::from("wiki/p.md"),
                frontmatter: fm,
                body,
            };
            let rendered = page.to_markdown().unwrap();
            let reparsed = Page::parse(page.path.clone(), &rendered).unwrap();
            prop_assert_eq!(reparsed.frontmatter.id, page.frontmatter.id);
            prop_assert_eq!(reparsed.frontmatter.title, page.frontmatter.title);
            prop_assert_eq!(reparsed.frontmatter.tags, page.frontmatter.tags);
            prop_assert_eq!(reparsed.frontmatter.sources, page.frontmatter.sources);
        }
    }
}
