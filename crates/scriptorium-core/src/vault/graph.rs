//! Link graph: forward edges (page → links it contains) and backlinks
//! (page → pages that reference it).
//!
//! Edges are keyed by [`PageId`] (the stable ULID), so the graph is robust to
//! file renames as long as the link target string still matches the new file
//! stem. Resolution rules in v1 are intentionally simple:
//!
//! - `[[foo]]` matches the page whose **filename stem** is `foo`
//! - Multiple matches → [`Resolution::Ambiguous`]
//! - No match → [`Resolution::Broken`]
//! - `[[#Heading]]` within a page → [`Resolution::SelfHeading`]
//!
//! Title-based resolution and path-prefixed disambiguation
//! (`[[concepts/foo]]`) are deferred — the lint phase surfaces ambiguity so
//! users can switch to ULID-keyed link rewriting in a later phase.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use super::page::{Page, PageId};
use super::wikilink::{self, Wikilink};

/// Lightweight per-page metadata held in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageInfo {
    pub id: PageId,
    pub title: String,
    pub path: Utf8PathBuf,
    /// Filename stem (e.g. `foo` from `wiki/concepts/foo.md`).
    pub stem: String,
}

/// One forward link from a page, with its resolution status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkRef {
    pub link: Wikilink,
    pub resolved: Resolution,
}

/// What a wikilink target resolves to in the current vault.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Resolution {
    /// Exactly one page matched the target stem.
    Resolved(PageId),
    /// No page matched the target stem.
    Broken,
    /// More than one page has the same filename stem; the link is ambiguous.
    Ambiguous(Vec<PageId>),
    /// `[[#Heading]]` — refers to a heading within the same page.
    SelfHeading,
}

impl Resolution {
    pub fn is_resolved(&self) -> bool {
        matches!(self, Self::Resolved(_))
    }

    pub fn is_broken(&self) -> bool {
        matches!(self, Self::Broken)
    }
}

/// The full link graph for a vault scan.
///
/// Cheap to build — `O(N + L)` where `N` is the page count and `L` is the
/// total link count — and read-only thereafter. Rebuild after any change.
#[derive(Debug, Clone, Default)]
pub struct LinkGraph {
    pages: BTreeMap<PageId, PageInfo>,
    by_stem: HashMap<String, Vec<PageId>>,
    forward: HashMap<PageId, Vec<LinkRef>>,
    backlinks: HashMap<PageId, BTreeSet<PageId>>,
}

impl LinkGraph {
    /// Build a graph by scanning every page's body for wikilinks.
    pub fn build(pages: &[Page]) -> Self {
        let mut info_map: BTreeMap<PageId, PageInfo> = BTreeMap::new();
        let mut by_stem: HashMap<String, Vec<PageId>> = HashMap::new();
        for page in pages {
            let stem = page.path.file_stem().unwrap_or_default().to_string();
            let id = page.frontmatter.id;
            by_stem.entry(stem.clone()).or_default().push(id);
            info_map.insert(
                id,
                PageInfo {
                    id,
                    title: page.frontmatter.title.clone(),
                    path: page.path.clone(),
                    stem,
                },
            );
        }
        // Sort each stem bucket by ID so resolution is deterministic.
        for ids in by_stem.values_mut() {
            ids.sort();
        }

        let mut forward: HashMap<PageId, Vec<LinkRef>> = HashMap::new();
        let mut backlinks: HashMap<PageId, BTreeSet<PageId>> = HashMap::new();
        for page in pages {
            let id = page.frontmatter.id;
            let links = wikilink::parse_markdown(&page.body);
            let mut refs = Vec::with_capacity(links.len());
            for link in links {
                let resolved = if link.is_self_link() {
                    Resolution::SelfHeading
                } else {
                    match by_stem.get(&link.target).map(Vec::as_slice) {
                        None | Some([]) => Resolution::Broken,
                        Some([single]) => Resolution::Resolved(*single),
                        Some(many) => Resolution::Ambiguous(many.to_vec()),
                    }
                };
                if let Resolution::Resolved(target_id) = &resolved {
                    if *target_id != id {
                        backlinks.entry(*target_id).or_default().insert(id);
                    }
                }
                refs.push(LinkRef { link, resolved });
            }
            forward.insert(id, refs);
        }

        Self {
            pages: info_map,
            by_stem,
            forward,
            backlinks,
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    pub fn pages(&self) -> impl Iterator<Item = &PageInfo> {
        self.pages.values()
    }

    pub fn page_info(&self, id: PageId) -> Option<&PageInfo> {
        self.pages.get(&id)
    }

    /// Forward links from a page in document order. Returns `None` if the
    /// page is not in this graph.
    pub fn forward(&self, id: PageId) -> Option<&[LinkRef]> {
        self.forward.get(&id).map(Vec::as_slice)
    }

    /// Pages that link **to** the given page. Deterministic order (sorted by
    /// `PageId`). Empty when there are no inbound links.
    pub fn backlinks(&self, id: PageId) -> impl Iterator<Item = PageId> + '_ {
        self.backlinks
            .get(&id)
            .into_iter()
            .flat_map(|s| s.iter().copied())
    }

    /// Pages with no inbound links from any other page. Deterministic order.
    pub fn orphans(&self) -> impl Iterator<Item = &PageInfo> + '_ {
        self.pages
            .values()
            .filter(|info| self.backlinks.get(&info.id).is_none_or(BTreeSet::is_empty))
    }

    /// All broken (unresolved, non-self) wikilinks in the vault.
    pub fn broken_links(&self) -> impl Iterator<Item = (PageId, &LinkRef)> + '_ {
        self.forward.iter().flat_map(|(id, refs)| {
            let owner = *id;
            refs.iter()
                .filter(|r| r.resolved.is_broken())
                .map(move |r| (owner, r))
        })
    }

    /// All ambiguous wikilinks (target matches more than one page).
    pub fn ambiguous_links(&self) -> impl Iterator<Item = (PageId, &LinkRef)> + '_ {
        self.forward.iter().flat_map(|(id, refs)| {
            let owner = *id;
            refs.iter()
                .filter(|r| matches!(r.resolved, Resolution::Ambiguous(_)))
                .map(move |r| (owner, r))
        })
    }

    /// All file stems that map to more than one page (collisions).
    pub fn duplicate_stems(&self) -> impl Iterator<Item = (&str, &[PageId])> {
        self.by_stem
            .iter()
            .filter(|(_, ids)| ids.len() > 1)
            .map(|(s, ids)| (s.as_str(), ids.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, Page};
    use chrono::TimeZone;

    fn page(stem: &str, title: &str, body: &str) -> Page {
        let now = chrono::Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: title.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: body.into(),
        }
    }

    #[test]
    fn build_resolves_simple_link() {
        let foo = page("foo", "Foo", "Body of foo.\n");
        let bar = page("bar", "Bar", "Bar links to [[foo]].\n");
        let foo_id = foo.frontmatter.id;
        let bar_id = bar.frontmatter.id;
        let graph = LinkGraph::build(&[foo, bar]);
        let bar_forward = graph.forward(bar_id).unwrap();
        assert_eq!(bar_forward.len(), 1);
        assert_eq!(bar_forward[0].resolved, Resolution::Resolved(foo_id));
    }

    #[test]
    fn build_records_backlinks() {
        let foo = page("foo", "Foo", "Body.\n");
        let bar = page("bar", "Bar", "Links [[foo]].\n");
        let foo_id = foo.frontmatter.id;
        let bar_id = bar.frontmatter.id;
        let graph = LinkGraph::build(&[foo, bar]);
        let backs: Vec<_> = graph.backlinks(foo_id).collect();
        assert_eq!(backs, vec![bar_id]);
    }

    #[test]
    fn detects_broken_link() {
        let foo = page("foo", "Foo", "Links [[nonexistent]].\n");
        let foo_id = foo.frontmatter.id;
        let graph = LinkGraph::build(&[foo]);
        let broken: Vec<_> = graph.broken_links().collect();
        assert_eq!(broken.len(), 1);
        assert_eq!(broken[0].0, foo_id);
        assert_eq!(broken[0].1.link.target, "nonexistent");
    }

    #[test]
    fn detects_ambiguous_link() {
        // Two pages with the same stem (different paths via different sub-titles).
        let mut a = page("foo", "Foo One", "Body.\n");
        a.path = Utf8PathBuf::from("wiki/concepts/foo.md");
        let mut b = page("foo", "Foo Two", "Body.\n");
        b.path = Utf8PathBuf::from("wiki/topics/foo.md");
        let c = page("bar", "Bar", "Links [[foo]].\n");
        let c_id = c.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, c]);
        let ambiguous: Vec<_> = graph.ambiguous_links().collect();
        assert_eq!(ambiguous.len(), 1);
        assert_eq!(ambiguous[0].0, c_id);
        match &ambiguous[0].1.resolved {
            Resolution::Ambiguous(ids) => assert_eq!(ids.len(), 2),
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        // duplicate_stems should also report it.
        let dups: Vec<_> = graph.duplicate_stems().collect();
        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].0, "foo");
    }

    #[test]
    fn finds_orphans() {
        let foo = page("foo", "Foo", "Body.\n");
        let bar = page("bar", "Bar", "Links [[foo]].\n");
        let baz = page("baz", "Baz", "No inbound links.\n");
        let baz_id = baz.frontmatter.id;
        let graph = LinkGraph::build(&[foo, bar, baz]);
        let orphans: Vec<_> = graph.orphans().map(|p| p.id).collect();
        // bar and baz are both orphans (foo is not — bar links to it).
        // Order is by PageId (BTreeMap), so we check membership.
        assert_eq!(orphans.len(), 2);
        assert!(orphans.contains(&baz_id));
    }

    #[test]
    fn self_heading_link_is_not_a_backlink() {
        let foo = page("foo", "Foo", "See [[#section]] above.\n");
        let foo_id = foo.frontmatter.id;
        let graph = LinkGraph::build(&[foo]);
        let backs: Vec<_> = graph.backlinks(foo_id).collect();
        assert!(backs.is_empty());
        let forward = graph.forward(foo_id).unwrap();
        assert_eq!(forward[0].resolved, Resolution::SelfHeading);
    }

    #[test]
    fn ignores_links_inside_code_blocks() {
        let body = "Real [[foo]]\n\n```\nfake [[ghost]]\n```\n";
        let foo = page("foo", "Foo", "Body.\n");
        let bar = page("bar", "Bar", body);
        let bar_id = bar.frontmatter.id;
        let graph = LinkGraph::build(&[foo, bar]);
        let forward = graph.forward(bar_id).unwrap();
        assert_eq!(forward.len(), 1);
        assert_eq!(forward[0].link.target, "foo");
    }
}
