//! End-to-end integration tests against `tests/fixtures/sample-vault/`.
//!
//! These tests run on every `cargo test` invocation, never touch the network,
//! and assert the behaviour of the whole stack from a fresh vault on disk.
//! They grow alongside each phase:
//!
//! - phase 4 (this phase) — scan the fixture, exercise the link graph, build
//!   a [`MockProvider`] and verify the trait surface
//! - phase 5 — mechanical lint end-to-end
//! - phase 8 — mock-driven ingest
//! - phase 10 — mock-driven query
//!
//! Each new phase adds a test module here instead of spreading integration
//! tests across the workspace.

use std::path::PathBuf;

use scriptorium_core::embed::{self, EmbeddingsStore};
use scriptorium_core::ingest;
use scriptorium_core::lint::{
    self, broken_links::BROKEN, frontmatter::DUPLICATE_ID, orphans::RULE as ORPHAN, Severity,
};
use scriptorium_core::llm::{
    CompletionRequest, IngestAction, IngestPageAction, IngestPlan, LlmProvider, MockProvider,
    QueryAnswer,
};
use scriptorium_core::query;
use scriptorium_core::vault::{Action, LinkGraph, Vault};
use tempfile::TempDir;

fn fixture_vault() -> Vault {
    let root: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "fixtures",
        "sample-vault",
    ]
    .iter()
    .collect();
    Vault::open(&root).expect("sample-vault opens")
}

#[test]
fn scan_finds_all_fixture_pages() {
    let vault = fixture_vault();
    let report = vault.scan().expect("scan succeeds");
    assert!(
        report.errors.is_empty(),
        "no parse errors: {:?}",
        report.errors
    );
    assert_eq!(report.page_count(), 4, "4 pages under wiki/");

    let titles: Vec<_> = report
        .pages
        .iter()
        .map(|p| p.frontmatter.title.as_str())
        .collect();
    assert!(titles.contains(&"Transformers"));
    assert!(titles.contains(&"Attention"));
    assert!(titles.contains(&"BERT"));
    assert!(titles.contains(&"Orphan Note"));
}

#[test]
fn link_graph_has_expected_edges() {
    let vault = fixture_vault();
    let report = vault.scan().unwrap();
    let graph = LinkGraph::build(&report.pages);
    assert_eq!(graph.page_count(), 4);

    // One broken link from transformers → does-not-exist.
    let broken: Vec<_> = graph.broken_links().collect();
    assert_eq!(broken.len(), 1, "exactly one deliberate broken link");
    assert_eq!(broken[0].1.link.target, "does-not-exist");

    // Orphan: orphan-note has no inbound links.
    let orphans: Vec<_> = graph.orphans().map(|p| p.title.clone()).collect();
    assert!(
        orphans.iter().any(|t| t == "Orphan Note"),
        "orphan-note should be an orphan; got {orphans:?}"
    );
    // transformers has backlinks from bert + attention.
    let transformers = report
        .pages
        .iter()
        .find(|p| p.frontmatter.title == "Transformers")
        .unwrap();
    let backs: Vec<_> = graph.backlinks(transformers.frontmatter.id).collect();
    assert_eq!(backs.len(), 2, "bert + attention link to transformers");
}

#[test]
fn mechanical_lint_flags_fixture_issues() {
    let vault = fixture_vault();
    let report = lint::run(&vault).expect("lint runs");
    // Fixture has: 1 deliberate broken link, 1 orphan page, no duplicate IDs.
    let broken: Vec<_> = report.issues.iter().filter(|i| i.rule == BROKEN).collect();
    assert_eq!(broken.len(), 1, "exactly one broken link; got {report:?}");
    assert_eq!(broken[0].severity, Severity::Error);

    let orphans: Vec<_> = report.issues.iter().filter(|i| i.rule == ORPHAN).collect();
    assert_eq!(orphans.len(), 1, "exactly one orphan; got {report:?}");
    assert_eq!(orphans[0].severity, Severity::Warning);
    assert!(orphans[0]
        .path
        .as_ref()
        .unwrap()
        .as_str()
        .ends_with("orphan-note.md"));

    let dups: Vec<_> = report
        .issues
        .iter()
        .filter(|i| i.rule == DUPLICATE_ID)
        .collect();
    assert!(dups.is_empty(), "no duplicate IDs in the fixture");

    // Broken link is an error; report should reflect that.
    assert!(report.has_errors());
    assert!(!report.is_clean());
}

fn empty_test_vault() -> (TempDir, Vault) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
    std::fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
    // Minimal CLAUDE.md so schema loading has something to work with.
    std::fs::write(
        dir.path().join("CLAUDE.md"),
        "# Vault Rules\n\nBe concise and link between concepts.\n",
    )
    .unwrap();
    let vault = Vault::open(dir.path()).unwrap();
    (dir, vault)
}

#[tokio::test]
async fn ingest_creates_page_and_log_entry_via_mock() {
    let (dir, vault) = empty_test_vault();

    // Prepare the plan the mock will return.
    let plan = IngestPlan {
        summary: "a mocked ingest".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/mock.md".into(),
            title: "Mock Page".into(),
            tags: vec!["test".into()],
            // No wikilinks — avoid tripping broken-link validation.
            body: "This is a mock page body.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "ingested test-source.md".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());

    // Drop a source file inside the vault.
    let source = dir.path().join("sources/articles/test-source.md");
    std::fs::write(&source, "Source file contents.").unwrap();

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert_eq!(report.created, 1);
    assert_eq!(report.updated, 0);
    assert_eq!(report.commit_id.len(), 40);
    assert!(report.source.as_str().starts_with("sources/articles/"));

    // Page landed in the vault with frontmatter linking back to the source.
    let page_path = dir.path().join("wiki/concepts/mock.md");
    let page_text = std::fs::read_to_string(&page_path).unwrap();
    assert!(page_text.contains("title: Mock Page"));
    assert!(page_text.contains("This is a mock page body"));
    assert!(page_text.contains("sources/articles/"));

    // log.md has the new entry.
    let log = std::fs::read_to_string(dir.path().join("log.md")).unwrap();
    assert!(log.contains("ingest | a mocked ingest"));
    assert!(log.contains("ingested test-source.md"));

    // A git commit was created.
    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    assert!(head.message().unwrap().contains("a mocked ingest"));
}

#[tokio::test]
async fn ingest_updates_existing_page_and_preserves_id() {
    let (dir, vault) = empty_test_vault();

    // Pre-seed an existing page via an ingest call.
    let first_plan = IngestPlan {
        summary: "seed".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/target.md".into(),
            title: "Target".into(),
            tags: vec![],
            body: "Original body.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "seed".into(),
        redundant: false,
    };
    let seed = MockProvider::constant(serde_json::to_string(&first_plan).unwrap());
    let source = dir.path().join("sources/articles/seed.md");
    std::fs::write(&source, "seed").unwrap();
    ingest::ingest(&vault, &seed, &source).await.unwrap();

    // Grab the ID that the seed assigned so we can assert it's preserved.
    let scan1 = vault.scan().unwrap();
    let original = scan1
        .pages
        .iter()
        .find(|p| p.path == "wiki/concepts/target.md")
        .unwrap()
        .clone();

    // Now run an update ingest against the same page.
    let update_plan = IngestPlan {
        summary: "update".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Update,
            path: "wiki/concepts/target.md".into(),
            title: "Target".into(),
            tags: vec!["refined".into()],
            body: "Revised body.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "updated target".into(),
        redundant: false,
    };
    let updater = MockProvider::constant(serde_json::to_string(&update_plan).unwrap());
    let source2 = dir.path().join("sources/articles/revision.md");
    std::fs::write(&source2, "revision").unwrap();
    let report = ingest::ingest(&vault, &updater, &source2).await.unwrap();
    assert_eq!(report.created, 0);
    assert_eq!(report.updated, 1);

    // ID preserved, body swapped, tags updated, source appended.
    let scan2 = vault.scan().unwrap();
    let after = scan2
        .pages
        .iter()
        .find(|p| p.path == "wiki/concepts/target.md")
        .unwrap();
    assert_eq!(after.frontmatter.id, original.frontmatter.id);
    assert!(after.body.contains("Revised body"));
    assert_eq!(after.frontmatter.tags, vec!["refined".to_string()]);
    assert_eq!(after.frontmatter.sources.len(), 2);
}

#[tokio::test]
async fn ingest_unlinks_unresolvable_plan_links_instead_of_aborting() {
    let (dir, vault) = empty_test_vault();

    // Plan references a non-existent page. Historically this aborted the
    // whole commit at the lint gate; since the bulk-ingest hardening the
    // phantom link is unlinked to plain text and the commit proceeds
    // (cross-referencing corpora legitimately link concepts that later
    // sources define). Links to pages created by the same plan survive.
    let plan = IngestPlan {
        summary: "phantom link".into(),
        pages: vec![
            IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/broken.md".into(),
                title: "Broken".into(),
                tags: vec![],
                body: "Linking to [[ghost-page]] and [[sibling]].\n".into(),
                source_quote: None,
                supersedes: vec![],
            },
            IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/sibling.md".into(),
                title: "Sibling".into(),
                tags: vec![],
                body: "Sibling page.\n".into(),
                source_quote: None,
                supersedes: vec![],
            },
        ],
        log_entry: "phantom link".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/bad.md");
    std::fs::write(&source, "bad").unwrap();

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert_eq!(report.created, 2);
    let body = std::fs::read_to_string(dir.path().join("wiki/concepts/broken.md")).unwrap();
    assert!(
        body.contains("Linking to ghost-page and [[sibling]]."),
        "phantom link unlinked, same-plan link kept: {body}"
    );
}

#[tokio::test]
async fn ingest_dry_run_stages_but_does_not_commit() {
    let (dir, vault) = empty_test_vault();
    let plan = IngestPlan {
        summary: "dry run preview".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/preview.md".into(),
            title: "Preview".into(),
            tags: vec!["draft".into()],
            body: "Body with no wikilinks.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "would ingest".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/src.md");
    std::fs::write(&source, "source content").unwrap();

    let report = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions {
            dry_run: true,
            hooks: None,
            force: false,
        },
    )
    .await
    .unwrap();

    assert_eq!(report.commit_id, "dry-run");
    assert_eq!(report.created, 1);
    assert!(!report.dry_run_diff.is_empty());
    // No wiki page on disk.
    assert!(!dir.path().join("wiki/concepts/preview.md").exists());
    // No log entry.
    assert!(!dir.path().join("log.md").exists());
    // Git history is only the init commit from empty_test_vault (none here).
    // The preview names the interned path as a file to create, but the dry
    // run writes nothing there. The file it was given stays.
    assert!(report.source.as_str().starts_with("sources/articles/"));
    assert!(report
        .dry_run_diff
        .iter()
        .any(|c| c.path == report.source && c.action == Action::Create));
    assert!(!dir.path().join(report.source.as_str()).exists());
    assert!(source.exists());
}

#[tokio::test]
async fn ingest_writes_usage_log() {
    let (dir, vault) = empty_test_vault();
    let plan = IngestPlan {
        summary: "usage test".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/usage.md".into(),
            title: "Usage".into(),
            tags: vec![],
            body: "Body.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "u".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/usage.md");
    std::fs::write(&source, "body").unwrap();
    ingest::ingest(&vault, &mock, &source).await.unwrap();

    let log_path = dir.path().join(".scriptorium/usage.jsonl");
    assert!(log_path.exists(), "usage.jsonl should be written");
    let contents = std::fs::read_to_string(&log_path).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 1, "one ingest call → one usage line");
    assert!(lines[0].contains("\"op\":\"ingest\""));
    assert!(lines[0].contains("\"provider\":\"mock\""));
}

#[tokio::test]
async fn query_returns_cited_answer_via_mock() {
    let (dir, vault) = empty_test_vault();

    // Seed the vault via ingest: create a page about "attention".
    let seed_plan = IngestPlan {
        summary: "seed".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/attention.md".into(),
            title: "Attention".into(),
            tags: vec!["concept".into()],
            body: "Attention weighs sequence positions. It runs in parallel.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "seed".into(),
        redundant: false,
    };
    let seed_mock = MockProvider::constant(serde_json::to_string(&seed_plan).unwrap());
    let source = dir.path().join("sources/articles/seed.md");
    std::fs::write(&source, "seed body").unwrap();
    ingest::ingest(&vault, &seed_mock, &source).await.unwrap();

    // Populate the embeddings store with the mock embedder.
    let store = EmbeddingsStore::in_memory().unwrap();
    let embedded = embed::reindex(&vault, &store, &seed_mock, "mock-1")
        .await
        .unwrap();
    assert!(embedded > 0, "expected at least one chunk embedded");

    // Now run a query. Use a *different* mock that returns a QueryAnswer.
    let answer = QueryAnswer {
        answer: "Attention weighs positions and runs in parallel; see [[attention]].".into(),
        citations: vec!["attention".into()],
        confidence: Some(0.9),
    };
    let query_mock = MockProvider::constant(serde_json::to_string(&answer).unwrap());

    // Cross-provider query: query_mock generates the answer, seed_mock
    // (which is the same mock provider used to reindex) handles the
    // embedding of the question. In production, these would typically be
    // different providers — e.g. Claude for answer, Gemini for embed.
    let report = query::query(
        &vault,
        &store,
        &query_mock,
        &seed_mock,
        "mock-1",
        "how does attention work?",
        5,
        &scriptorium_core::config::SearchConfig::default(),
    )
    .await
    .unwrap();

    assert_eq!(report.answer.answer, answer.answer);
    assert_eq!(report.cited_stems, vec!["attention".to_string()]);
    assert!(
        !report.retrieved.is_empty(),
        "should retrieve at least one page"
    );
}

#[tokio::test]
async fn query_strips_citations_for_pages_not_retrieved() {
    let (dir, vault) = empty_test_vault();
    let seed = IngestPlan {
        summary: "seed".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/topics/alpha.md".into(),
            title: "Alpha".into(),
            tags: vec![],
            body: "Alpha is the first letter.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "seed".into(),
        redundant: false,
    };
    let seed_mock = MockProvider::constant(serde_json::to_string(&seed).unwrap());
    let source = dir.path().join("sources/articles/seed.md");
    std::fs::write(&source, "seed").unwrap();
    ingest::ingest(&vault, &seed_mock, &source).await.unwrap();

    let store = EmbeddingsStore::in_memory().unwrap();
    embed::reindex(&vault, &store, &seed_mock, "mock-1")
        .await
        .unwrap();

    // The LLM hallucinates a citation to a page the retrieval never surfaced.
    let bad_answer = QueryAnswer {
        answer: "see [[nonexistent]]".into(),
        citations: vec!["nonexistent".into(), "alpha".into()],
        confidence: None,
    };
    let bad_mock = MockProvider::constant(serde_json::to_string(&bad_answer).unwrap());
    let report = query::query(
        &vault,
        &store,
        &bad_mock,
        &seed_mock,
        "mock-1",
        "alphabet?",
        5,
        &scriptorium_core::config::SearchConfig::default(),
    )
    .await
    .unwrap();
    // "nonexistent" was dropped because it isn't in the retrieved set.
    assert_eq!(report.cited_stems, vec!["alpha".to_string()]);
}

// ---------- ingest commits interned source + produces non-empty commit ----------
//
// Regression tests pinning two related bugs that produced empty and
// partially-written commits in scriptorium-vault:
//
// 1. `VaultTx::commit` → `git::commit_paths` → libgit2 `index.add_all`
//    was silently producing empty commits when `commit_paths` received
//    relative paths with a `./` prefix (which happens when the CLI opens
//    the vault at `"."` and `apply_and_commit` does `root.join("log.md")`
//    → `./log.md`). libgit2's pathspec matcher doesn't treat `./log.md`
//    as equivalent to the index entry `log.md`.
//
// 2. `intern_source` writes the interned source to disk via
//    `std::fs::write`, bypassing the VaultTx. The interned file never
//    entered git, so every wiki page's `sources: [...]` frontmatter
//    referenced a file that didn't exist in the git history.
//
// The fix for (1) is in `git::stage_paths` (normalize to absolute before
// strip_prefix, use `add_path` per file). The fix for (2) is in
// `ingest::ingest_with_options` (stage the interned source via
// `tx.put_file`). Both must hold for this test to pass.

#[tokio::test]
async fn ingest_commit_is_non_empty_and_contains_both_source_and_wiki_page() {
    let (dir, vault) = empty_test_vault();

    let plan = IngestPlan {
        summary: "commit coverage".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/covered.md".into(),
            title: "Covered".into(),
            tags: vec!["test".into()],
            body: "## Body\n\nPlain body with no wikilinks.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "ingested source".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/coverage-source.md");
    std::fs::write(&source, "Coverage source contents.\n").unwrap();

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert_eq!(report.created, 1);
    assert_eq!(report.commit_id.len(), 40);

    // Inspect the HEAD commit's tree. It MUST contain:
    //   - wiki/concepts/covered.md (the page)
    //   - log.md (the append)
    //   - the interned source under sources/articles/<hash>-<slug>.md
    //   - (optionally index.md if it was regenerated)
    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    let tree = head.tree().unwrap();

    // Non-empty commit assertion — guards against the `./log.md` bug.
    // `empty_test_vault` returns a vault with no prior commits, so this
    // ingest is the initial commit (no parent). In that case, the guard
    // becomes "tree is not empty" (enforced by the walk assertions below).
    // If a parent exists, assert tree divergence.
    if let Ok(parent) = head.parent(0) {
        assert_ne!(
            head.tree_id(),
            parent.tree_id(),
            "commit tree matches parent tree — ingest produced an empty commit \
             (the `./log.md` libgit2 pathspec bug)"
        );
    }

    // Walk wiki/concepts/covered.md
    let wiki_oid = tree.get_name("wiki").expect("wiki/ tree").id();
    let wiki_tree = repo.find_tree(wiki_oid).unwrap();
    let concepts_oid = wiki_tree.get_name("concepts").expect("wiki/concepts/").id();
    let concepts_tree = repo.find_tree(concepts_oid).unwrap();
    assert!(
        concepts_tree.get_name("covered.md").is_some(),
        "commit tree missing wiki/concepts/covered.md"
    );

    // Walk sources/articles/<hash>-<slug>.md — the interned source.
    // We don't know the hash prefix ahead of time, so just enumerate.
    let sources_oid = tree.get_name("sources").expect("sources/ tree").id();
    let sources_tree = repo.find_tree(sources_oid).unwrap();
    let articles_oid = sources_tree
        .get_name("articles")
        .expect("sources/articles/ tree")
        .id();
    let articles_tree = repo.find_tree(articles_oid).unwrap();
    // Interned sources are written with a lowercase `.md` suffix; match it exactly.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    let any_source = (0..articles_tree.len())
        .map(|i| articles_tree.get(i).unwrap())
        .find(|entry| {
            let name = entry.name().unwrap_or("");
            name.contains("coverage-source") && name.ends_with(".md")
        });
    assert!(
        any_source.is_some(),
        "commit tree missing interned source under sources/articles/<hash>-coverage-source.md — \
         tx.put_file for the interned source was not committed"
    );

    // log.md is in the tree with the new entry.
    let log_oid = tree.get_name("log.md").expect("log.md in tree").id();
    let log_blob = repo.find_blob(log_oid).unwrap();
    let log_body = std::str::from_utf8(log_blob.content()).unwrap();
    assert!(
        log_body.contains("ingested source"),
        "committed log.md missing the append: {log_body:?}"
    );
}

/// Regression for the second ingest scenario: `empty_test_vault` uses an
/// absolute `TempDir` path, but the production failure happened when the
/// CLI ran from inside the vault with `-C .`. This test explicitly
/// exercises a `Vault` opened via a relative path to cover that code
/// path at the e2e level.
#[tokio::test]
async fn ingest_commit_is_non_empty_when_vault_opened_via_relative_path() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
    std::fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
    std::fs::write(
        dir.path().join("CLAUDE.md"),
        "# Vault Rules\n\nBe concise.\n",
    )
    .unwrap();

    // CHANGE current directory into the temp vault and open with a
    // RELATIVE path. This reproduces the CLI flow where `scriptorium
    // ingest` is run from inside the vault without `-C`.
    let orig_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    // Open with `.` to force vault.root() to be a relative path.
    let vault = Vault::open(std::path::Path::new(".")).unwrap();

    let plan = IngestPlan {
        summary: "relative path test".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/rel.md".into(),
            title: "Rel".into(),
            tags: vec![],
            body: "plain body\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "relative".into(),
        redundant: false,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/rel-source.md");
    std::fs::write(&source, "rel source\n").unwrap();

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();

    // Restore cwd BEFORE assertions so a test failure doesn't leave the
    // test process in the tempdir (which is about to be removed).
    std::env::set_current_dir(&orig_cwd).unwrap();

    assert_eq!(report.created, 1);
    // The critical assertion: non-empty commit.
    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    let parent = head.parent(0).ok();
    if let Some(parent) = parent {
        assert_ne!(
            head.tree_id(),
            parent.tree_id(),
            "relative-vault-path ingest produced an empty commit — the \
             `./log.md` bug is back"
        );
    }
    // Page is in the tree.
    let tree = head.tree().unwrap();
    let wiki = repo
        .find_tree(tree.get_name("wiki").expect("wiki/").id())
        .unwrap();
    let concepts = repo
        .find_tree(wiki.get_name("concepts").expect("concepts/").id())
        .unwrap();
    assert!(concepts.get_name("rel.md").is_some());
}

// ---------- ingest + embed::reindex contract ----------
//
// These tests pin the building blocks the CLI/MCP dispatch sites compose
// to give callers "ingested pages are immediately searchable" semantics.
// Each dispatch site calls `ingest_with_options` then `embed::reindex` —
// these tests verify the second step finds the just-ingested pages and
// only does work for chunks that aren't already cached.

#[tokio::test]
async fn embed_reindex_after_ingest_writes_chunks_for_new_pages() {
    let (dir, vault) = empty_test_vault();

    let plan = IngestPlan {
        summary: "embed coverage".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/embedme.md".into(),
            title: "Embed Me".into(),
            tags: vec!["test".into()],
            body: "## First\n\nFirst section body.\n\n## Second\n\nSecond section body.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "ingested embedme".into(),
        redundant: false,
    };
    let chat_mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let embed_mock = MockProvider::constant("");
    let source = dir.path().join("sources/articles/embed-source.md");
    std::fs::write(&source, "embed-source-bytes").unwrap();

    // Step 1: ingest the source via the chat mock.
    let report = ingest::ingest(&vault, &chat_mock, &source).await.unwrap();
    assert_eq!(report.created, 1);

    // Step 2: reindex with the embed mock — must walk the freshly-written
    // page set and embed both chunks (one per H2 section).
    let store = EmbeddingsStore::in_memory().unwrap();
    let written = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(
        written, 2,
        "two H2 sections in the new page should produce two embedded chunks"
    );
    assert_eq!(store.len().unwrap(), 2);
}

#[tokio::test]
async fn embed_reindex_after_ingest_is_cache_hit_on_repeat() {
    let (dir, vault) = empty_test_vault();

    let plan = IngestPlan {
        summary: "cache test".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/cached.md".into(),
            title: "Cached".into(),
            tags: vec![],
            body: "## Single\n\nOne section is enough.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "seed".into(),
        redundant: false,
    };
    let chat_mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let embed_mock = MockProvider::constant("");
    let source = dir.path().join("sources/articles/cache-source.md");
    std::fs::write(&source, "cache-source-bytes").unwrap();

    ingest::ingest(&vault, &chat_mock, &source).await.unwrap();

    let store = EmbeddingsStore::in_memory().unwrap();
    let first = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert!(first >= 1, "first reindex must embed at least one chunk");
    let after_first = store.len().unwrap();

    // Second call against the same vault — page content unchanged, so
    // every chunk's `(page_id, content_hash, provider, model)` is already
    // in the store. The reindex should report 0 new chunks and the store
    // size should be unchanged.
    let second = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(
        second, 0,
        "repeat reindex on unchanged content must be a full cache hit"
    );
    assert_eq!(
        store.len().unwrap(),
        after_first,
        "cache hit must not grow the store"
    );
}

/// `embed::reindex` should prune orphan rows for pages that have been
/// removed from the vault (e.g. via direct file delete, `scriptorium
/// undo` / git revert, or rename). Before this fix, removed pages'
/// chunks stayed in the embeddings store forever and showed up in
/// `scriptorium_search` as "valid" hits — a data-integrity bug.
#[tokio::test]
async fn embed_reindex_prunes_orphan_rows_for_deleted_pages() {
    let (dir, vault) = empty_test_vault();
    let embed_mock = MockProvider::constant("");
    let store = EmbeddingsStore::in_memory().unwrap();

    // Seed three pages directly via write_page transactions (no ingest
    // LLM needed).
    for (stem, body) in [
        ("alpha", "## A1\n\nAlpha body.\n"),
        ("beta", "## B1\n\nBeta body.\n"),
        ("gamma", "## G1\n\nGamma body.\n"),
    ] {
        let path = camino::Utf8PathBuf::from(format!("wiki/concepts/{stem}.md"));
        let mut fm = scriptorium_core::vault::Frontmatter::new(stem);
        fm.tags = vec!["test".into()];
        let page = scriptorium_core::vault::Page {
            path: path.clone(),
            frontmatter: fm,
            body: (*body).into(),
        };
        let mut tx = vault.begin();
        tx.write_page(&page).unwrap();
        tx.commit_without_validation("seed").unwrap();
    }

    // Reindex — all three pages embed.
    let first = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(first, 3, "three H2 sections → three chunks embedded");
    let after_first = store.len().unwrap();
    assert_eq!(after_first, 3);

    // Delete one page directly from disk (simulates `scriptorium undo`
    // removing a file via git-revert, or a manual rm). The page_id row
    // in the embeddings store is now an orphan.
    std::fs::remove_file(dir.path().join("wiki/concepts/gamma.md")).unwrap();

    // Reindex again — no new chunks to embed (alpha + beta are cache
    // hits) but gamma's orphan row must be pruned.
    let second = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(second, 0, "unchanged pages are cache hits, nothing new");
    assert_eq!(
        store.len().unwrap(),
        2,
        "gamma's orphan row should have been pruned, store size 3 → 2"
    );
}

/// `embed::reindex` should also prune orphan rows for pages that are
/// STILL in the vault but whose content has been updated — the old
/// `content_hash` rows are superseded by new ones. Without pruning,
/// every edit to a page would accumulate dead rows forever.
#[tokio::test]
async fn embed_reindex_prunes_stale_content_hash_rows_on_page_update() {
    let (dir, vault) = empty_test_vault();
    let embed_mock = MockProvider::constant("");
    let store = EmbeddingsStore::in_memory().unwrap();

    // Seed a page.
    let path = camino::Utf8PathBuf::from("wiki/concepts/mutable.md");
    let mut fm = scriptorium_core::vault::Frontmatter::new("Mutable");
    fm.tags = vec!["test".into()];
    let page_v1 = scriptorium_core::vault::Page {
        path: path.clone(),
        frontmatter: fm.clone(),
        body: "## V1\n\nFirst version body.\n".into(),
    };
    {
        let mut tx = vault.begin();
        tx.write_page(&page_v1).unwrap();
        tx.commit_without_validation("seed v1").unwrap();
    }

    // First reindex: embed v1.
    let first = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(first, 1);
    assert_eq!(store.len().unwrap(), 1);

    // Overwrite the page on disk with new body (no new page_id). This
    // simulates a `scriptorium_write_page` patch, an ingest update, or
    // a manual edit — any path that changes `Page::content_hash` while
    // keeping `Page::frontmatter.id` the same.
    let existing = std::fs::read_to_string(dir.path().join(path.as_str())).unwrap();
    let updated_body = existing.replacen(
        "## V1\n\nFirst version body.\n",
        "## V1\n\nFirst version body.\n\n## V2\n\nSecond version body added.\n",
        1,
    );
    std::fs::write(dir.path().join(path.as_str()), updated_body).unwrap();

    // Second reindex: v1 hash no longer matches, so v2 gets embedded
    // as a new version. The old v1 rows must be pruned.
    let second = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(
        second, 2,
        "updated page has two H2 sections → two new chunks embedded"
    );
    assert_eq!(
        store.len().unwrap(),
        2,
        "store should contain only v2's chunks (1 old pruned + 2 new = 2 total, not 3)"
    );
}

#[tokio::test]
async fn embed_reindex_after_two_distinct_ingests_only_embeds_new_pages() {
    let (dir, vault) = empty_test_vault();
    let embed_mock = MockProvider::constant("");
    let store = EmbeddingsStore::in_memory().unwrap();

    // Ingest A.
    let plan_a = IngestPlan {
        summary: "page a".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/a.md".into(),
            title: "A".into(),
            tags: vec![],
            body: "## A1\n\nFirst.\n\n## A2\n\nSecond.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "a".into(),
        redundant: false,
    };
    let chat_a = MockProvider::constant(serde_json::to_string(&plan_a).unwrap());
    let source_a = dir.path().join("sources/articles/a.md");
    std::fs::write(&source_a, "a-bytes").unwrap();
    ingest::ingest(&vault, &chat_a, &source_a).await.unwrap();

    let after_a = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(after_a, 2, "page A has two H2 sections");
    let store_after_a = store.len().unwrap();
    assert_eq!(store_after_a, 2);

    // Ingest B (different page, different body).
    let plan_b = IngestPlan {
        summary: "page b".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/b.md".into(),
            title: "B".into(),
            tags: vec![],
            body: "## B1\n\nB first.\n".into(),
            source_quote: None,
            supersedes: vec![],
        }],
        log_entry: "b".into(),
        redundant: false,
    };
    let chat_b = MockProvider::constant(serde_json::to_string(&plan_b).unwrap());
    let source_b = dir.path().join("sources/articles/b.md");
    std::fs::write(&source_b, "b-bytes").unwrap();
    ingest::ingest(&vault, &chat_b, &source_b).await.unwrap();

    // Second reindex should ONLY embed B's new chunk(s); A is already cached.
    let after_b = embed::reindex(&vault, &store, &embed_mock, "mock-embed-test")
        .await
        .unwrap();
    assert_eq!(
        after_b, 1,
        "only page B's new chunk should be embedded; page A is a cache hit"
    );
    assert_eq!(
        store.len().unwrap(),
        store_after_a + 1,
        "store should grow by exactly the new chunk count"
    );
}

#[tokio::test]
async fn mock_provider_round_trips_a_structured_response() {
    // Canned "ingest plan" the mock returns when the prompt mentions INGEST.
    let fixture = r#"{"summary":"attention is parallel","pages":[]}"#;
    let mock = MockProvider::with_fixtures([("INGEST".to_string(), fixture.to_string())]);

    let req =
        CompletionRequest::new("You are the INGEST operator.").with_user("Process this source.");
    let resp = mock.complete(req).await.unwrap();
    assert_eq!(resp.text, fixture);

    // Embeddings are deterministic and the right shape.
    let vecs = mock
        .embed(&["hello".to_string(), "world".to_string()])
        .await
        .unwrap();
    assert_eq!(vecs.len(), 2);
    assert_eq!(vecs[0].len(), mock.embedding_dim());
    let vecs2 = mock
        .embed(&["hello".to_string(), "world".to_string()])
        .await
        .unwrap();
    assert_eq!(vecs, vecs2, "embeddings are deterministic");
}

#[tokio::test]
async fn ingest_salvages_missing_log_entry_from_llm_response() {
    // Reproduces today's production bug: the LLM returns an IngestPlan with
    // `summary` and `pages` but omits `log_entry`. With the salvage layer
    // in place, the ingest should succeed and the missing log_entry should
    // be synthesized from the summary.
    let (dir, vault) = empty_test_vault();

    // Craft raw JSON directly (NOT via the `IngestPlan` Serialize path,
    // because serializing a real `IngestPlan` always includes log_entry).
    let raw = serde_json::json!({
        "summary": "salvage test",
        "pages": [{
            "action": "create",
            "path": "wiki/concepts/salvage.md",
            "title": "Salvage",
            "tags": [],
            "body": "Body with no wikilinks.\n"
        }]
    });
    let mock = MockProvider::constant(raw.to_string());
    let source = dir.path().join("sources/articles/salvage.md");
    std::fs::write(&source, "body").unwrap();

    let report = ingest::ingest(&vault, &mock, &source)
        .await
        .expect("ingest must succeed via log_entry salvage");
    assert_eq!(report.created, 1);
    assert_eq!(report.summary, "salvage test");

    // The synthesized log entry landed in log.md with the auto-synthesized
    // marker, so a human reviewing the log knows it wasn't the model's text.
    let log_contents = std::fs::read_to_string(dir.path().join("log.md")).unwrap();
    assert!(log_contents.contains("salvage test"));
    assert!(
        log_contents.contains("auto-synthesized"),
        "synthesized log entry should be labelled: {log_contents}"
    );
}

#[tokio::test]
async fn ingest_persists_failure_record_on_unsalvageable_response() {
    // When the LLM returns JSON missing a field the salvage layer cannot
    // recover (e.g. `pages` is absent, which has no sensible default), the
    // ingest must fail loudly AND persist the raw response to
    // `<vault>/.ingest-failures/` for later debugging.
    let (dir, vault) = empty_test_vault();

    let unsalvageable = r#"{"summary": "broken"}"#; // no `pages` field
    let mock = MockProvider::constant(unsalvageable.to_string());
    let source = dir.path().join("sources/articles/broken.md");
    std::fs::write(&source, "body").unwrap();

    let err = ingest::ingest(&vault, &mock, &source).await.unwrap_err();
    assert!(
        err.to_string().contains("invalid IngestPlan"),
        "error should mention invalid IngestPlan: {err}"
    );
    assert!(
        err.to_string().contains(".ingest-failures/"),
        "error should point at the persisted record path: {err}"
    );

    // Failure record must exist under .ingest-failures/.
    let failures_dir = dir.path().join(".ingest-failures");
    assert!(
        failures_dir.exists(),
        ".ingest-failures directory must be created"
    );
    let entries: Vec<_> = std::fs::read_dir(&failures_dir)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(entries.len(), 1, "exactly one failure file must be written");
    let filename = entries[0].file_name();
    let filename = filename.to_string_lossy();
    assert!(
        filename.ends_with("-broken.json"),
        "filename should include source stem: {filename}"
    );

    // Record contents must include the raw response, parse error, and prompt.
    let contents = std::fs::read_to_string(entries[0].path()).unwrap();
    let record: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(record["raw_response"], unsalvageable);
    assert!(record["parse_error"].as_str().unwrap().contains("pages"));
    assert_eq!(record["provider"], "mock");
    assert!(record["prompt_system"].is_string());
    assert!(record["prompt_user_messages"].is_array());
    assert!(record["timestamp"].is_string());

    // No wiki page was written and log.md was not touched.
    assert!(!dir.path().join("wiki/concepts/broken.md").exists());
    assert!(!dir.path().join("log.md").exists());
}

#[tokio::test]
async fn redundant_true_archives_but_skips_pages() {
    let (dir, vault) = empty_test_vault();
    let mock = MockProvider::constant(
        r#"{"summary":"all redundant","pages":[],"log_entry":"covered by existing pages","redundant":true}"#,
    );

    let source = dir.path().join("sources/articles/redundant-source.md");
    std::fs::write(&source, "Source contents.\n").unwrap();

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();

    assert!(report.redundant);
    assert_eq!(report.created, 0);
    assert_eq!(report.updated, 0);
    assert_eq!(report.commit_id.len(), 40);
    assert!(report.source.as_str().starts_with("sources/articles/"));

    let log = std::fs::read_to_string(dir.path().join("log.md")).unwrap();
    assert!(log.contains("[skip] redundant"));

    let wiki = dir.path().join("wiki");
    if wiki.exists() {
        for entry in std::fs::read_dir(&wiki).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let count = std::fs::read_dir(&path).unwrap().count();
                assert_eq!(count, 0, "no wiki pages should be written");
            }
        }
    }

    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    assert!(
        head.message().unwrap().starts_with("[ingest:skip]"),
        "commit message: {:?}",
        head.message()
    );
}

#[tokio::test]
async fn redundant_true_with_nonempty_pages_errors() {
    let (dir, vault) = empty_test_vault();
    let mock = MockProvider::constant(
        r#"{"summary":"oops","pages":[{"action":"create","path":"wiki/concepts/x.md","title":"X","tags":[],"body":"hi\n"}],"log_entry":"x","redundant":true}"#,
    );
    let source = dir.path().join("sources/articles/bad.md");
    std::fs::write(&source, "Body").unwrap();

    let err = ingest::ingest(&vault, &mock, &source).await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("redundant=true"), "msg: {msg}");
    assert!(msg.contains("non-empty pages"), "msg: {msg}");
}

#[tokio::test]
async fn enqueue_drain_dedup_e2e() {
    use scriptorium_core::ingest_queue;
    use scriptorium_core::llm::{
        prompts::{IngestAction, IngestPageAction, IngestPlan},
        CompletionResponse, MockProvider, Usage,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let (dir, vault) = empty_test_vault();
    let sources_dir = dir.path().join("sources/articles");

    let s1 = sources_dir.join("s1.md");
    let s2 = sources_dir.join("s2.md");
    let s3 = sources_dir.join("s3.md");
    std::fs::write(&s1, "Session ID: A\nbody\n").unwrap();
    std::fs::write(&s2, "Session ID: B\nbody\n").unwrap();
    std::fs::write(&s3, "Peak Turn Score: 99\nbody\n").unwrap();

    let u1 = sources_dir.join("u1.md");
    let u2 = sources_dir.join("u2.md");
    std::fs::write(&u1, "Unique alpha content.\n").unwrap();
    std::fs::write(&u2, "Unique beta content.\n").unwrap();

    for src in [&s1, &s2, &s3, &u1, &u2] {
        ingest_queue::enqueue(&vault, src, None).unwrap();
    }
    let hash_path = vault
        .meta_dir()
        .join("source-hashes.txt")
        .into_std_path_buf();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_handler = counter.clone();
    let mock = MockProvider::with_handler(move |_req| {
        let n = counter_handler.fetch_add(1, Ordering::SeqCst);
        let plan = IngestPlan {
            summary: format!("e2e ingest {n}"),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: format!("wiki/concepts/e2e-{n}.md"),
                title: format!("E2E {n}"),
                tags: vec![],
                body: format!("Distinct body {n}.\n"),
                source_quote: None,
                supersedes: vec![],
            }],
            log_entry: format!("e2e {n}"),
            redundant: false,
        };
        Ok(CompletionResponse {
            text: serde_json::to_string(&plan).unwrap(),
            usage: Usage::default(),
            model: "mock-1".into(),
        })
    });

    let cfg = ingest_queue::DrainConfig {
        debounce: Duration::from_secs(0),
        ..Default::default()
    };
    let report = ingest_queue::drain(&vault, &mock, cfg).await.unwrap();

    assert_eq!(report.considered, 5);
    assert_eq!(report.skipped_dup, 2);
    assert_eq!(report.ingested, 3);
    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );
    assert_eq!(
        ingest_queue::list_queued(&vault).unwrap(),
        [] as [ingest_queue::QueueMarker; 0]
    );

    let lines: Vec<String> = std::fs::read_to_string(&hash_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 3, "hash store: {lines:?}");

    let repo = git2::Repository::open(dir.path()).unwrap();
    let mut walk = repo.revwalk().unwrap();
    walk.push_head().unwrap();
    let ingest_commits: usize = walk
        .filter_map(|oid| {
            let oid = oid.ok()?;
            let commit = repo.find_commit(oid).ok()?;
            let msg = commit.message().ok()?.to_string();
            if msg.contains("e2e ingest") {
                Some(())
            } else {
                None
            }
        })
        .count();
    assert_eq!(ingest_commits, 3);
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

/// A long, unique body: well above the near-duplicate gate's 40-word
/// floor, so the gate runs on it.
fn gate_sized_body(tag: &str) -> String {
    let words: Vec<String> = (0..300).map(|i| format!("{tag}{i}")).collect();
    words.join(" ") + "\n"
}

/// A mock that returns a distinct one-page plan per call, plus its call
/// counter, so a test can tell an LLM ingest from a pre-LLM skip.
fn counting_plan_mock() -> (MockProvider, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use scriptorium_core::llm::{CompletionResponse, Usage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let counter = Arc::new(AtomicUsize::new(0));
    let calls = counter.clone();
    let mock = MockProvider::with_handler(move |_req| {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        let plan = IngestPlan {
            summary: format!("gate ingest {n}"),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: format!("wiki/concepts/gate-{n}.md"),
                title: format!("Gate {n}"),
                tags: vec![],
                body: format!("Gate body {n}.\n"),
                source_quote: None,
                supersedes: vec![],
            }],
            log_entry: format!("gate {n}"),
            redundant: false,
        };
        Ok(CompletionResponse {
            text: serde_json::to_string(&plan).unwrap(),
            usage: Usage::default(),
            model: "mock-1".into(),
        })
    });
    (mock, counter)
}

/// Regression: the session hook stages notes in `sources/sessions/`. The
/// near-duplicate gate scanned all of `sources/`, matched each note
/// against itself (similarity 1.00) and reported it as a redundant
/// ingest before any LLM call, so the drain ingested nothing.
#[tokio::test]
async fn drain_ingests_session_note_staged_under_sources() {
    use scriptorium_core::ingest_queue;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let (dir, vault) = empty_test_vault();
    let sessions = dir.path().join("sources/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let note = sessions.join("2026-10-08-session-abcd1234.md");
    std::fs::write(&note, gate_sized_body("note")).unwrap();
    ingest_queue::enqueue(&vault, &note, Some("abcd1234")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let cfg = ingest_queue::DrainConfig {
        debounce: Duration::from_secs(0),
        ..Default::default()
    };
    let report = ingest_queue::drain(&vault, &mock, cfg).await.unwrap();

    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );
    assert_eq!(report.ingested, 1);
    assert_eq!(report.redundant_skips, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the LLM must see the note");
    assert!(dir.path().join("wiki/concepts/gate-0.md").exists());
    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    assert!(head.message().unwrap().contains("gate ingest 0"));
}

/// A file dropped into a category folder by hand (or picked up by
/// `watch`) is not interned yet, so it must not match itself.
#[tokio::test]
async fn near_duplicate_gate_ignores_the_source_itself() {
    use std::sync::atomic::Ordering;

    let (dir, vault) = empty_test_vault();
    let source = dir.path().join("sources/articles/dropped.md");
    std::fs::write(&source, gate_sized_body("dropped")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();

    assert!(!report.redundant, "summary: {}", report.summary);
    assert_eq!(report.created, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Nor may it when its name has the shape of an interned copy, such as a
/// timestamp: a copy is named after its own content's hash.
#[tokio::test]
async fn near_duplicate_gate_ignores_a_timestamped_source_itself() {
    use std::sync::atomic::Ordering;

    let (dir, vault) = empty_test_vault();
    let source = dir.path().join("sources/articles/202610291230-meeting.md");
    std::fs::write(&source, gate_sized_body("meeting")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();

    assert!(!report.redundant, "summary: {}", report.summary);
    assert_eq!(report.created, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Staged files that were never ingested are not existing sources. A
/// near-copy among them (the same session's note from an earlier day)
/// must not make the gate skip the source.
#[tokio::test]
async fn near_duplicate_gate_ignores_staged_copies() {
    use std::sync::atomic::Ordering;

    let (dir, vault) = empty_test_vault();
    let sessions = dir.path().join("sources/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let body = gate_sized_body("staged");
    std::fs::write(sessions.join("2026-10-07-session-abcd1234.md"), &body).unwrap();
    let today = sessions.join("2026-10-08-session-abcd1234.md");
    std::fs::write(&today, format!("{body}one more line\n")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let report = ingest::ingest(&vault, &mock, &today).await.unwrap();

    assert!(!report.redundant, "summary: {}", report.summary);
    assert_eq!(report.created, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// The gate still does its job: a lightly edited copy of an interned
/// source is skipped before any LLM call, and the skip names the
/// interned copy.
#[tokio::test]
async fn near_duplicate_gate_still_skips_copies_of_interned_sources() {
    use std::sync::atomic::Ordering;

    let (dir, vault) = empty_test_vault();
    let outside = TempDir::new().unwrap();
    let original = outside.path().join("report.md");
    let body = gate_sized_body("doc");
    std::fs::write(&original, &body).unwrap();

    let (mock, calls) = counting_plan_mock();
    let first = ingest::ingest(&vault, &mock, &original).await.unwrap();
    assert!(!first.redundant);
    assert!(first.source.as_str().starts_with("sources/articles/"));

    let sessions = dir.path().join("sources/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let copy = sessions.join("2026-10-08-session-feedbeef.md");
    std::fs::write(&copy, format!("{body}one more line\n")).unwrap();
    let second = ingest::ingest(&vault, &mock, &copy).await.unwrap();

    assert!(second.redundant);
    assert_eq!(second.commit_id, "skipped-near-duplicate");
    assert_eq!(second.source, first.source);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no LLM call for the copy");
}

fn page_action(action: IngestAction, path: &str, body: &str) -> IngestPageAction {
    IngestPageAction {
        action,
        path: path.into(),
        title: "Page".into(),
        tags: vec![],
        body: body.into(),
        source_quote: None,
        supersedes: vec![],
    }
}

fn plan_with(pages: Vec<IngestPageAction>) -> MockProvider {
    let plan = IngestPlan {
        summary: "plan".into(),
        pages,
        log_entry: "plan".into(),
        redundant: false,
    };
    MockProvider::constant(serde_json::to_string(&plan).unwrap())
}

/// Regression: an update whose body came back empty from the LLM replaced
/// the page body with nothing (the live vault lost one page this way).
/// The page keeps its body; the new source is still recorded.
#[tokio::test]
async fn ingest_update_with_empty_body_keeps_the_page_body() {
    let (dir, vault) = empty_test_vault();
    let seed = dir.path().join("sources/articles/seed.md");
    std::fs::write(&seed, "seed").unwrap();
    let path = "wiki/concepts/target.md";
    let seeded = plan_with(vec![page_action(
        IngestAction::Create,
        path,
        "Original body.\n",
    )]);
    ingest::ingest(&vault, &seeded, &seed).await.unwrap();

    let source = dir.path().join("sources/articles/blank-update.md");
    std::fs::write(&source, "blank update").unwrap();
    let blank = plan_with(vec![page_action(IngestAction::Update, path, " \n")]);
    let report = ingest::ingest(&vault, &blank, &source).await.unwrap();
    assert_eq!(report.updated, 1);

    let scan = vault.scan().unwrap();
    let page = scan.pages.iter().find(|p| p.path == path).unwrap();
    assert!(
        page.body.contains("Original body."),
        "body: {:?}",
        page.body
    );
    assert_eq!(
        page.frontmatter.sources.len(),
        2,
        "the new source is recorded"
    );
}

/// A create with an empty body is skipped instead of writing an empty
/// page; the rest of the plan still applies.
#[tokio::test]
async fn ingest_skips_a_create_with_an_empty_body() {
    let (dir, vault) = empty_test_vault();
    let source = dir.path().join("sources/articles/mixed.md");
    std::fs::write(&source, "mixed").unwrap();
    let mock = plan_with(vec![
        page_action(IngestAction::Create, "wiki/concepts/empty.md", ""),
        page_action(
            IngestAction::Create,
            "wiki/concepts/full.md",
            "Full body.\n",
        ),
    ]);

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert_eq!(report.created, 1);
    assert!(!dir.path().join("wiki/concepts/empty.md").exists());
    assert!(dir.path().join("wiki/concepts/full.md").exists());
}

/// Regression: a dry run left its interned copy in `sources/`. The real
/// run of the same file then matched that copy in the near-duplicate gate
/// (similarity 1.00) and was skipped, so a dry run followed by a commit
/// always needed `force`.
#[tokio::test]
async fn ingest_after_dry_run_commits() {
    use std::sync::atomic::Ordering;

    let (dir, vault) = empty_test_vault();
    let outside = TempDir::new().unwrap();
    let source = outside.path().join("canvas.md");
    std::fs::write(&source, gate_sized_body("canvas")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let preview = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions {
            dry_run: true,
            hooks: None,
            force: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(preview.commit_id, "dry-run");

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert!(!report.redundant, "summary: {}", report.summary);
    assert_eq!(report.commit_id.len(), 40);
    assert_eq!(report.created, 1);
    assert_eq!(report.source, preview.source);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one LLM call per run");
    let repo = git2::Repository::open(dir.path()).unwrap();
    let tree = repo.head().unwrap().peel_to_tree().unwrap();
    assert!(
        tree.get_path(report.source.as_std_path()).is_ok(),
        "the commit is missing the interned source {}",
        report.source
    );
}

/// The dry-run cleanup removes only the dry run's own copy. After a dry
/// run and a real run of one file, a near-copy of that file is still
/// skipped before any LLM call, in a dry run and in a real run. `force`
/// still ingests it.
#[tokio::test]
async fn near_duplicate_gate_still_skips_another_source_after_dry_run() {
    use std::sync::atomic::Ordering;

    let (_dir, vault) = empty_test_vault();
    let outside = TempDir::new().unwrap();
    let original = outside.path().join("report.md");
    let body = gate_sized_body("kept");
    std::fs::write(&original, &body).unwrap();
    let options = |dry_run, force| ingest::IngestOptions {
        dry_run,
        hooks: None,
        force,
    };

    let (mock, calls) = counting_plan_mock();
    ingest::ingest_with_options(&vault, &mock, &original, options(true, false))
        .await
        .unwrap();
    let first = ingest::ingest(&vault, &mock, &original).await.unwrap();
    assert!(!first.redundant, "summary: {}", first.summary);

    let copy = outside.path().join("report-copy.md");
    std::fs::write(&copy, format!("{body}one more line\n")).unwrap();
    for dry_run in [true, false] {
        let skipped = ingest::ingest_with_options(&vault, &mock, &copy, options(dry_run, false))
            .await
            .unwrap();
        assert!(skipped.redundant, "dry_run={dry_run}: {}", skipped.summary);
        assert_eq!(skipped.commit_id, "skipped-near-duplicate");
        assert_eq!(skipped.source, first.source);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2, "no LLM call for the copy");

    let forced = ingest::ingest_with_options(&vault, &mock, &copy, options(false, true))
        .await
        .unwrap();
    assert!(!forced.redundant, "summary: {}", forced.summary);
    assert_eq!(forced.commit_id.len(), 40);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

/// A dry run removes only a copy it interned itself. A forced dry run of
/// a source that an earlier ingest committed finds the interned copy
/// already there and keeps it.
#[tokio::test]
async fn dry_run_keeps_an_interned_copy_it_did_not_create() {
    let (dir, vault) = empty_test_vault();
    let outside = TempDir::new().unwrap();
    let source = outside.path().join("report.md");
    std::fs::write(&source, gate_sized_body("committed")).unwrap();

    let (mock, _calls) = counting_plan_mock();
    let first = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert_eq!(first.commit_id.len(), 40);

    let preview = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions {
            dry_run: true,
            hooks: None,
            force: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(preview.commit_id, "dry-run");
    assert_eq!(preview.source, first.source);
    assert!(
        dir.path().join(first.source.as_str()).exists(),
        "the dry run removed the committed source {}",
        first.source
    );
}

/// Bulk ingest skips a file whose content is already interned. A bulk dry
/// run left every file interned, so the real run after it skipped them
/// all, even with `--fresh` (which deletes the dry run's checkpoint).
#[tokio::test]
async fn bulk_ingest_after_dry_run_ingests() {
    use scriptorium_core::bulk_ingest::{self, BulkIngestOptions};
    use std::sync::atomic::Ordering;

    let (_dir, vault) = empty_test_vault();
    let inbox = TempDir::new().unwrap();
    std::fs::write(inbox.path().join("a.md"), gate_sized_body("bulka")).unwrap();
    std::fs::write(inbox.path().join("b.md"), gate_sized_body("bulkb")).unwrap();

    let (mock, calls) = counting_plan_mock();
    let dry_run = BulkIngestOptions {
        dry_run: true,
        ..Default::default()
    };
    let preview = bulk_ingest::bulk_ingest(&vault, &mock, inbox.path(), &dry_run, |_, _, _| {})
        .await
        .unwrap();
    assert_eq!(preview.ingested, 2);
    // `--fresh`.
    std::fs::remove_file(vault.meta_dir().join("bulk-ingest-checkpoint.json")).unwrap();

    let report = bulk_ingest::bulk_ingest(
        &vault,
        &mock,
        inbox.path(),
        &BulkIngestOptions::default(),
        |_, _, _| {},
    )
    .await
    .unwrap();
    assert!(report.failed.is_empty(), "failures: {:?}", report.failed);
    assert_eq!(report.skipped_already_interned, 0);
    assert_eq!(report.ingested, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

/// A dry run copies nothing into `sources/`, not even while it waits on
/// the LLM. Other ingests (the drain, a real ingest of the same file) took
/// such a copy for an ingested source: the near-duplicate gate skipped
/// their file as a copy of it, and a real ingest could commit the copy.
#[tokio::test]
async fn dry_run_writes_nothing_to_sources_during_the_llm_call() {
    use scriptorium_core::llm::{CompletionResponse, Usage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (dir, vault) = empty_test_vault();
    let outside = TempDir::new().unwrap();
    let source = outside.path().join("note.md");
    std::fs::write(&source, gate_sized_body("note")).unwrap();

    let articles = dir.path().join("sources/articles");
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    let seen_in_call = seen.clone();
    let mock = MockProvider::with_handler(move |_req| {
        let files = std::fs::read_dir(&articles).unwrap().count();
        seen_in_call.store(files, Ordering::SeqCst);
        let plan = IngestPlan {
            summary: "note".into(),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/note.md".into(),
                title: "Note".into(),
                tags: vec![],
                body: "Note body.\n".into(),
                source_quote: None,
                supersedes: vec![],
            }],
            log_entry: "note".into(),
            redundant: false,
        };
        Ok(CompletionResponse {
            text: serde_json::to_string(&plan).unwrap(),
            usage: Usage::default(),
            model: "mock-1".into(),
        })
    });
    let preview = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions {
            dry_run: true,
            hooks: None,
            force: false,
        },
    )
    .await
    .unwrap();

    assert_eq!(preview.commit_id, "dry-run");
    assert_eq!(
        seen.load(Ordering::SeqCst),
        0,
        "files in sources/articles/ during the dry run's LLM call"
    );
}

/// A dry run whose plan judges the source redundant committed the
/// `[ingest:skip]` record anyway. The real run after it then skipped the
/// file as a near-duplicate of that commit. The dry run now previews the
/// skip, and the real run makes it.
#[tokio::test]
async fn redundant_dry_run_commits_nothing() {
    let (dir, vault) = empty_test_vault();
    let plan = IngestPlan {
        summary: "nothing new".into(),
        pages: vec![],
        log_entry: "redundant".into(),
        redundant: true,
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let outside = TempDir::new().unwrap();
    let source = outside.path().join("known.md");
    std::fs::write(&source, gate_sized_body("known")).unwrap();

    let preview = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions {
            dry_run: true,
            hooks: None,
            force: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(preview.commit_id, "dry-run");
    assert!(preview.redundant);
    assert!(preview
        .dry_run_diff
        .iter()
        .any(|c| c.path.as_str() == "log.md" && c.action == Action::Append));
    assert!(!dir.path().join("log.md").exists());
    assert!(!dir.path().join(preview.source.as_str()).exists());

    let report = ingest::ingest(&vault, &mock, &source).await.unwrap();
    assert!(report.redundant);
    assert_eq!(report.commit_id.len(), 40, "summary: {}", report.summary);
    let repo = git2::Repository::open(dir.path()).unwrap();
    let head = repo.head().unwrap().peel_to_commit().unwrap();
    assert!(head.message().unwrap().starts_with("[ingest:skip]"));
}
