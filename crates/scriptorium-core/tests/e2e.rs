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
use scriptorium_core::vault::{LinkGraph, Vault};
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
        }],
        log_entry: "ingested test-source.md".into(),
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
        }],
        log_entry: "seed".into(),
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
        }],
        log_entry: "updated target".into(),
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
async fn ingest_aborts_when_plan_introduces_broken_link() {
    let (dir, vault) = empty_test_vault();

    // Plan references a non-existent page — validation should block the commit.
    let plan = IngestPlan {
        summary: "bad".into(),
        pages: vec![IngestPageAction {
            action: IngestAction::Create,
            path: "wiki/concepts/broken.md".into(),
            title: "Broken".into(),
            tags: vec![],
            body: "Linking to [[ghost-page]].\n".into(),
        }],
        log_entry: "bad".into(),
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/bad.md");
    std::fs::write(&source, "bad").unwrap();

    let err = ingest::ingest(&vault, &mock, &source).await.unwrap_err();
    assert!(err.to_string().contains("broken wikilink"));
    // Nothing was written.
    assert!(!dir.path().join("wiki/concepts/broken.md").exists());
    assert!(!dir.path().join("log.md").exists());
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
        }],
        log_entry: "would ingest".into(),
    };
    let mock = MockProvider::constant(serde_json::to_string(&plan).unwrap());
    let source = dir.path().join("sources/articles/src.md");
    std::fs::write(&source, "source content").unwrap();

    let report = ingest::ingest_with_options(
        &vault,
        &mock,
        &source,
        ingest::IngestOptions { dry_run: true },
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
    // The source file WAS interned — that's intentional (dry run still tries
    // the real copy so the LLM sees the real path).
    assert!(report.source.as_str().starts_with("sources/articles/"));
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
        }],
        log_entry: "u".into(),
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
        }],
        log_entry: "seed".into(),
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
        }],
        log_entry: "seed".into(),
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
        }],
        log_entry: "ingested source".into(),
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
/// absolute TempDir path, but the production failure happened when the
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
        }],
        log_entry: "relative".into(),
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
        }],
        log_entry: "ingested embedme".into(),
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
        }],
        log_entry: "seed".into(),
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
        }],
        log_entry: "a".into(),
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
        }],
        log_entry: "b".into(),
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
