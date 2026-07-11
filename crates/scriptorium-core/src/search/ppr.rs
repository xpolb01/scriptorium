//! Personalized `PageRank` (PPR) over the vault link graph.
//!
//! Multi-hop graph expansion for retrieval: seed pages (the top hybrid
//! search results, weighted by their fused scores) become the restart
//! vector of a random walk over wikilink edges — forward links and
//! backlinks alike. The stationary distribution ranks every reachable page
//! by graph proximity to the seed set: direct neighbors of strong seeds
//! rank above distant or weakly-connected pages, and multi-hop relevance
//! decays geometrically with distance.
//!
//! The restart mechanism is what makes it *personalized*: with probability
//! `1 - damping` the walker teleports back to a seed page instead of
//! following an edge, so probability mass stays concentrated around the
//! seed neighborhood rather than accumulating on global hubs.
//!
//! Reference: `HippoRAG` (arXiv 2405.14831, 2502.14802) uses seeded PPR over
//! a knowledge graph as its retrieval backbone. `damping = 0.5` follows its
//! setup — stronger locality than the classic 0.85 used for whole-web
//! ranking, appropriate when the walk starts from query-relevant seeds.

use std::collections::{BTreeSet, HashMap};

use crate::embed::SearchHit;
use crate::vault::{LinkGraph, PageId, Resolution};

/// Tuning knobs for the power iteration.
#[derive(Debug, Clone)]
pub struct PprConfig {
    /// Probability of following an edge (vs. restarting at a seed).
    /// Lower values keep mass closer to the seeds.
    pub damping: f32,
    /// Upper bound on power-iteration rounds. With damping 0.5 the error
    /// halves each round, so 20 rounds ≈ 1e-6 residual.
    pub max_iterations: usize,
    /// L1 convergence threshold; iteration stops early once the
    /// distribution moves less than this between rounds.
    pub epsilon: f32,
}

impl Default for PprConfig {
    fn default() -> Self {
        Self {
            damping: 0.5,
            max_iterations: 20,
            epsilon: 1e-6,
        }
    }
}

/// Run power-iteration PPR over the link graph.
///
/// `seeds` maps page → restart weight; weights need not be normalized.
/// Seeds with non-positive weight or unknown to the graph are ignored.
/// Propagation treats the graph as undirected: each page's neighbors are
/// the union of its resolved forward links and its backlinks (self-links
/// and duplicate edges collapse). Pages with no neighbors are dangling;
/// their mass returns to the restart vector so the distribution stays a
/// probability distribution.
///
/// Returns `(page, score)` pairs for every page with positive mass, sorted
/// by score descending (ties broken by `PageId` for determinism). Empty
/// when the graph or the effective seed set is empty.
pub fn personalized_pagerank<S: std::hash::BuildHasher>(
    graph: &LinkGraph,
    seeds: &HashMap<PageId, f32, S>,
    config: &PprConfig,
) -> Vec<(PageId, f32)> {
    let ids: Vec<PageId> = graph.pages().map(|p| p.id).collect();
    let n = ids.len();
    if n == 0 {
        return Vec::new();
    }
    let index: HashMap<PageId, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();

    // Restart vector: positive-weight seeds present in the graph, normalized.
    let mut restart = vec![0.0f32; n];
    let mut total = 0.0f32;
    for (id, weight) in seeds {
        if *weight > 0.0 {
            if let Some(&i) = index.get(id) {
                restart[i] += *weight;
                total += *weight;
            }
        }
    }
    if total <= 0.0 {
        return Vec::new();
    }
    for r in &mut restart {
        *r /= total;
    }

    // Undirected adjacency: resolved forward links ∪ backlinks, deduped.
    let mut neighbors: Vec<Vec<usize>> = Vec::with_capacity(n);
    for (i, id) in ids.iter().enumerate() {
        let mut set: BTreeSet<usize> = BTreeSet::new();
        for link_ref in graph.forward(*id).unwrap_or_default() {
            if let Resolution::Resolved(target) = &link_ref.resolved {
                if let Some(&j) = index.get(target) {
                    if j != i {
                        set.insert(j);
                    }
                }
            }
        }
        for back in graph.backlinks(*id) {
            if let Some(&j) = index.get(&back) {
                if j != i {
                    set.insert(j);
                }
            }
        }
        neighbors.push(set.into_iter().collect());
    }

    // Power iteration: p' = (1-d)·r + d·(Mᵀp + dangling·r), where M spreads
    // each node's mass uniformly over its neighbors and dangling nodes
    // restart.
    let mut p = restart.clone();
    for _ in 0..config.max_iterations {
        let mut next = vec![0.0f32; n];
        let mut dangling = 0.0f32;
        for (i, adj) in neighbors.iter().enumerate() {
            if adj.is_empty() {
                dangling += p[i];
                continue;
            }
            #[allow(clippy::cast_precision_loss)]
            let share = config.damping * p[i] / adj.len() as f32;
            for &j in adj {
                next[j] += share;
            }
        }
        let restart_mass = (1.0 - config.damping) + config.damping * dangling;
        for (nx, r) in next.iter_mut().zip(&restart) {
            *nx += restart_mass * r;
        }
        let delta: f32 = next.iter().zip(&p).map(|(a, b)| (a - b).abs()).sum();
        p = next;
        if delta < config.epsilon {
            break;
        }
    }

    let mut ranked: Vec<(PageId, f32)> = ids
        .into_iter()
        .zip(p)
        .filter(|(_, score)| *score > 0.0)
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked
}

/// Build the graph-expansion result list for RRF fusion.
///
/// Seeds the walk with the pages behind the top `seed_count` fused chunks
/// (weight = best fused score per page), runs PPR, and emits the ranking of
/// **non-seed** pages as a ranked `SearchHit` list, capped at `limit`. Seed
/// pages are omitted because they already carry the retrieval evidence this
/// list is fused against — re-emitting them would double-count it, while
/// their neighbors are the new information a graph hop contributes.
///
/// Pages that already have a chunk in `fused` reuse their best-ranked chunk
/// (so RRF merges the graph contribution into the same logical result);
/// pages known only through the graph get a synthetic hit carrying the page
/// title.
pub fn graph_expansion_list(
    graph: &LinkGraph,
    fused: &[SearchHit],
    seed_count: usize,
    limit: usize,
    config: &PprConfig,
) -> Vec<SearchHit> {
    if limit == 0 || seed_count == 0 || fused.is_empty() {
        return Vec::new();
    }

    // Seed weights: best fused score per page over the top `seed_count`
    // chunks. `fused` is sorted best-first, so the first chunk seen for a
    // page is its best.
    let mut seeds: HashMap<PageId, f32> = HashMap::new();
    for hit in fused.iter().take(seed_count) {
        seeds
            .entry(hit.page_id)
            .and_modify(|s| *s = s.max(hit.score))
            .or_insert(hit.score);
    }

    let ranking = personalized_pagerank(graph, &seeds, config);

    // Best (first-seen) chunk per page, for identity-preserving reuse.
    let mut best_chunk: HashMap<PageId, &SearchHit> = HashMap::new();
    for hit in fused {
        best_chunk.entry(hit.page_id).or_insert(hit);
    }

    ranking
        .into_iter()
        .filter(|(id, _)| !seeds.contains_key(id))
        .take(limit)
        .map(|(id, score)| match best_chunk.get(&id) {
            Some(hit) => {
                let mut hit = (*hit).clone();
                hit.score = score;
                hit
            }
            None => SearchHit {
                page_id: id,
                chunk_idx: 0,
                heading: None,
                chunk_text: graph
                    .page_info(id)
                    .map(|p| p.title.clone())
                    .unwrap_or_default(),
                score,
                page_path: graph.page_info(id).map(|p| p.path.to_string()),
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, Page};
    use camino::Utf8PathBuf;
    use chrono::TimeZone;
    use std::collections::BTreeMap;

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

    fn score_of(ranking: &[(PageId, f32)], id: PageId) -> Option<f32> {
        ranking.iter().find(|(p, _)| *p == id).map(|(_, s)| *s)
    }

    fn seed(id: PageId) -> HashMap<PageId, f32> {
        HashMap::from([(id, 1.0)])
    }

    #[test]
    fn seed_page_ranks_itself_highest() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "Links [[c]].\n");
        let c = page("c", "C", "End of chain.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let c_id = c.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, c]);

        let ranking = personalized_pagerank(&graph, &seed(a_id), &PprConfig::default());
        assert_eq!(ranking[0].0, a_id, "seed must rank first");
        // Mass decays with distance from the seed.
        assert!(score_of(&ranking, b_id).unwrap() > score_of(&ranking, c_id).unwrap());
    }

    #[test]
    fn two_hop_neighbor_outranks_disconnected_page() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "Links [[c]].\n");
        let c = page("c", "C", "Two hops from a.\n");
        let d = page("d", "D", "Disconnected island.\n");
        let a_id = a.frontmatter.id;
        let c_id = c.frontmatter.id;
        let d_id = d.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, c, d]);

        let ranking = personalized_pagerank(&graph, &seed(a_id), &PprConfig::default());
        let c_score = score_of(&ranking, c_id).expect("two-hop neighbor must receive mass");
        assert!(c_score > 0.0);
        assert!(
            score_of(&ranking, d_id).is_none(),
            "disconnected page must receive no mass"
        );
    }

    #[test]
    fn backlinks_propagate_mass() {
        // b links to a; seeding a must still reach b (undirected walk).
        let a = page("a", "A", "No outbound links.\n");
        let b = page("b", "B", "Links [[a]].\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, b]);

        let ranking = personalized_pagerank(&graph, &seed(a_id), &PprConfig::default());
        assert_eq!(ranking[0].0, a_id);
        assert!(score_of(&ranking, b_id).unwrap() > 0.0);
    }

    #[test]
    fn heavier_seed_lifts_its_neighborhood() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "Neighbor of a.\n");
        let c = page("c", "C", "Links [[d]].\n");
        let d = page("d", "D", "Neighbor of c.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let c_id = c.frontmatter.id;
        let d_id = d.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, c, d]);

        let seeds = HashMap::from([(a_id, 0.9), (c_id, 0.1)]);
        let ranking = personalized_pagerank(&graph, &seeds, &PprConfig::default());
        assert!(score_of(&ranking, b_id).unwrap() > score_of(&ranking, d_id).unwrap());
    }

    #[test]
    fn empty_graph_returns_empty() {
        let graph = LinkGraph::build(&[]);
        let ranking = personalized_pagerank(&graph, &seed(PageId::new()), &PprConfig::default());
        assert!(ranking.is_empty());
    }

    #[test]
    fn empty_or_unknown_seeds_return_empty() {
        let a = page("a", "A", "Body.\n");
        let graph = LinkGraph::build(&[a]);
        assert!(personalized_pagerank(&graph, &HashMap::new(), &PprConfig::default()).is_empty());
        // A seed the graph has never heard of contributes nothing.
        assert!(
            personalized_pagerank(&graph, &seed(PageId::new()), &PprConfig::default()).is_empty()
        );
    }

    #[test]
    fn dangling_seed_keeps_distribution_normalized() {
        // d has no links at all: its mass must restart, not leak.
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "Neighbor.\n");
        let d = page("d", "D", "Isolated.\n");
        let a_id = a.frontmatter.id;
        let d_id = d.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, d]);

        let seeds = HashMap::from([(a_id, 1.0), (d_id, 1.0)]);
        let ranking = personalized_pagerank(&graph, &seeds, &PprConfig::default());
        let total: f32 = ranking.iter().map(|(_, s)| s).sum();
        assert!(
            (total - 1.0).abs() < 1e-3,
            "mass must be conserved, got {total}"
        );
        assert!(ranking.iter().all(|(_, s)| s.is_finite()));
        assert!(score_of(&ranking, d_id).unwrap() > 0.0);
    }

    #[test]
    fn self_links_do_not_trap_mass() {
        let a = page("a", "A", "Links [[a]] and [[b]].\n");
        let b = page("b", "B", "Neighbor.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, b]);

        let ranking = personalized_pagerank(&graph, &seed(a_id), &PprConfig::default());
        // The self-edge is dropped, so b receives the full damped share.
        assert!(score_of(&ranking, b_id).unwrap() > 0.0);
        let total: f32 = ranking.iter().map(|(_, s)| s).sum();
        assert!((total - 1.0).abs() < 1e-3);
    }

    // --- graph_expansion_list ---

    fn hit(page_id: PageId, text: &str, score: f32) -> SearchHit {
        SearchHit {
            page_id,
            chunk_idx: 0,
            heading: None,
            chunk_text: text.to_string(),
            score,
            page_path: None,
        }
    }

    #[test]
    fn expansion_list_surfaces_neighbors_but_not_seeds() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "One hop out.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, b]);

        let fused = vec![hit(a_id, "chunk from a", 0.5)];
        let list = graph_expansion_list(&graph, &fused, 5, 10, &PprConfig::default());
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].page_id, b_id);
        // b has no chunk in the fused list → synthetic hit carries the title.
        assert_eq!(list[0].chunk_text, "B");
        assert_eq!(list[0].page_path.as_deref(), Some("wiki/b.md"));
    }

    #[test]
    fn expansion_list_reuses_existing_chunk_for_known_pages() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "One hop out.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, b]);

        // b has a (low-ranked) chunk in the fused list, outside the seed set.
        let fused = vec![
            hit(a_id, "chunk from a", 0.5),
            hit(b_id, "existing chunk from b", 0.01),
        ];
        let list = graph_expansion_list(&graph, &fused, 1, 10, &PprConfig::default());
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].page_id, b_id);
        // Reuses the real chunk so RRF merges it with the retrieval lists.
        assert_eq!(list[0].chunk_text, "existing chunk from b");
    }

    #[test]
    fn expansion_list_respects_limit_and_empty_input() {
        let a = page("a", "A", "Links [[b]] and [[c]].\n");
        let b = page("b", "B", "Neighbor.\n");
        let c = page("c", "C", "Neighbor.\n");
        let a_id = a.frontmatter.id;
        let graph = LinkGraph::build(&[a, b, c]);

        let fused = vec![hit(a_id, "chunk from a", 0.5)];
        let list = graph_expansion_list(&graph, &fused, 5, 1, &PprConfig::default());
        assert_eq!(list.len(), 1, "limit must cap the expansion list");

        assert!(graph_expansion_list(&graph, &[], 5, 10, &PprConfig::default()).is_empty());
        assert!(graph_expansion_list(&graph, &fused, 5, 0, &PprConfig::default()).is_empty());
        assert!(graph_expansion_list(&graph, &fused, 0, 10, &PprConfig::default()).is_empty());
    }
}
