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
/// every reached page, seeds included, as a ranked `SearchHit` list capped
/// at `limit`. Seeds must stay in the list: RRF adds one `1/(k + rank)` term
/// per list a result appears in, so a list without the seeds gives each of
/// their neighbors a term the seeds never get. With `k = 60` that term
/// outweighs the seeds' lead in the retrieval lists, and the best hits sink
/// below their own neighbors.
///
/// PPR decides where the seeds sit among their neighbors, but its scores do
/// not keep the seeds in retrieval order: a well-linked seed outranks a
/// peripheral one. The seed slots are therefore refilled in retrieval order,
/// and the top seed moves to the first slot. The top retrieval hit then gets
/// the largest graph term, so no page can pass it in the final fusion, also
/// when `limit` truncates the list.
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
    let mut seed_rank: HashMap<PageId, usize> = HashMap::new();
    for hit in fused.iter().take(seed_count) {
        let next = seed_rank.len();
        seed_rank.entry(hit.page_id).or_insert(next);
        seeds
            .entry(hit.page_id)
            .and_modify(|s| *s = s.max(hit.score))
            .or_insert(hit.score);
    }

    let mut ranking = personalized_pagerank(graph, &seeds, config);

    // Fill the slots PPR gave to seeds in retrieval order, then move the top
    // seed to the first slot.
    let mut seeds_in_order: Vec<(PageId, f32)> = ranking
        .iter()
        .copied()
        .filter(|(id, _)| seed_rank.contains_key(id))
        .collect();
    seeds_in_order.sort_by_key(|(id, _)| seed_rank[id]);
    let mut next_seed = seeds_in_order.into_iter();
    for entry in &mut ranking {
        if seed_rank.contains_key(&entry.0) {
            if let Some(seed) = next_seed.next() {
                *entry = seed;
            }
        }
    }
    if let Some(first) = ranking
        .iter()
        .position(|(id, _)| seed_rank.contains_key(id))
    {
        ranking[..=first].rotate_right(1);
    }

    // Best (first-seen) chunk per page, for identity-preserving reuse.
    let mut best_chunk: HashMap<PageId, &SearchHit> = HashMap::new();
    for hit in fused {
        best_chunk.entry(hit.page_id).or_insert(hit);
    }

    ranking
        .into_iter()
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
    fn expansion_list_keeps_seeds_ahead_of_neighbors() {
        let a = page("a", "A", "Links [[b]].\n");
        let b = page("b", "B", "One hop out.\n");
        let a_id = a.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, b]);

        let fused = vec![hit(a_id, "chunk from a", 0.5)];
        let list = graph_expansion_list(&graph, &fused, 5, 10, &PprConfig::default());
        assert_eq!(list.len(), 2);
        // The seed leads and reuses its retrieved chunk.
        assert_eq!(list[0].page_id, a_id);
        assert_eq!(list[0].chunk_text, "chunk from a");
        assert_eq!(list[1].page_id, b_id);
        // b has no chunk in the fused list → synthetic hit carries the title.
        assert_eq!(list[1].chunk_text, "B");
        assert_eq!(list[1].page_path.as_deref(), Some("wiki/b.md"));
    }

    #[test]
    fn expansion_list_keeps_seeds_in_retrieval_order() {
        // a is the best retrieval hit but the least-linked seed.
        let a = page("a", "A", "Links [[hub]].\n");
        let hub = page("hub", "Hub", "Links [[b]], [[c]] and [[d]].\n");
        let b = page("b", "B", "Links [[c]] and [[d]].\n");
        let c = page("c", "C", "Leaf.\n");
        let d = page("d", "D", "Leaf.\n");
        let ids = vec![
            a.frontmatter.id,
            hub.frontmatter.id,
            b.frontmatter.id,
            c.frontmatter.id,
            d.frontmatter.id,
        ];
        let graph = LinkGraph::build(&[a, hub, b, c, d]);

        let retrieval: Vec<SearchHit> = ids
            .iter()
            .zip([0.9, 0.8, 0.7, 0.6, 0.5])
            .zip(["ca", "ch", "cb", "cc", "cd"])
            .map(|((id, score), text)| hit(*id, text, score))
            .collect();
        let prelim = crate::search::fusion::rrf_fuse(std::slice::from_ref(&retrieval));

        // Precondition: raw PPR ranks a better-linked seed above a.
        let weights: HashMap<PageId, f32> = prelim.iter().map(|h| (h.page_id, h.score)).collect();
        let raw = personalized_pagerank(&graph, &weights, &PprConfig::default());
        assert_ne!(raw[0].0, ids[0]);

        let list = graph_expansion_list(&graph, &prelim, 5, 20, &PprConfig::default());
        let order: Vec<PageId> = list.iter().map(|h| h.page_id).collect();
        assert_eq!(order, ids, "the graph list must not reorder seeds");
        let fused = crate::search::fusion::rrf_fuse(&[retrieval, list]);
        assert_eq!(fused[0].page_id, ids[0]);

        // A limit below the seed count keeps the best seed, not the best-linked one.
        let short = graph_expansion_list(&graph, &prelim, 5, 1, &PprConfig::default());
        assert_eq!(short.len(), 1);
        assert_eq!(short[0].page_id, ids[0]);
    }

    #[test]
    fn neighbors_keep_their_ppr_place_behind_the_top_seed() {
        // n bridges three seeds, so PPR ranks it above them; only the top
        // seed moves ahead of it.
        let a = page("a", "A", "Links [[n]].\n");
        let b = page("b", "B", "Links [[n]].\n");
        let c = page("c", "C", "Links [[n]].\n");
        let n = page("n", "N", "Bridge page.\n");
        let (a_id, b_id, c_id, n_id) = (
            a.frontmatter.id,
            b.frontmatter.id,
            c.frontmatter.id,
            n.frontmatter.id,
        );
        let graph = LinkGraph::build(&[a, b, c, n]);

        let fused = vec![
            hit(a_id, "chunk a", 0.9),
            hit(b_id, "chunk b", 0.8),
            hit(c_id, "chunk c", 0.7),
        ];
        let list = graph_expansion_list(&graph, &fused, 3, 10, &PprConfig::default());
        let order: Vec<PageId> = list.iter().map(|h| h.page_id).collect();
        assert_eq!(order, vec![a_id, n_id, b_id, c_id]);
    }

    #[test]
    fn bridge_neighbor_cannot_pass_the_top_hit() {
        // n links all five seeds and ranks just below them in retrieval, so
        // PPR puts n first.
        let mut pages: Vec<Page> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| page(s, s, "Links [[n]].\n"))
            .collect();
        let seed_ids: Vec<PageId> = pages.iter().map(|p| p.frontmatter.id).collect();
        let n = page("n", "N", "Bridge page.\n");
        let n_id = n.frontmatter.id;
        pages.push(n);
        let graph = LinkGraph::build(&pages);

        // Vector: a..e, n, fillers. Keyword: seven fillers, then n, e..a.
        let filler = |text: String| hit(PageId::new(), &text, 0.1);
        let mut vector: Vec<SearchHit> = seed_ids.iter().map(|id| hit(*id, "seed", 0.9)).collect();
        vector.push(hit(n_id, "bridge", 0.8));
        vector.extend((0..6).map(|i| filler(format!("vector filler {i}"))));
        let mut keyword: Vec<SearchHit> = (0..7)
            .map(|i| filler(format!("keyword filler {i}")))
            .collect();
        keyword.push(hit(n_id, "bridge", 0.5));
        keyword.extend(seed_ids.iter().rev().map(|id| hit(*id, "seed", 0.4)));

        let prelim = crate::search::fusion::rrf_fuse(&[vector.clone(), keyword.clone()]);
        assert_eq!(prelim[0].page_id, seed_ids[0], "precondition: a leads");
        assert_eq!(prelim[5].page_id, n_id, "precondition: n is sixth");
        let weights: HashMap<PageId, f32> = prelim
            .iter()
            .take(5)
            .map(|h| (h.page_id, h.score))
            .collect();
        let raw = personalized_pagerank(&graph, &weights, &PprConfig::default());
        assert_eq!(raw[0].0, n_id, "precondition: raw PPR ranks n first");

        let list = graph_expansion_list(&graph, &prelim, 5, 20, &PprConfig::default());
        let fused = crate::search::fusion::rrf_fuse(&[vector, keyword, list]);
        assert_eq!(fused[0].page_id, seed_ids[0], "n must not pass the top hit");

        // With room for one page, the slot goes to the top hit, not to n.
        let short = graph_expansion_list(&graph, &prelim, 5, 1, &PprConfig::default());
        let order: Vec<PageId> = short.iter().map(|h| h.page_id).collect();
        assert_eq!(order, vec![seed_ids[0]]);
    }

    #[test]
    fn tied_top_hits_keep_the_preliminary_leader_first() {
        // a and b tie in retrieval and b is better linked. Whichever page RRF
        // ranks first before the graph list must stay first after it.
        let a = page("a", "A", "Leaf.\n");
        let b = page("b", "B", "Links [[x]] and [[y]].\n");
        let x = page("x", "X", "Links [[y]].\n");
        let y = page("y", "Y", "Leaf.\n");
        let (a_id, b_id) = (a.frontmatter.id, b.frontmatter.id);
        let graph = LinkGraph::build(&[a, b, x, y]);

        let vector = vec![hit(a_id, "chunk a", 0.9), hit(b_id, "chunk b", 0.8)];
        let keyword = vec![hit(b_id, "chunk b", 0.9), hit(a_id, "chunk a", 0.8)];
        let prelim = crate::search::fusion::rrf_fuse(&[vector.clone(), keyword.clone()]);
        assert!(
            (prelim[0].score - prelim[1].score).abs() < f32::EPSILON,
            "precondition: a and b tie"
        );

        let list = graph_expansion_list(&graph, &prelim, 2, 20, &PprConfig::default());
        let fused = crate::search::fusion::rrf_fuse(&[vector, keyword, list]);
        assert_eq!(fused[0].page_id, prelim[0].page_id);
    }

    #[test]
    fn top_retrieval_hit_stays_first_after_graph_fusion() {
        // Regression: the graph list used to omit seeds, so a seed's linked
        // neighbor collected an extra RRF term and outranked the seed even
        // when the seed was the top retrieval hit.
        let a = page("a", "A", "Links [[hub]].\n");
        let hub = page("hub", "Hub", "Links [[a]] and [[b]].\n");
        let b = page("b", "B", "Links [[hub]].\n");
        let a_id = a.frontmatter.id;
        let hub_id = hub.frontmatter.id;
        let b_id = b.frontmatter.id;
        let graph = LinkGraph::build(&[a, hub, b]);

        // a is the best retrieval hit; the hub trails far behind.
        let mut retrieval = vec![hit(a_id, "chunk from a", 0.9)];
        retrieval.extend((0..8).map(|i| hit(PageId::new(), &format!("filler {i}"), 0.5)));
        retrieval.push(hit(hub_id, "chunk from hub", 0.1));
        retrieval.push(hit(b_id, "chunk from b", 0.05));

        let prelim = crate::search::fusion::rrf_fuse(std::slice::from_ref(&retrieval));
        let graph_list = graph_expansion_list(&graph, &prelim, 5, 20, &PprConfig::default());
        let fused = crate::search::fusion::rrf_fuse(&[retrieval, graph_list]);
        assert_eq!(fused[0].page_id, a_id, "top retrieval hit must stay first");
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
        assert_eq!(list.len(), 2);
        let b_hit = list
            .iter()
            .find(|h| h.page_id == b_id)
            .expect("b is reached");
        // Reuses the real chunk so RRF merges it with the retrieval lists.
        assert_eq!(b_hit.chunk_text, "existing chunk from b");
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
        assert_eq!(list[0].page_id, a_id, "the seed takes the first slot");

        assert!(graph_expansion_list(&graph, &[], 5, 10, &PprConfig::default()).is_empty());
        assert!(graph_expansion_list(&graph, &fused, 5, 0, &PprConfig::default()).is_empty());
        assert!(graph_expansion_list(&graph, &fused, 0, 10, &PprConfig::default()).is_empty());
    }
}
