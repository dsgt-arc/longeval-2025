# Distribution of the BM25 top-k retrieved documents

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

What does BM25 actually pull back, and does it inherit the corpus's topic skew?
Analysis over the BM25 **top-100** for 2023-01 + 2023-08 (23,191 queries,
~2.3M retrieved rows). Script: `scripts/retrieved-dist.py`. Companion to the
topic-signal probe ([`20260522-topic-signal-probe.md`](20260522-topic-signal-probe.md))
and the query/LDA mismatch ([`20260522-query-stats-and-lda.md`](20260522-query-stats-and-lda.md)).

---

## 1. Topic mix — corpus vs query vs retrieved (K=4)

| topic | theme | corpus | query | **retrieved** |
|---|---|---|---|---|
| 0 | English media / film | 0.252 | 0.091 | **0.048** |
| 1 | French travel / hotels | 0.104 | 0.233 | 0.163 |
| 2 | pharmacy spam | 0.041 | 0.037 | **0.024** |
| 3 | French general / admin | 0.603 | 0.639 | **0.765** |

**BM25 filters the corpus bloat rather than inheriting it.** The retrieved set
suppresses English-media (corpus 0.252 → retrieved 0.048, *below* even the query
rate) and pharmacy spam (0.041 → 0.024), and concentrates into French-general
content (0.603 → 0.765). French lexical matching naturally avoids the
English/spam documents that dominate the index, so the ~30% un-queried bloat is
largely **never retrieved** — the corpus skew does not directly poison
retrieval.

**But BM25 leans too generic and under-serves specific intent.** Retrieved is
*more* topic-3-concentrated than the queries themselves (0.765 vs 0.639), while
travel (topic 1) drops from query demand 0.233 to retrieved 0.163. BM25
over-surfaces generic French content and under-delivers the specific
travel/local intent users have — a concrete gap a better retriever could close.

### K=20 (refines the same story)

- English-filler topic 8 (corpus 0.178) → retrieved 0.031; general topic 16
  (0.211) → 0.197 preserved.
- e-commerce topic 14 (corpus 0.132) → **0.223 over-retrieved**; admin topic 13
  (0.080) → 0.146 over-retrieved.
- geography topic 7 (query 0.103) → **0.060 under-retrieved** (mirrors travel).
- every spam/boilerplate topic (6, 9, 11, 12, 15, 17-pharmacy, 18) collapses to
  ~0.002–0.04 retrieved.

## 2. Score / rank distribution

- hits per query: median 85 (many queries match <100 docs).
- score percentiles (all rows): p1=2.75, p25=6.3, p50=7.45, p75=9.12, p99=15.5.
- smooth decay by rank: rank-1 mean **10.44**, rank-5 9.39, rank-10 8.82,
  rank-25 8.04, rank-50 7.46.

## 3. Positive control — pairwise AUC for relevance (retrieved ∩ judged, n=75,469)

| signal | AUC (rel>0 vs rel=0) |
|---|---|
| BM25 score | 0.515 (K=4) / 0.515 (K=20) |
| topic cosine | 0.497 / 0.507 |

**Honest surprise:** raw BM25 score is itself only a *weak* pairwise
discriminator within the already-retrieved judged set (AUC 0.515) — barely above
the topic cosine. Among docs BM25 retrieved, score doesn't monotonically track
assessor relevance, because they all scored high enough to be retrieved. BM25's
ranking quality comes from **top-rank precision + recall** (which NDCG@10
rewards), not a high global pairwise AUC. This does not rescue topics (still
≈0.50), and it explains why the **cross-encoder reranker earns its +0.05** in
the paper: it supplies the within-candidate discrimination that neither BM25
score nor LDA topics provide.

## Takeaways

1. BM25 lexical matching already does the corpus-filtering job that topic-based
   pruning was proposed for — the un-queried English/spam bloat is mostly not
   retrieved. So topic-filtering the index would be redundant for quality
   (though still possibly useful for index *size*).
2. The real retrieval gap is **specific intent under-served** (travel/geography),
   not bloat — pointing at semantic/dense retrieval, not topics.
3. Pairwise AUC is a poor proxy for ranking quality here; future relevance
   experiments should report NDCG/MAP over the candidate set, not pairwise AUC.

### Caveats

- "hits/query max=200" is an artifact of pooling a qid shared across the two
  dates; it doesn't affect the topic-mix or AUC numbers (computed over
  (qid,docid) pairs / retrieved∩judged).
- rank-100 mean is small-n (n=93, only queries reaching full depth) — ignore.
- doc θ deduped by docid (near-identical across copies, verified in the probe).

## Artifacts

| What | Path |
|---|---|
| Driver | `scripts/retrieved-dist.py` |
| BM25 indexes | `/mnt/data/tmp/longeval-bm25-test/index_trec/date=*` |
| Doc θ / query θ | `/mnt/data/tmp/lda-k{4,20}-converged/.../docTopicDistribution_lda.parquet`, `/mnt/data/tmp/lda-queries/` |
