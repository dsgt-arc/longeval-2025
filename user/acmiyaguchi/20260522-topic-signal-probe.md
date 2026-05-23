# Topic-signal probe — can LDA topics rank documents?

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

A gate experiment before committing compute to a high-K (e.g. K=200) LDA for
**retrieval ranking**. Hypothesis: if query↔document topic similarity separates
relevant from non-relevant judged docs — and especially if K=20 separates them
better than K=4 — then a finer-grained K is worth training as a ranking
feature. **Result: it does not. Topic cosine ≈ chance at both K, K=20 ≯ K=4.
Do not train K=200 for ranking.** Script: `scripts/topic-signal-probe.py`.

---

## Method

For every judged `(qid, docid, rel)` pair in the qrels (2023-01 train +
2023-08 test, 142,376 pairs after join, 16,836 queries) compute
`cos(theta_query, theta_doc)` using the converged K=4 and K=20 LDA models —
query θ from `lda-infer-queries.py` (non-empty queries only), document θ from
the full-corpus `docTopicDistribution_lda.parquet` (docids `^doc`-stripped to
match qrels, deduped by docid). Then ask whether cosine ranks relevant docs
above non-relevant ones: mean cosine per relevance grade, pooled AUC (rel>0 vs
rel=0), macro per-query AUC.

## Result

| | rel=0 (n=87,102) | rel=1 (n=25,209) | rel=2 (n=30,065) | pooled AUC | macro per-query AUC |
|---|---|---|---|---|---|
| **K=4** | 0.8442 | 0.8502 | 0.8251 | **0.4888** | 0.5185 |
| **K=20** | 0.5426 | 0.5558 | 0.5336 | **0.5005** | 0.5177 |

(AUC 0.5 = no signal; >0.5 = cosine ranks relevant higher.)

## Reading

1. **No discriminative signal.** AUC ≈ 0.5 (chance) at both K; K=4 pooled AUC
   is even marginally below 0.5. Topic cosine cannot separate relevant from
   non-relevant judged docs.
2. **Non-monotonic by grade.** rel=2 (most relevant) has the *lowest* mean
   cosine of the three grades in both models — so topical similarity is, if
   anything, mildly *anti*-correlated with high relevance. Not a usable signal
   in either direction.
3. **K=20 ≯ K=4** (gate fails). Finer topics did not add separation; macro AUC
   is even marginally lower. More, narrower topics of the same bag-of-words
   signal will not rescue it — so **K=200 is not worth training for ranking.**
4. K=4 cosines are uniformly high (~0.84): both queries and docs are dominated
   by topic 3, so everything looks similar (the coarseness manifesting as
   undiscriminating high cosine). K=20 lowers the absolute cosines (~0.54) but
   still yields no separation.

## Why (the mechanism)

Within a query's judged candidate set, documents are **already topically
homogeneous** — they are candidates precisely because they match the query's
subject, so they all look similar to the query and to each other. What
separates relevant from non-relevant inside that set is specific entities,
facts, and freshness — exactly the information a 4–200-dim topic vector
discards. LDA discriminates *across* the collection (see the query↔document
mismatch in [`20260522-query-stats-and-lda.md`](20260522-query-stats-and-lda.md))
but is blind *within* an on-topic candidate set, which is where ranking
happens.

## Conclusion

This is a clean **negative result**: LDA topics do not function as a useful
ranking signal on this dataset, and the coarse→fine (K=4→K=20) trend gives no
reason to expect K=200 to differ. It empirically justifies *not* adding a topic
feature to the BM25→cross-encoder pipeline, and points the gain path to dense
retrieval (Nomic Embed v2, paper future-work) rather than topic overlap. LDA's
value here stays **diagnostic** (collection characterization, drift, the
un-queried ~30% bloat), not a ranking component.

## Hardening (pi-review follow-up)

pi-review flagged three risks; the two cheap, decisive ones were run
(`scripts/topic-signal-probe-harden.py`) and the null **survives both**:

| | dedup: mean θ-stddev among dup docids | cosine macro AUC | Hellinger macro AUC |
|---|---|---|---|
| K=4 | 0.0029 | 0.5186 | 0.5187 |
| K=20 | 0.0063 | 0.5183 | 0.5209 |

1. **Date-blind dedup is harmless.** Of 66,605 judged docids, 72% have
   duplicates (≤9 copies, corpus deduped within-date not globally), but their θ
   are near-identical (mean max-stddev ≈0.003–0.006; thin tail to ~0.64 for the
   few docs whose content actually changed across months). Too small to move a
   66k-docid AUC — the dedup does not explain the null.
2. **Hellinger ≈ cosine.** Re-scoring with the simplex-appropriate Hellinger
   similarity gives the same ≈0.5 AUC at both K. The null is not a wrong-metric
   artifact.
3. **(Not run)** Restricting to the BM25 top-100 candidate set (vs. the full
   qrels pool) needs persisted retrieval lists. Expected to *strengthen* the
   null: within the tighter candidate set, docs are even more topically
   homogeneous, so topics discriminate even less.

Net: the ~0.5 AUC is a robust null, not an artifact of dedup or metric choice.
A consistent whisper of signal remains (macro AUC ~0.518–0.521 > 0.5) but is
negligible for ranking and flat across K.

## Candidate-set rerank (the principled test)

Closing pi-review point #2: instead of the full qrels pool, operate on the
**actual BM25 top-25** — reorder it by topic similarity and measure NDCG@10.
`scripts/topic-rerank-bm25.py`, 2023-01 + 2023-08, 20,848 queries scored.

| ordering | K=4 NDCG@10 | K=20 NDCG@10 |
|---|---|---|
| BM25 baseline | **0.2553** | 0.2553 |
| topic-only rerank | 0.1215 | 0.1208 |
| fusion w=0.1 | 0.2556 | 0.2548 |
| fusion w=0.3 | 0.2506 | 0.2479 |
| fusion w=0.5 | 0.2335 | 0.2215 |
| Kendall τ (BM25 vs topic order) | 0.017 | 0.045 |

- **Topics change the order almost completely** (τ≈0.02–0.05 ≈ uncorrelated with
  BM25) but the new order is no better than random: **topic-only reranking
  halves NDCG** (0.255→0.121).
- **Fusion never improves on BM25** — w=0.1 is flat (+0.0003 / −0.0005, noise),
  every higher weight degrades monotonically. No topic weight beats the baseline.
- K=20 ≈ K=4. This is stronger than the AUC null: not only do topics fail to
  discriminate, *acting* on them destroys the ranking.
- Baseline 0.2553 < the reproduction's 2023-01=0.3127 because this uses strict
  full-qrels IDCG (pytrec_eval) over top-25 pooled across both dates, vs the
  repo's `score_search` join method; all orderings here share the same eval, so
  the comparison is internally clean.

### Caveats

- Judged docs are pooled qrels, not strictly the BM25 top-100; but the
  conclusion ("topic cosine doesn't separate relevant from non-relevant judged
  docs") is exactly the within-candidate-set question and is unaffected.
- Doc θ deduped by docid (corpus is deduped within-date, not globally); same
  text → ~same θ, so this does not change the AUC verdict.
- 7.7% of queries are empty in-vocab bags and excluded (no query θ).

## Artifacts

| What | Path |
|---|---|
| Probe driver | `scripts/topic-signal-probe.py` |
| Query θ (K=4 / K=20) | `/mnt/data/tmp/lda-queries/k{4,20}_query_topics.parquet` |
| Doc θ (full corpus) | `/mnt/data/tmp/lda-k{4,20}-converged/k{4,20}/docTopicDistribution_lda.parquet` |
