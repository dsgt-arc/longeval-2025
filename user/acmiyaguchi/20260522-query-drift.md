# Per-month query-set churn + query topic drift

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

Does each month present a different set of queries, and does the query *topic*
mix drift over time? Since each `qid`'s text (hence its LDA topic) is fixed
across months, any monthly topic drift is driven purely by *which* queries
appear. Script: `scripts/query-drift.py`. Companion to
[`20260522-query-stats-and-lda.md`](20260522-query-stats-and-lda.md).

---

## qid is a globally stable identifier (validity check)

train qids = 53,908 · test qids = 30,201 · shared = 16,287 → 67,822 unique.
Across all 16,287 qids appearing in *both* splits, query text **mismatch = 0**;
0 train qids have >1 distinct text across months. So one qid = one query string
everywhere, and qid-set operations below are meaningful (qid *is* query
identity). 16,287 queries (24%) persist across the train→test boundary.

## 1. Query-set churn — high, with one sharp break

Months each unique qid appears in:

| appears in … | # qids | share |
|---|---|---|
| exactly 1 month | 19,327 | 28.5% |
| ≤3 months | ~53,800 | 79% |
| all 15 months | 607 | 0.9% |

So queries are mostly short-lived (28.5% one-offs) with a tiny persistent core
(0.9%). The turnover is **not uniform** — there's a near-total break at 2022-09:

| transition | Jaccard | new this month | sizes |
|---|---|---|---|
| 2022-06 → 07 | 0.89 | 7.6% | 24.7k → 25.5k |
| 2022-07 → 08 | 0.82 | 14.1% | 25.5k → 28.3k |
| **2022-08 → 09** | **0.13** | **46.3%** | **28.3k → 7.8k** |
| 2022-09 → 10 | 0.26 | 65.7% | 7.8k → 12.2k |
| 2022-10 → 11 | 0.44 | 44.2% | 12.2k → 15.0k |
| 2022-11 → 12 | 0.46 | 37.9% | 15.0k → 15.4k |
| 2022-12 → 2023-01 | 0.43 | 41.5% | 15.4k → 16.0k |
| 2023-01 → 02 | 0.33 | 25.4% | 16.0k → 8.0k |
| 2023-02 → 03 | 0.25 | 51.3% | 8.0k → 5.7k |
| 2023-03 → 04 | 0.24 | 72.7% | 5.7k → 14.5k |
| 2023-04 → 05 | 0.36 | 40.7% | 14.5k → 11.7k |
| 2023-05 → 06 | 0.28 | 48.3% | 11.7k → 8.8k |
| 2023-06 → 07 | 0.36 | 50.2% | 8.8k → 9.8k |
| 2023-07 → 08 | 0.42 | 47.9% | 9.8k → 12.9k |

Before 2022-09 consecutive months share ~85% of queries; at 2022-08→09 the set
turns over almost completely (Jaccard 0.13 ≈ 87% disjoint) **and** volume
collapses 28k → 7.8k. That is a collection/methodology change point — the same
~2022-09 spot where the BM25 NDCG curve has its regime change.

## 2. Query topic mix is stable despite the churn (K=4)

Per-month mean θ over that month's non-empty queries (renormalized):

| month | n_q | t0 (Eng media) | t1 (travel) | t2 (spam) | t3 (Fr gen) |
|---|---|---|---|---|---|
| 2022-06 | 22,110 | 0.117 | 0.228 | 0.038 | 0.617 |
| 2022-07 | 22,878 | 0.118 | 0.228 | 0.038 | 0.616 |
| 2022-08 | 25,530 | 0.117 | 0.227 | 0.037 | 0.619 |
| 2022-09 | 7,382 | 0.107 | 0.188 | 0.033 | 0.672 |
| 2022-10 | 11,297 | 0.071 | 0.223 | 0.036 | 0.670 |
| 2022-11 | 13,948 | 0.071 | 0.218 | 0.036 | 0.676 |
| 2022-12 | 14,251 | 0.069 | 0.215 | 0.037 | 0.678 |
| 2023-01 | 14,870 | 0.069 | 0.218 | 0.037 | 0.676 |
| 2023-02 | 7,357 | 0.069 | 0.220 | 0.036 | 0.675 |
| 2023-03 | 5,194 | 0.070 | 0.215 | 0.036 | 0.679 |
| 2023-04 | 13,501 | 0.068 | 0.233 | 0.036 | 0.663 |
| 2023-05 | 10,785 | 0.069 | 0.236 | 0.036 | 0.659 |
| 2023-06 | 8,120 | 0.069 | 0.231 | 0.036 | 0.664 |
| 2023-07 | 9,076 | 0.068 | 0.255 | 0.036 | 0.640 |
| 2023-08 | 11,986 | 0.069 | 0.260 | 0.036 | 0.635 |

Two real signals against an otherwise flat mix:

1. **A discrete step at 2022-09/10**: English-media (topic 0) **halves**
   (0.117 → 0.070) and stays there permanently. The query stream shifts away
   from English-media content right at the regime break.
2. **A summer-2023 travel bump**: travel (topic 1) climbs 0.233 → 0.260 across
   May→Aug (with topic 3 ceding), a plausible seasonal travel-query signal.

So the specific queries churn heavily, but the *thematic* composition is
remarkably stable — different queries, same topic mix.

## Caveat — query drift ≠ document drift (different events)

This query-topic step is at **2022-09/10** (tracking the query-set break and the
NDCG regime change). The *document* topic drift established earlier is a
**Dec-2022** step (see [`20260518-lda-k-selection.md`](20260518-lda-k-selection.md)).
They are not the same event and should not be conflated: the query side tracks
the 2022-09 collection break, the document side has its own later step.

## Artifacts

| What | Path |
|---|---|
| Driver | `scripts/query-drift.py` |
| Query θ (K=4) | `/mnt/data/tmp/lda-queries/k4_query_topics.parquet` |
