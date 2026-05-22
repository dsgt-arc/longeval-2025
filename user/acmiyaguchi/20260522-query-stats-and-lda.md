# Query-set statistics + LDA topic inference on queries

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

Two query-side analyses for the CLEF Working Notes
(`../longeval-2025-working-notes`): (1) a query-statistics field the dataset
section currently lacks, and (2) applying the converged K=4 / K=20 LDA models
(trained on documents) to the query set, which surfaces a query↔document topic
mismatch. Scripts: `scripts/query-stats.py`, `scripts/lda-infer-queries.py`.

---

## 1. Query statistics

LongEval-Web French queries are short keyword queries: **~3 words / ~5.8
tiktoken tokens / ~20 chars**. Tokenized with whitespace + `cl100k_base`
(tiktoken), matching the document-stats methodology.

| date | split | queries | avg words | avg tokens | avg chars |
|---|---|---|---|---|---|
| 2022-06 | train | 24,651 | 2.90 | 5.36 | 19.1 |
| 2022-07 | train | 25,470 | 2.90 | 5.37 | 19.2 |
| 2022-08 | train | 28,271 | 2.94 | 5.42 | 19.4 |
| 2022-09 | train | 7,838 | 3.13 | 5.69 | 20.9 |
| 2022-10 | train | 12,204 | 3.14 | 5.97 | 20.9 |
| 2022-11 | train | 15,003 | 3.17 | 5.99 | 21.0 |
| 2022-12 | train | 15,351 | 3.18 | 6.00 | 20.9 |
| 2023-01 | train | 16,007 | 3.21 | 6.07 | 21.3 |
| 2023-02 | train | 7,981 | 3.13 | 5.93 | 20.8 |
| 2023-03 | test | 5,685 | 3.11 | 5.83 | 20.7 |
| 2023-04 | test | 14,534 | 3.22 | 6.10 | 21.3 |
| 2023-05 | test | 11,655 | 3.16 | 5.98 | 21.0 |
| 2023-06 | test | 8,758 | 3.24 | 6.08 | 21.5 |
| 2023-07 | test | 9,846 | 3.10 | 5.88 | 20.6 |
| 2023-08 | test | 12,938 | 3.15 | 5.96 | 20.9 |
| **pooled (instances)** | — | **216,192** | **3.07** | 5.76 | 20.4 |
| **unique (distinct qids)** | — | **67,822** | **3.21** | 6.00 | 21.3 |

- Same `qid` recurs across months with identical text; the pooled mean is
  slightly lower than the unique mean because the high-volume early months
  (2022-06/07/08, ~2.9 words) are over-weighted.
- Mild upward drift in length over time (2.90 → ~3.2), tracking the corpus
  regime change around 2022-09 also seen in the NDCG curve.
- Word-count distribution (unique): 2-word 18.1k, 3-word 23.8k, 4-word 13.6k;
  ~1% exceed 6 words.
- **Two degenerate "queries"** (document text in the query field):
  `qid 75100` (2023-01, 592 words, a 19th-c. book quote) and `qid 75200`
  (2023-06, 876 words, a law-study summary). 2 of 67,822 — negligible, but
  worth a filter/footnote.

JSON: `/mnt/data/tmp/longeval-query-stats.json`.

## 2. LDA topic inference on queries

Applied the **frozen** document feature space (Lucene-FR analyzer → NLTK stop
stems → len≥3/non-numeric filter → NPMI phrases → CountVectorizer 20k vocab)
and each converged LDA model to the 67,822 unique queries. **5,198 queries
(7.7%)** reduce to an empty in-vocab bag and are excluded from the
dominant-topic histograms below.

**Why queries go empty — vocab pruning, not tokenization.** Queries are
tokenized with the *same* chain documents were for LDA (same frozen Lucene-FR
analyzer, phrases, and 20k vocab). The empties are caused by LDA's `min_df=50`
vocab cap dropping rare terms — the empty queries are proper nouns / rare words
(`ibis beauvais`, `gigi clozeau`, `petit ateau`) that appear in <50 documents
and so aren't in the LDA vocab. Note this LDA pipeline is **not** the BM25
retrieval pipeline: both share the Lucene-FR base analyzer, but LDA adds the
NLTK pass, len≥3/numeric filter, NPMI phrases, and vocab pruning that BM25's
full index does not. Those same empty queries are fully indexed and retrievable
under BM25.

**What "empty" yields.** Spark's `LocalLDAModel.transform` returns a **zero
vector** `[0,…,0]` for a doc with no in-vocab terms (it short-circuits empty
docs) — it does *not* fall back to the Dirichlet prior α. So empties deflate the
all-query mean θ uniformly (it sums to 0.923 = 1 − 0.077); the headline query θ
below is the **non-empty renormalized** mean.

### K=4 — query topic mix vs. document topic mix

n_docs-weighted corpus document mean θ vs. mean query θ:

n_docs-weighted corpus document mean θ vs. non-empty-renormalized mean query θ:

| Topic | Theme (top words) | Doc θ | Query θ | Dominant (queries) |
|---|---|---|---|---|
| 0 | English media / film / awards (Eurovision, Johnny Depp, US) | 0.252 | 0.091 | 5.5% |
| 1 | French hotels / travel / real-estate (hôtel, nuit, réservation) | 0.104 | 0.233 | 24.5% |
| 2 | Online-pharmacy spam (viagra, cialis, pharmacy) | 0.041 | 0.037 | 0.6% |
| 3 | French general web / admin / society (site web, produit, travail) | 0.602 | 0.639 | 69.4% |

(All-query mean incl. empties: 0.084 / 0.215 / 0.034 / 0.590, sums to 0.923.)

**Finding — query↔document topic mismatch.** The corpus is heavily skewed
toward English media/film content (topic 0, **25% of documents but only 8% of
queries**), while users disproportionately query French travel/local content
(topic 1, **2× over-represented in queries**, 21.5% vs 10.4%). Pharmacy spam
(topic 2) is a measurable document slice that is essentially never queried.
The dominant French-general topic 3 is balanced (~0.60 both). In short: a large
fraction of the indexed corpus (English media + spam, ~30%) is content the
French query stream does not ask about — direct motivation for topic-aware
retrieval / corpus filtering.

### K=20 — top query topics (non-empty, dominant)

| Topic | Theme | Share |
|---|---|---|
| 14 | e-commerce / products (produit, client, livraison) | 16.6% |
| 7 | French geography / regions (saint, Rhône-Alpes, départements) | 13.2% |
| 13 | French admin / municipal / law (conseil municipal, travail, loi) | 12.8% |
| 16 | general life / society (monde, vie, histoire, femme) | 11.8% |
| 3 | climate / economy / development (chang. climat, projet, entreprise) | 10.1% |
| 10 | forums / vehicles | 8.9% |
| 12 | (mailing-list / boilerplate) | 8.3% |

Spam/boilerplate document topics are bottom-ranked among queries: pharmacy
(t17) 0.2%, date-spam (t15) 0.2%, country-list (t11) 0.1%, parent-id boilerplate
(t12 is high here but t6 awards-spam 0.1%). K=20 confirms the K=4 story at finer
grain: real queries concentrate in e-commerce, geography, admin, society and
climate/economy; the spam/English-media mass that bloats the document corpus is
un-queried.

### Caveats

- ~3-word queries are a thin signal for a 20k-vocab document topic model; 7.7%
  map to nothing in-vocab (pruned by `min_df=50`, see above). The dominant-topic
  histograms and headline mean θ are over the 92.3% with ≥1 in-vocab term;
  empties yield Spark's zero vector (not the prior).
- Inference uses each archive's frozen `preprocess/` (identical vocab/phrases
  across K=4 and K=20 by construction) and the converged `lda_model`. Online
  LDA `transform` is deterministic given fixed features.

## Artifacts

| What | Path |
|---|---|
| Per-query topic dist (K=4) | `/mnt/data/tmp/lda-queries/k4_query_topics.parquet` |
| Per-query topic dist (K=20) | `/mnt/data/tmp/lda-queries/k20_query_topics.parquet` |
| Query stats JSON | `/mnt/data/tmp/longeval-query-stats.json` |
| Query-stats driver | `scripts/query-stats.py` |
| LDA-on-queries driver | `scripts/lda-infer-queries.py` |
| Models used | `longeval-lda-k{4,20}-converged-archive/` (see [`20260518-lda-k-selection.md`](20260518-lda-k-selection.md)) |
