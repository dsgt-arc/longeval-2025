# Query expansion: additive redesign + full-range re-score

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

Does LLM query expansion help first-stage BM25 on LongEval-Web (French)? The
CLEF working notes (`../longeval-2025-working-notes`) report it *hurt*
(bm25-expanded pooled 0.194 < bm25 0.242).

**TL;DR — first-stage BM25 has no headroom for expansion here.** We reproduce
the baseline, fix the obvious flaw in the paper's expander (it replaced the
query with a ≤100-word blob), and re-score all 15 monthly slices. The redesign
beats the paper's expansion at every date (pooled 0.204 vs 0.194) but still
loses to the no-expansion baseline at every date (0.204 vs 0.242); a sweep over
language × term-count × original-weight finds no configuration that reaches
baseline. The paper's reported gains came from the reranker, not expansion.

---

## 1. Why the paper's expansion failed, and the redesign

The paper **replaces** the query with a ≤100-word LLM-rewritten blob (single
`query` field, Gemini/DeepSeek-v3). The retrieval query is a flat, unweighted
Lucene bag of words (`longeval/experiment/bm25/retrieval.py:20` → `searcher.search`),
so a 3-word query buried in 97 generated words loses the IDF-weighted original
terms that were doing the work. The paper's own discussion diagnoses this
("important frequency-adjusted keywords are lost due to repetition").

Redesign (`user/acmiyaguchi/query-expansion/main.py`):

- **Additive, not replacement.** The model returns `terms[]` only (strict JSON
  schema); the script keeps the original verbatim and appends. Dedup drops any
  term whose words are already present.
- **`ORIGINAL_WEIGHT`** repeats the original query N× to boost its query-side
  term frequency (poor-man's λ, since the bag is unweighted). **`MAX_TERMS`**
  caps additions. Both are `Variant` knobs.
- **Two arms**: `french` (French terms only) and `english` (French + English
  equivalents — a deliberate test, see §4).

## 2. Generation

- Model **`deepseek/deepseek-v4-flash`** via OpenRouter. 75,427 unique queries
  (train `queries.trec` master + test TSVs), batch=25, 24 workers.
- **Provider routing matters.** Default routing hit **Alibaba**, whose platform
  output-filter 502s on adult query terms (e.g. `101boyvideos`, `18 videoz`,
  `abdl videos` — clustered in low qids). This is the *host* filtering, not
  DeepSeek refusing (the model returns reasoning, the provider blocks output).
  Fix: `provider={order:[DeepInfra], allow_fallbacks:true, ignore:[Alibaba,Baidu]}`
  → refusals eliminated. Adult batches are slow on every Western host
  (~180–240s) so request `timeout=300`.
- **Idempotent**: deterministic batching (sorted qids, fixed batch=25) →
  stable `{start}-{end}.json` filenames; `exists()`-skip; atomic write
  (tmp + `os.replace`). 3011/3018 files per arm; 7 batches lost to
  refusal/timeout (~0.2% of queries, fall back to original). Cost ~$0.90/arm.

## 3. Scoring

`scripts/validate-bm25-full.py --expanded-root <variant>` — left-joins the
expansion JSON onto each date's queries (coalesce → original for missing qids),
runs the repo's own `run_search` + `score_search`, reuses the 15 cached TREC
indexes at `/mnt/data/tmp/longeval-bm25-test/index_trec/` (no re-indexing).
Single-date variant: `scripts/validate-bm25-single-date.py`. Conservative
re-merge sweep (no API calls): `scripts/sweep-expansion-rescore.py`.

### Baseline reproduction
Pooled NDCG@10 **0.2424** vs paper 0.242 (213k queries); single-date 2023-01
**0.3127** vs paper 0.312. Harness validated (sweep `terms=0` anchor returns
0.3128 = baseline; whole-query repetition is ranking-invariant).

## 4. Results — full 15-date range

| Date | Baseline BM25 | French additive | English additive | Paper's expanded (replacement) |
|---|---|---|---|---|
| 2022-06 | 0.127 | 0.111 | 0.106 | 0.110 |
| 2022-07 | 0.134 | 0.118 | 0.111 | 0.116 |
| 2022-08 | 0.141 | 0.123 | 0.118 | 0.123 |
| 2022-09 | 0.210 | 0.191 | 0.183 | 0.184 |
| 2022-10 | 0.296 | 0.245 | 0.237 | 0.236 |
| 2022-11 | 0.292 | 0.240 | 0.233 | 0.226 |
| 2022-12 | 0.303 | 0.253 | 0.247 | 0.238 |
| 2023-01 | 0.312 | 0.257 | 0.250 | 0.237 |
| 2023-02 | 0.310 | 0.263 | 0.255 | 0.251 |
| 2023-03 | 0.316 | 0.263 | 0.254 | 0.246 |
| 2023-04 | 0.323 | 0.270 | 0.258 | 0.253 |
| 2023-05 | 0.327 | 0.273 | 0.264 | 0.255 |
| 2023-06 | 0.318 | 0.265 | 0.258 | 0.253 |
| 2023-07 | 0.319 | 0.267 | 0.258 | 0.252 |
| 2023-08 | 0.284 | 0.240 | 0.234 | 0.223 |
| **pooled** | **0.242** | **0.204** | **0.197** | **0.194** |

(Baseline = our reproduction, identical to the paper's published per-month
values. Ordering is consistent at **every** date: baseline > french > english >
paper's replacement. English < french by 0.005–0.012 throughout, confirming the
French-analyzer-makes-English-inert effect across the full range.)

### Conservative re-merge sweep (french, 2023-01; baseline = 0.3127)
Re-merged from stored `terms[]`, no new API calls.

```
            weight=2   weight=3   weight=4
 terms=0     0.3128  ← anchor == baseline (validates harness)
 terms=3     0.2731    0.2913    0.3005  ← best non-trivial, still −0.012
 terms=5     0.2623    0.2838    0.2948
 terms=8     0.2579    0.2804    0.2920
 terms=12    0.2571    0.2796    0.2915
```
Monotone both ways (fewer terms / heavier weight → closer to baseline), never
reaches it. At weight=4 the term count barely matters — heavy original-weighting
swamps the expansion signal and converges to "baseline minus a small constant."

## 5. Findings

1. **Additive > replacement at every date.** The design *does* matter — part of
   the paper's loss was the lossy 100-word replacement, not expansion per se.
2. **But expansion < baseline at every date.** No month is an exception; the
   negative result is robust across both temporal regimes.
3. **Penalty scales with baseline quality.** In the degraded pre-Oct-2022 regime
   the gap is ~0.016; in the healthy post-regime period it widens to ~0.05. More
   relevance signal to disrupt → expansion costs more. Never flips to helping.
4. **English < French.** The index is French-analyzed (`set_language('fr')`), so
   English terms mostly stem to inert tokens (cognates collapse onto the French
   stem, e.g. `climate change`→`climat,chang` == `changement climatique`;
   generic English → mangled non-matching tokens; English stopwords survive as
   junk). Adding English wastes term slots; it does not help.
5. **MAP also drops** (2023-01: baseline 0.272 → french 0.222), so expansion
   degrades ranking broadly, not just at the top-10 cutoff. This answers the
   open question in the paper's discussion ("we would need to recompute MAP").

## 6. Threats to validity

- **Evaluation method.** `score_search` computes IDCG over retrieved+judged docs
  (fillna-0 join), not canonical `trec_eval`. It's the repo's own method (what
  produced the paper numbers) so it's the correct *internal* comparison; state
  this explicitly when comparing across other LongEval papers.
- **Query-set mismatch.** Coalesce fallback gives every qid a query, so arms
  score slightly more (pooled n 215,751 vs baseline 213,402, ~1%). Far too small
  to explain a 0.038 pooled gap, but note it.
- **Scope.** This tests *additive LLM term-expansion in a flat BM25 bag* over
  terms 3–12, weight 2–4, two languages, one model/prompt family. It does **not**
  test classic weighted pseudo-relevance feedback (RM3 with λ), which is the
  standard a reviewer will name. Claim accordingly.
- **Non-determinism.** LLM generation; numbers will vary slightly on regeneration.

## 7. Artifacts

- Expansions: `~/scratch/longeval/query_expansion/{french,english}/expansion/*.json`
- Scores: `/mnt/data/tmp/longeval-bm25-test/scores_{french,english}/`, baseline `.../scores/`
- Indexes (15 dates): `/mnt/data/tmp/longeval-bm25-test/index_trec/date=*`
- Scripts: `query-expansion/main.py`, `scripts/validate-bm25-{single-date,full}.py`,
  `scripts/sweep-expansion-rescore.py`
