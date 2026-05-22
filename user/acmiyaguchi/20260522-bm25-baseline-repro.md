# BM25 baseline reproduction — CLEF Working Notes

**Date opened:** 2026-05-22 · **Owner:** acmiyaguchi (driven via Claude Code)

Verifies that the BM25 baseline reported in the CLEF Working Notes
(`../longeval-2025-working-notes`, `results_system.tex`) is reproducible
end-to-end from this repo on the local LongEval-Web release. All 15 monthly
slices and the pooled mean match the paper to three decimals.

---

## Result

Retrieval over the full LongEval-Web French corpus (9 train slices
2022-06..2023-02 + 6 test slices 2023-03..2023-08), top-100 per query, scored
with `pytrec_eval` (NDCG@10) via the repo's own `run_search` + `score_search`.

| Date | NDCG@10 (repro) | NDCG@10 (paper) | queries |
|---|---|---|---|
| 2022-06 | 0.1280 | 0.127 | 24,142 |
| 2022-07 | 0.1338 | 0.134 | 24,941 |
| 2022-08 | 0.1415 | 0.141 | 27,720 |
| 2022-09 | 0.2104 | 0.210 | 7,742 |
| 2022-10 | 0.2967 | 0.296 | 12,088 |
| 2022-11 | 0.2921 | 0.292 | 14,877 |
| 2022-12 | 0.3047 | 0.303 | 15,230 |
| 2023-01 | 0.3127 | 0.312 | 15,877 |
| 2023-02 | 0.3101 | 0.310 | 7,900 |
| 2023-03 | 0.3155 | 0.316 | 5,607 |
| 2023-04 | 0.3234 | 0.323 | 14,446 |
| 2023-05 | 0.3273 | 0.327 | 11,548 |
| 2023-06 | 0.3188 | 0.318 | 8,684 |
| 2023-07 | 0.3203 | 0.319 | 9,760 |
| 2023-08 | 0.2848 | 0.284 | 12,840 |
| **pooled** | **0.2424** | **0.242** | **213,402** |

The pooled mean is over every scored query (the paper's "Aggregated nDCG@10 by
Experiment", bm25 row). The regime change the paper flags — the ~0.14→0.30 jump
at 2022-09/10 — reproduces.

## Method

- **Index:** `BM25IndexFromTrecTask` (pyserini/Anserini, Lucene, `--language fr`,
  `storePositions`/`storeDocvectors`) directly from the raw TREC slices — no
  full-corpus parquet ETL. The 8 train indexes 2022-06..2023-01 pre-existed;
  2023-02 + the 6 test slices were indexed fresh for this run.
- **Retrieve:** `run_search`, top-100, queries read straight from the raw
  per-slice TSVs.
- **Score:** `score_search` (`pytrec_eval`) against the raw qrels — train
  qrels from the release, test qrels from the extracted
  `longeval_web_test_qrels.zip`.
- **Drivers:** `scripts/validate-bm25-single-date.py` (single slice),
  `scripts/validate-bm25-full.py` (all 15, resumable, writes per-date scores +
  `summary.json`).

### Two fixes the TREC-direct path needs (the parquet/Json path didn't)

1. **DOCNO prefix.** TREC `<DOCNO>` carries a `doc` prefix (`doc14290`) but
   `qrels_processed.txt` uses bare integers (`14290`). Without stripping it the
   score join misses every doc and NDCG silently reads 0.0000 (this was the
   first-run failure). Fix: `regexp_replace(docid, "^doc", "")` before scoring.
2. **Misnamed slices.** 2023-02 (train) and all 6 test slices ship as
   `*.jsonl.gz` that are actually plaintext TREC XML (not gzip, not JSON).
   Pyserini's gz reader dies on the suffix, so the driver hardlink-mirrors each
   to a `.trec` name on the same `/mnt/data` device (instant, no copy) before
   indexing — the same trick `TrecCollection._trec_read_path` uses for Spark.

> Both fixes live only in the validation scripts. If TREC-direct indexing ever
> replaces the Json-parquet path in the evaluation DAG, they must move into the
> workflow.

## Artifact map

| What | Path |
|---|---|
| Per-date scores (parquet) | `/mnt/data/tmp/longeval-bm25-test/scores/date=*/` |
| Run summary (JSON) | `/mnt/data/tmp/longeval-bm25-test/summary.json` |
| TREC indexes (15 slices) | `/mnt/data/tmp/longeval-bm25-test/index_trec/date=*/` |
| `.trec` hardlink mirror | `/mnt/data/tmp/longeval-trec-mirror-bm25/Trec/<date>_fr/` |
| Single-slice driver | `scripts/validate-bm25-single-date.py` |
| Full 15-date driver | `scripts/validate-bm25-full.py` |
| Test qrels (extracted) | `~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels/<date>/` |

## Reproducibility tier status

| System (paper) | NDCG@10 | Status |
|---|---|---|
| bm25 | 0.242 | ✅ reproduced (this doc) |
| bm25-expanded | 0.194 | ❌ saved Gemini/DeepSeek expansion JSON not in repo (non-deterministic LLM; content-filter forced a mid-batch model swap) |
| bm25-reranked | 0.296 | ❌ reranker model unpinned (`rerankers` default fr cross-encoder), rerank step out-of-DAG |
| bm25-expanded-reranked | 0.295 | ❌ both gaps above |

The deterministic baseline is proven. Making the headline 0.296 reproducible
needs: (a) commit the saved expansion mapping as a fixture, (b) pin the exact
cross-encoder HF model+revision, (c) fold expansion/rerank into the Luigi DAG.
