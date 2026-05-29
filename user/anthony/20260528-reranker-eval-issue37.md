# Reranker evaluation — Anthony's contribution (issue #37)

**Date opened:** 2026-05-28 · **Tracks:** GitHub issue #37 · **PR:** #38
**Joins:** `20260527-reranker-eval-issue37.md` (primary worklog,
read that first for the protocol + plumbing).

## What's already done

Reranker eval is mid-flight on PACE; cells land under `~/scratch/longeval/2025/`.

- **5-arm 1k 3-seed sweep** (the same protocol as the first issue-#37 pass):
  - `bm25` baseline · `antoinelouis/crossencoder-camembert-base-mmarcoFR` (paper
    control) · `antoinelouis/crossencoder-camembert-large-mmarcoFR` ·
    `antoinelouis/crossencoder-camemberta-L10-mmarcoFR` · `BAAI/bge-reranker-v2-m3`
    · `jinaai/jina-reranker-v2-base-multilingual`
  - 15 dates × 3 seeds × 6 arms = **270 cells** when complete, written into
    `~/scratch/longeval/2025/rerank/date=*/seed=*/model=*/{summary,ndcg_per_qid}.parquet`
  - Aggregator: `scripts/aggregate-rerank-results.py`
- **Full-query single-seed sweep** for the 3 baseline arms (bm25, camembert-base,
  jina-v2) into `~/scratch/longeval/2025/rerank-full/`. Confirms the 1k subsample
  numbers within ≤1.4% absolute.

Current headline (from the prior 1k 3-seed run, 3 arms): jina-v2 beats the
paper control on all 15/15 dates; the wider 5-arm comparison is what's
finishing now.

## Where Anthony picks up

Open hooks — pick whichever is most useful (or propose your own):

### 1. Per-topic reranker effectiveness using LDA registers

Cross your K=4 (and/or K=20) topic registers from `20260525-lda-topic-register-names.md`
with per-qid reranker nDCG@10. Does jina-v2's lift over BM25 (and over the
control) vary by topic register? "French Web Boilerplate" queries might respond
differently than "English Web Forum & Entertainment Mix" queries — that would
tie the topic work to the reranker decision directly.

Data inputs:
- per-qid nDCG: `~/scratch/longeval/2025/rerank/date=*/seed=42/model=*/ndcg_per_qid.parquet`
- LDA query topics: (wherever your K=4/K=20 query topic assignments live —
  point me at the path and I'll wire the join)

Output suggestion: a table `topic_register × arm → mean nDCG@10`, plus a
"reranker lift" column (`Δ(arm − bm25)` per register).

### 2. Statistical significance — paired Wilcoxon per pair of arms per date

The 1k 3-seed table has between-seed std, but no formal paired test on the
per-qid scores. With `ndcg_per_qid.parquet`, a paired Wilcoxon (or paired
bootstrap) of jina-v2 vs camembert-base / vs bge-v2-m3 per date would say
whether the differences hold up at the p<0.05 level on the actual qids.

### 3. Drift over time

Recompute the across-dates "did jina's lead grow or shrink" trend — useful for
the LongEval-specific narrative. The full-query sweep gives one number per
date with no error bars; the 1k 3-seed sweep gives 3 numbers per date.

### 4. Reproducibility cross-check

Pick one (date, seed, model) cell, re-run it from your own venv on a separate
machine, and verify the nDCG matches to 4 decimal places. Useful for the paper
reproducibility statement.

## Pointers

- Protocol details + bug discoveries + execution notes: see acmiyaguchi's
  worklog (`20260527-reranker-eval-issue37.md`).
- Code: `scripts/rerank-eval.py` (driver), `scripts/aggregate-rerank-results.py`
  (table generator), `sbatch/experiment-rerank{,-extras}.sbatch` (runners).
- Raw rows: `results/issue37-results.csv` (1k 3-seed for first 3 arms;
  the 5-arm + full-query versions will land in sibling files).
- Issue: <https://github.com/dsgt-arc/longeval-2025/issues/37>; PR: #38.

## Findings

(Anthony to fill in.)
