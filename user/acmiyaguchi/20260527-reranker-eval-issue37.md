# Comprehensive reranker evaluation across LongEval-Web dates (issue #37)

**Date opened:** 2026-05-27 · **Owner:** acmiyaguchi (driven via Claude Code)
**Branch:** `issue-37-reranker-eval` · **Tracks:** GitHub issue #37

Follows up `20260523-reranker-model-comparison.md`, which found
`jinaai/jina-reranker-v2-base-multilingual` (0.4145) beat the paper control
`antoinelouis/crossencoder-camembert-base-mmarcoFR` (0.3772) on a single 2023-01
1k-query subsample. This issue tests whether that win **holds across all 15
LongEval-Web dates** (9 train + 6 test), with multi-seed subsampling.

## Why the pipeline is rebuilt from scratch on PACE

The 2025 environment is gone: project storage `/storage/coda1/p-dsgt_clef2025/...`
is deallocated, `~/scratch/longeval` was cleared (no `app.sif`/venv/data), and the
prior single-date rerank ran on a separate GCP box (`/mnt/data`). The repo also
moved to `~/arc/longeval-2025` (old sbatch hardcoded the dead `~/clef`). So this
work re-stages everything under `~/scratch/longeval` and runs on PACE V100s.

**Path hygiene:** code stays read-only at `~/arc/longeval-2025`; everything mutable
(`app.sif`, the single venv at `app/.venv`, `hf_cache/`, `ir_datasets/`,
`2025/{parquet,bm25,rerank}`) lives under `~/scratch/longeval`; Spark scratch is the
node-local `$TMPDIR/spark-tmp`. Every job runs inside apptainer activating the one
`app/.venv`.

## Experiment design

- **Coverage:** 15 dates — train 2022-06..2023-02, test 2023-03..2023-08. **Test
  qrels come from `ir_datasets`** (the new ETL writes `test/Qrels`, unlike the old
  `Raw2025TestCollection` which raised `NotImplementedError`).
- **Arms (3-core):** `bm25` baseline, `antoinelouis/crossencoder-camembert-base-mmarcoFR`
  (control), `jinaai/jina-reranker-v2-base-multilingual`.
- **Sampling:** seeds 42, 1, 2 × 1000 queries/date; candidate depth k=100; metric
  nDCG@10; max_seq_len=512; fp16 on V100.
- **Query expansion:** excluded (it was negative in the prior worklog and the BM25
  expanded loops are pinned off — `workflow.py` `for with_expanded_queries in [False]`).

## Pipeline stages / artifacts

| Stage | Script / sbatch | Output |
|---|---|---|
| 0 env | `sbatch/configure-apptainer.sbatch` | `~/scratch/longeval/app.sif`, `app/.venv` |
| 1 download | `sbatch/download-longeval.sbatch` → `scripts/download-longeval` | `~/scratch/longeval/ir_datasets/longeval-web` |
| 2a probe | `scripts/inspect-irds-layout.py` | stdout (gates 2b) |
| 2b ETL | `sbatch/etl-irds-parquet.sbatch` → `longeval etl irds-parquet` (`longeval/etl/irds/workflow.py`) | `~/scratch/longeval/2025/parquet/{train,test}/{Documents,Queries,Qrels}` |
| 3 BM25 | `sbatch/experiment-bm25.sbatch` → `…bm25.workflow run-bm25` | `~/scratch/longeval/2025/bm25/retrieval/date=*/sample_id=*` |
| 4 rerank | `sbatch/experiment-rerank.sbatch` → `scripts/rerank-eval.py` | `~/scratch/longeval/2025/rerank/date=*/seed=*/model=*` |
| 5 aggregate | `scripts/aggregate-rerank-results.py` | `results.{csv,md}`, `manifest.json` |

## Run order (PACE)

```bash
sbatch sbatch/configure-apptainer.sbatch          # once
sbatch sbatch/download-longeval.sbatch            # resumable; resubmit if it times out

# probe BEFORE the full ETL — confirms id->date convention + that test ids carry qrels
apptainer exec ~/scratch/longeval/app.sif bash -lc \
  'source ~/scratch/longeval/app/.venv/bin/activate && \
   python ~/arc/longeval-2025/scripts/inspect-irds-layout.py'
# if the probe disagrees with longeval/etl/irds/workflow.py::_split_date_for_id, fix that fn

# ETL: smoke one train + one test date first, then the full run
ONLY_DATE=2023-01 sbatch sbatch/etl-irds-parquet.sbatch
ONLY_DATE=2023-03 sbatch sbatch/etl-irds-parquet.sbatch   # verify test/Qrels is non-empty
sbatch sbatch/etl-irds-parquet.sbatch                     # all 15 dates

sbatch sbatch/experiment-bm25.sbatch              # array 0-3, all 15 dates indexed+retrieved

# rerank smoke (highest-value gate): control on 2023-01 seed 42 should be ~0.377
apptainer exec --nv ~/scratch/longeval/app.sif bash -lc \
  'source ~/scratch/longeval/app/.venv/bin/activate && \
   python ~/arc/longeval-2025/scripts/rerank-eval.py --date 2023-01 \
     --models antoinelouis/crossencoder-camembert-base-mmarcoFR --seeds 42'
sbatch sbatch/experiment-rerank.sbatch            # array 0-14, full sweep

apptainer exec ~/scratch/longeval/app.sif bash -lc \
  'source ~/scratch/longeval/app/.venv/bin/activate && \
   python ~/arc/longeval-2025/scripts/aggregate-rerank-results.py'
```

## Results

_(populated after the run — `aggregate-rerank-results.py` prints the per-date table
and writes `~/scratch/longeval/2025/rerank/results.md` + `manifest.json`.)_

## Decisions (to fill in)

- [ ] Does jina-v2 consistently beat the control across dates? (mean±std)
- [ ] Promote jina-v2 as the default/final reranker?
- [ ] Keep query expansion excluded from final runs?

## Open risks carried from planning

- **ir_datasets API/layout** (`ir_datasets_longeval==0.0.15`) was unverifiable off
  PACE; the Stage-2a probe gates it. If the API lacks per-test-id qrels or is too
  slow, fall back to symlinking the extracted tree into the
  `release_2025_p2/French/...` layout and implementing `Raw2025TestCollection.qrels`.
- **docid match:** BM25 candidate docids and qrels docids both come from
  ir_datasets `doc_id` (no `doc` prefix), so no stripping — the rerank smoke must
  still confirm non-zero BM25 nDCG.
