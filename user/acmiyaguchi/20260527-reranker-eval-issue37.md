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
| 2b ETL | `sbatch/etl-web2025-parquet.sbatch` → `longeval etl web2025-parquet` (`longeval/etl/parquet/workflow.py`, reuses `TrecCollection`) | `~/scratch/longeval/2025/parquet/{train,test}/{Documents,Queries,Qrels}` |
| 3 BM25 | `sbatch/experiment-bm25.sbatch` → `…bm25.workflow run-bm25` | `~/scratch/longeval/2025/bm25/retrieval/date=*/sample_id=*` |
| 4 rerank | `sbatch/experiment-rerank.sbatch` → `scripts/rerank-eval.py` | `~/scratch/longeval/2025/rerank/date=*/seed=*/model=*` |
| 5 aggregate | `scripts/aggregate-rerank-results.py` | `results.{csv,md}`, `manifest.json` |

## Run order (PACE)

```bash
sbatch sbatch/configure-apptainer.sbatch          # once (builds app.sif + venv; pins torch cu126, transformers<5)
sbatch sbatch/download-longeval.sbatch            # resumable; also fetches+extracts the test-qrels zip

# ETL: smoke one train + one test date first (array tasks 7=2023-01, 9=2023-03), then full
sbatch --array=7,9 sbatch/etl-web2025-parquet.sbatch   # verify test/Qrels is non-empty
sbatch --array=0-14 sbatch/etl-web2025-parquet.sbatch  # all 15 dates

sbatch sbatch/experiment-bm25.sbatch              # array 0-3, all 15 dates indexed + retrieved (k=100)

# rerank smoke (highest-value gate): control on 2023-01 seed 42 should be ~0.377
sbatch sbatch/experiment-rerank-smoke.sbatch
sbatch sbatch/experiment-rerank.sbatch            # array 0-14, full 3-arm sweep
# (re-run the largest/earliest dates with more RAM if they OOM:)
# sbatch --array=0,1,2 --mem-per-cpu=16G sbatch/experiment-rerank.sbatch

apptainer exec ~/scratch/longeval/app.sif bash -lc \
  'source ~/scratch/longeval/app/.venv/bin/activate && \
   python ~/arc/longeval-2025/scripts/aggregate-rerank-results.py \
     --rerank-root ~/scratch/longeval/2025/rerank'
```

## Results

Full grid completed: 15 dates × 3 seeds (42,1,2) × 3 arms = **135 runs**. Cells are
mean±std over seeds; 1k-query subsample, k=100, nDCG@10, fp16 on V100. Artifacts:
`issue37-results.{md,csv}`, `issue37-manifest.json` (copied from
`~/scratch/longeval/2025/rerank/`).

| date | split | bm25 | camembert-base | jina-v2 |
|---|---|---|---|---|
| 2022-06 | train | 0.1240±0.0118 | 0.1599±0.0160 | 0.1782±0.0210 |
| 2022-07 | train | 0.1310±0.0151 | 0.1589±0.0120 | 0.1759±0.0103 |
| 2022-08 | train | 0.1402±0.0139 | 0.1657±0.0104 | 0.1912±0.0152 |
| 2022-09 | train | 0.2131±0.0031 | 0.2583±0.0035 | 0.2901±0.0015 |
| 2022-10 | train | 0.2924±0.0066 | 0.3548±0.0109 | 0.3940±0.0094 |
| 2022-11 | train | 0.2886±0.0074 | 0.3524±0.0077 | 0.3828±0.0042 |
| 2022-12 | train | 0.3173±0.0072 | 0.3757±0.0019 | 0.4133±0.0095 |
| 2023-01 | train | 0.3123±0.0086 | 0.3738±0.0108 | 0.4134±0.0113 |
| 2023-02 | train | 0.3195±0.0062 | 0.3780±0.0058 | 0.4212±0.0107 |
| 2023-03 | test | 0.3129±0.0076 | 0.3847±0.0036 | 0.4281±0.0063 |
| 2023-04 | test | 0.3159±0.0065 | 0.3770±0.0061 | 0.4079±0.0185 |
| 2023-05 | test | 0.3321±0.0033 | 0.3986±0.0116 | 0.4350±0.0120 |
| 2023-06 | test | 0.3170±0.0100 | 0.3733±0.0106 | 0.4136±0.0130 |
| 2023-07 | test | 0.3254±0.0082 | 0.3982±0.0059 | 0.4346±0.0041 |
| 2023-08 | test | 0.2895±0.0081 | 0.3471±0.0043 | 0.3698±0.0063 |

| group | bm25 | camembert-base | jina-v2 |
|---|---|---|---|
| train (9) | 0.2376±0.0856 | 0.2864±0.1004 | 0.3178±0.1092 |
| test (6) | 0.3155±0.0146 | 0.3798±0.0192 | 0.4148±0.0247 |
| pooled (15) | 0.2687±0.0763 | 0.3237±0.0902 | 0.3566±0.0972 |

Throughput (V100, fp16): camembert-base ~530 pairs/s, jina-v2 ~412 pairs/s.
Versions (manifest): torch 2.12.0+cu126, transformers 4.57.6, sentence-transformers
5.5.1, ir_datasets 0.5.11.

## Decisions

- **Does jina-v2 consistently beat the control? YES — on all 15/15 dates**, train
  and test, every seed. On the held-out test set jina-v2 leads camembert-base by
  **+0.035 nDCG@10 (0.4148 vs 0.3798, ~9% relative)**; both beat BM25 everywhere.
  The prior single-date observation (2023-01: jina 0.4145 vs control 0.3772)
  generalizes — this run gives 2023-01 jina 0.4134 / control 0.3738.
- **Promote jina-v2 as the default reranker: YES.** The win is consistent and
  sizable, with no date where the control is competitive. The fp16 cost (~412 vs
  ~530 pairs/s) is modest.
- **Keep query expansion excluded: YES** — negative in the prior worklog; BM25
  expanded loops stay pinned off (`for with_expanded_queries in [False]`).

## Execution notes (what actually happened)

The plan's irds-API ETL was **pivoted to reuse `TrecCollection`** (Spark direct
read of the raw `.trec` files) — the ir_datasets docstore build was ~20–45 min/date;
the TrecCollection path is ~1–2 min/date. New entrypoint
`longeval etl web2025-parquet` (`longeval/etl/parquet/workflow.py`), sbatch
`etl-web2025-parquet.sbatch`. **Test qrels** are not exposed by the ir_datasets API
(`has_qrels()==False` for test ids); they ship as `longeval_web_test_qrels.zip` in
the package `downloads.json` (`longeval_2025_web_test_qrels`) and are fetched +
extracted by `download-longeval.sbatch`, then read by the ETL.

Issues found and fixed during execution (all on PR #38):

1. **`.env` NixOS leakage into the container.** The repo `.env` hardcodes the dev
   box's `JAVA_HOME`/`JVM_PATH` (a `/nix/store/...` openjdk) and
   `SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt` (RHEL/Nix naming).
   `longeval.spark` loads it via python-dotenv with `override=False`, so PySpark's
   JVM launch failed (`JAVA_GATEWAY_EXITED`). Each Spark/pyserini sbatch now exports
   the container's OpenJDK 21 (`/usr/lib/jvm/java-21-openjdk-amd64`) and Ubuntu's
   `ca-certificates.crt`, which win over `.env`.
2. **pyserini import needs OpenAI creds.** `pyserini.search.lucene` transitively
   imports `encode._openai`, which instantiates `openai.OpenAI()` at import time →
   needs a (dummy) `OPENAI_API_KEY`; that client also reads `SSL_CERT_FILE`. Fixed
   in `experiment-bm25.sbatch`.
3. **docid prefix mismatch → nDCG=0 (the big one).** LongEval document DOCNOs carry
   a `doc` prefix (`doc14290`) so the collection, index, and BM25 retrieval all use
   prefixed ids, but the qrels use the **bare** numeric id (`14290`). The
   `qid+docid` join in `score_search` never landed → every `rel`=0 → BM25 nDCG was
   0.0 across all dates (caught at the Stage-3 gate). Fixed by stripping a leading
   `doc` (only before a digit) on both sides of the join; verified 2023-01 BM25
   0.0 → 0.3116. Fixes both BM25 eval and rerank scoring (both route through
   `score_search`). The planning note that "no stripping needed" was wrong.
4. **torch was cu130 (CUDA 13).** PACE V100 nodes (driver 575.57.08) support only
   CUDA 12.9, so `torch.cuda.is_available()==False` and the reranker silently ran on
   CPU (45-min timeout). torch 2.12.0 ships only cu126 and cu130 builds; re-pinned
   to **cu126** in `configure-apptainer.sbatch`.
5. **jina-v2 vs transformers 5.x.** The container resolved transformers 5.9.0,
   whose major refactor dropped XLM-R helpers (`create_position_ids_from_input_ids`)
   that jina-reranker-v2's `trust_remote_code` modeling imports → jina failed to
   load. Pinned **transformers <5** (4.57.6); sentence-transformers 5.5.1 stays
   compatible and the camembert control is unaffected.
6. **OOM on the 3 earliest dates.** 2022-06/07/08 are the largest snapshots; the
   full-candidate `toPandas` in `_build_candidates` exceeded the 96G cgroup. Re-ran
   those three at `--mem-per-cpu=16G` (192G); all 135 cells then present. (A future
   optimization: subsample queries before materializing candidate contents.)
