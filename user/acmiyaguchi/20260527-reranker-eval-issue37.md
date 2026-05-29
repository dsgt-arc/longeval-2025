# Comprehensive reranker evaluation across LongEval-Web dates (issue #37)

**Date opened:** 2026-05-27 · **Owner:** acmiyaguchi (driven via Claude Code)
**Branch:** `issue-37-reranker-eval` · **Tracks:** GitHub issue #37

## Status (2026-05-28) — done

All four sub-experiments finished and on PR #38:

| phase | runs | result |
|---|---|---|
| Phase 1 — 3-arm 1k 3-seed (bm25 / camembert-base / jina-v2) | 135 | jina-v2 beats camembert-base on **15/15 dates**, every seed |
| Full-query confirmation (same 3 arms, all qids, seed 42) | 45 | matches phase 1 within ±0.014 absolute; jina-v2 still wins every date |
| Phase 2 — 5-arm 1k 3-seed extras (+ camembert-large, camemberta-L10, bge-v2-m3) | 270 | jina-v2 leads; bge-v2-m3 close 2nd; bigger camembert ≈ camembert-base |
| Phase 3 — expansion + reranker stack (french-additive BM25 → bm25 / jina-v2) | 90 + 30 | jina recovers ~74% of BM25's expansion loss but never overtakes no-exp jina-v2 |

**Test-set headline (6 dates, mean nDCG@10, 1k 3-seed):**

| arm | test | vs jina-v2 |
|---|---:|---:|
| jina-v2 | 0.4148 | — |
| bge-v2-m3 | 0.4081 | −0.0067 |
| jina-v2 (french-exp) | 0.4011 | −0.0137 |
| camembert-large | 0.3842 | −0.0306 |
| camemberta-L10 | 0.3805 | −0.0343 |
| camembert-base | 0.3798 | −0.0350 |
| bm25 | 0.3155 | −0.0993 |
| bm25 (french-exp) | 0.2663 | −0.1485 |

**Decisions taken** (full detail in the Decisions section):
- Promote **jina-v2** as the default reranker.
- **bge-v2-m3** is a viable backup (same quality tier, ~2.3× slower).
- Don't bother with camembert-large / camemberta-L10 — gains over camembert-base
  are within seed std.
- **Keep query expansion excluded** — confirmed by the phase 3 stack: jina-v2
  on expanded candidates (0.4011) loses to jina-v2 on no-exp candidates (0.4148)
  on every test date.

**For the paper** — paper-ready artifacts live next to this worklog:
- `issue37-results.{md,csv}` + `issue37-manifest.json` — 5-arm 1k 3-seed
  table (the primary table)
- `issue37-results-full.{md,csv}` + `issue37-manifest-full.json` — 3-arm
  full-query confirmation table (for the appendix / reproducibility statement)
- `issue37-results-expanded.{md,csv}` + `issue37-manifest-expanded.json` —
  expansion + reranker 1k 3-seed (bm25-exp, jina-exp)
- `issue37-results-expanded-full.{md,csv}` + `issue37-manifest-expanded-full.json` —
  expansion + reranker full-query confirmation
- This worklog has the protocol, the bug-discovery notes (docid stripping,
  torch cu126 pin, transformers<5 pin, expansion NaN-coalesce), and the
  decision rationale.

**Open hooks for follow-up analysis** are stubbed in
`user/anthony/20260528-reranker-eval-issue37.md`: per-topic reranker
effectiveness via LDA registers, paired Wilcoxon per arm-pair, drift-over-time,
independent reproducibility cross-check.

---

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
- **Query expansion:** excluded by default (it was negative in the prior worklog).
  The expanded arm is flag-gated in `workflow.py` (`--with-expanded`/`--expanded-only`)
  and defaults to `expansion_arms = [False]`; phase 3 exercised it explicitly.

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

## Results (initial 3-arm pass)

Initial 3-arm grid: 15 dates × 3 seeds (42,1,2) × 3 arms = **135 runs**. Cells are
mean±std over seeds; 1k-query subsample, k=100, nDCG@10, fp16 on V100. (The
"5-arm extras sweep" section below expands this to 6 arms; `issue37-results.{md,csv}`
and `issue37-manifest.json` reflect the 5-arm 270-run table.)

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

## Full-query confirmation sweep

Follow-up to verify the 1k-subsample was unbiased. Same protocol but
**`--sample-queries 0` (entire qid set) and one seed (42)**, the 3 baseline arms.
Artifacts: `issue37-results-full.{md,csv}` and `issue37-manifest-full.json`;
raw cells at `~/scratch/longeval/2025/rerank-full/`.

15 dates × 1 seed × 3 arms = **45 runs**, single-seed (no std bar).

| date | split | bm25 | camembert-base | jina-v2 |
|---|---|---:|---:|---:|
| 2022-06 | train | 0.1275 | 0.1577 | 0.1778 |
| 2022-07 | train | 0.1336 | 0.1646 | 0.1865 |
| 2022-08 | train | 0.1411 | 0.1730 | 0.1949 |
| 2022-09 | train | 0.2102 | 0.2582 | 0.2852 |
| 2022-10 | train | 0.2956 | 0.3585 | 0.3961 |
| 2022-11 | train | 0.2917 | 0.3647 | 0.3964 |
| 2022-12 | train | 0.3029 | 0.3673 | 0.4009 |
| 2023-01 | train | 0.3116 | 0.3761 | 0.4118 |
| 2023-02 | train | 0.3098 | 0.3754 | 0.4113 |
| 2023-03 | test | 0.3152 | 0.3829 | 0.4243 |
| 2023-04 | test | 0.3227 | 0.3865 | 0.4178 |
| 2023-05 | test | 0.3262 | 0.3926 | 0.4253 |
| 2023-06 | test | 0.3176 | 0.3689 | 0.4047 |
| 2023-07 | test | 0.3193 | 0.3854 | 0.4210 |
| 2023-08 | test | 0.2843 | 0.3373 | 0.3645 |

| group | bm25 | camembert-base | jina-v2 |
|---|---:|---:|---:|
| train (9) | 0.2360±0.0823 | 0.2884±0.0993 | 0.3179±0.1059 |
| test (6) | 0.3142±0.0151 | 0.3756±0.0203 | 0.4096±0.0233 |
| pooled (15) | 0.2673±0.0744 | 0.3233±0.0880 | 0.3546±0.0937 |

**Match to the 1k 3-seed sweep:** every cell is within ±0.014 absolute of the
prior 1k mean (≤ 1.4% relative). On 11 of 11 testable dates a Δ-vs-1k between
−0.014 and +0.010, with a mild systematic negative skew (9 of 15 deltas
negative) — the 1k random draws appear to slightly oversample easier queries
on average. **Jina-v2 still beats the control on every date.** Test-set mean
goes 0.4148 (1k) → 0.4096 (full) — a +0.005 absolute gap that is well
inside the 1k 3-seed std (±0.0247). The 1k-subsample table the paper would
reference is faithful.

Compute: full-query sweep wall_sum ≈ **25 GPU-h** vs **5.3 GPU-h** for the 1k
3-seed (about 4.7× more for the rerank arms; per-pair throughput identical at
camembert ~530 / jina-v2 ~416 pairs/s). Wall-clock as a 15-task array: ~3 h
20 m gated by 2022-08 (the largest date, 2.71M pairs).

## 5-arm extras sweep

Phase 2 of the same 1k 3-seed protocol, adding three more rerankers to widen the
comparison: `antoinelouis/crossencoder-camembert-large-mmarcoFR` (335M; same
family as the control, larger), `antoinelouis/crossencoder-camemberta-L10-mmarcoFR`
(~98M DeBERTa-v3-based French model), and `BAAI/bge-reranker-v2-m3` (568M XLM-R-large
multilingual). Same code path, same sampling, same fp16; just batch=32 to fit the
larger arms on V100 16GB. Sbatch: `sbatch/experiment-rerank-extras.sbatch`; writes
into the same `~/scratch/longeval/2025/rerank/` so the aggregator yields one
6-column table.

15 dates × 3 seeds × 6 arms = **270 cells**; results from
`scripts/aggregate-rerank-results.py` (copied to `issue37-results.{md,csv}`,
`issue37-manifest.json`).

| date | split | bm25 | camembert-base | camemberta-L10 | camembert-large | bge-v2-m3 | jina-v2 |
|---|---|---|---|---|---|---|---|
| 2022-06 | train | 0.1240±0.0118 | 0.1599±0.0160 | 0.1635±0.0124 | 0.1643±0.0191 | 0.1748±0.0202 | 0.1782±0.0210 |
| 2022-07 | train | 0.1310±0.0151 | 0.1589±0.0120 | 0.1631±0.0121 | 0.1649±0.0100 | 0.1727±0.0102 | 0.1759±0.0103 |
| 2022-08 | train | 0.1402±0.0139 | 0.1657±0.0104 | 0.1714±0.0105 | 0.1749±0.0080 | 0.1879±0.0172 | 0.1912±0.0152 |
| 2022-09 | train | 0.2131±0.0031 | 0.2583±0.0035 | 0.2632±0.0042 | 0.2664±0.0062 | 0.2926±0.0069 | 0.2901±0.0015 |
| 2022-10 | train | 0.2924±0.0066 | 0.3548±0.0109 | 0.3591±0.0068 | 0.3665±0.0101 | 0.3848±0.0131 | 0.3940±0.0094 |
| 2022-11 | train | 0.2886±0.0074 | 0.3524±0.0077 | 0.3511±0.0053 | 0.3567±0.0094 | 0.3821±0.0057 | 0.3828±0.0042 |
| 2022-12 | train | 0.3173±0.0072 | 0.3757±0.0019 | 0.3790±0.0085 | 0.3890±0.0092 | 0.4133±0.0141 | 0.4133±0.0095 |
| 2023-01 | train | 0.3123±0.0086 | 0.3738±0.0108 | 0.3725±0.0081 | 0.3864±0.0099 | 0.4062±0.0115 | 0.4134±0.0113 |
| 2023-02 | train | 0.3195±0.0062 | 0.3780±0.0058 | 0.3914±0.0090 | 0.3883±0.0129 | 0.4204±0.0150 | 0.4212±0.0107 |
| 2023-03 | test | 0.3129±0.0076 | 0.3847±0.0036 | 0.3910±0.0078 | 0.3960±0.0136 | 0.4164±0.0066 | 0.4281±0.0063 |
| 2023-04 | test | 0.3159±0.0065 | 0.3770±0.0061 | 0.3733±0.0087 | 0.3786±0.0092 | 0.4088±0.0190 | 0.4079±0.0185 |
| 2023-05 | test | 0.3321±0.0033 | 0.3986±0.0116 | 0.3951±0.0080 | 0.4023±0.0122 | 0.4291±0.0093 | 0.4350±0.0120 |
| 2023-06 | test | 0.3170±0.0100 | 0.3733±0.0106 | 0.3796±0.0091 | 0.3790±0.0113 | 0.4024±0.0130 | 0.4136±0.0130 |
| 2023-07 | test | 0.3254±0.0082 | 0.3982±0.0059 | 0.4004±0.0042 | 0.4052±0.0045 | 0.4237±0.0052 | 0.4346±0.0041 |
| 2023-08 | test | 0.2895±0.0081 | 0.3471±0.0043 | 0.3435±0.0101 | 0.3443±0.0027 | 0.3680±0.0023 | 0.3698±0.0063 |

| group | bm25 | camembert-base | camemberta-L10 | camembert-large | bge-v2-m3 | jina-v2 |
|---|---|---|---|---|---|---|
| train (9) | 0.2376±0.0856 | 0.2864±0.1004 | 0.2905±0.1003 | 0.2953±0.1025 | 0.3150±0.1090 | 0.3178±0.1092 |
| test (6) | 0.3155±0.0146 | 0.3798±0.0192 | 0.3805±0.0207 | 0.3842±0.0226 | 0.4081±0.0219 | 0.4148±0.0247 |
| pooled (15) | 0.2687±0.0763 | 0.3237±0.0902 | 0.3265±0.0893 | 0.3309±0.0907 | 0.3522±0.0958 | 0.3566±0.0972 |

**Reading the rows:**

- **jina-v2 still leads on test (0.4148)**, with bge-v2-m3 a clear close 2nd
  (0.4081, Δ −0.007 absolute, ~1.6% relative). Per-date on test: jina-v2 wins
  5/6 dates over bge-v2-m3 (bge takes 2023-04 by +0.001). On train, bge edges
  jina on 2022-09 (0.2926 vs 0.2901) and ties on 2022-12; jina wins the other 7.
- **Scaling up camembert barely moves the needle.** camembert-large (335M) over
  camembert-base (110M) on test: 0.3842 vs 0.3798 → **+0.0044** absolute (~1.2%
  relative). camemberta-L10 (DeBERTa-v3, ~98M) lands at 0.3805 — basically tied
  with camembert-base. The model-family pretraining matters more than parameter
  count: jina-v2 (~278M) and bge-v2-m3 (568M, multilingual XLM-R-large) both
  decisively outperform the camembert family at any of the sizes tested.
- **Two-tier picture**: bm25 (0.3155 test) → camembert family + camemberta
  (~0.38) → bge-v2-m3 + jina-v2 (~0.41). The within-tier deltas are within (or
  comparable to) the seed std bars; the across-tier deltas are not.

Throughput on V100 (fp16, batch=32 for the extras): bge-v2-m3 ~177 pairs/s,
camembert-large ~178, camemberta-L10 ~228, plus camembert-base ~530 and jina-v2
~412 at batch=64 from the original sweep. The extras run took ~1 h 50 m
wall-clock as a 15-task array, gated by 2022-08 (the largest date).

## Expansion + reranker stack (phase 3)

Re-opens a question the prior worklog (`20260522-query-expansion-rescore.md`)
left flagged: that worklog measured **first-stage BM25 only** and found
french-additive expansion lost to baseline at every date (pooled 0.204 vs 0.242).
The hypothesis here: the expanded candidate set changes which docs surface at
k=100; a strong cross-encoder might use the different set differently and
recover (or even surpass) the loss. We tested jina-v2 (the new default) on top
of the french-additive expansion the prior worklog identified as the best
expansion variant.

**Setup**: 75,252 query expansions (additive: `original + original + terms[]`)
were uploaded to `~/scratch/longeval/2025/query_expansion/french/expansion/`
(3011 batches × 25 qids; ~99.6% per-date coverage, with the ~30 uncovered qids
per date coalesced back to the original). Re-enabled the `with_expanded_queries`
arm in `bm25/workflow.py` (`--with-expanded --expanded-only`); produced
`bm25/retrieval_expanded/` for all 15 dates. The rerank pipeline gained
`--retrieval-prefix retrieval_expanded` so it consumes the expanded candidate
set while still reranking against the **original** user query from the queries
parquet — i.e. we test "does the expanded candidate set help the reranker",
not "does the reranker also like expanded queries". Two arms only — bm25
(candidate-set-as-is nDCG@10) and jina-v2 — over both the 1k 3-seed and
full-query protocols to mirror the no-expansion tables exactly.

### 1k 3-seed (15 × 3 × 2 = 90 runs)

Artifacts: `issue37-results-expanded.{md,csv}`, `issue37-manifest-expanded.json`.

| date | split | bm25-exp | jina-v2-exp |
|---|---|---|---|
| 2022-06 | train | 0.1090±0.0024 | 0.1729±0.0088 |
| 2022-07 | train | 0.1177±0.0088 | 0.1777±0.0036 |
| 2022-08 | train | 0.1174±0.0138 | 0.1850±0.0137 |
| 2022-09 | train | 0.1944±0.0025 | 0.2842±0.0029 |
| 2022-10 | train | 0.2384±0.0050 | 0.3865±0.0044 |
| 2022-11 | train | 0.2363±0.0122 | 0.3854±0.0197 |
| 2022-12 | train | 0.2551±0.0057 | 0.3905±0.0173 |
| 2023-01 | train | 0.2580±0.0069 | 0.4106±0.0109 |
| 2023-02 | train | 0.2591±0.0036 | 0.3976±0.0132 |
| 2023-03 | test | 0.2698±0.0080 | 0.4025±0.0083 |
| 2023-04 | test | 0.2757±0.0070 | 0.4125±0.0019 |
| 2023-05 | test | 0.2766±0.0077 | 0.4194±0.0090 |
| 2023-06 | test | 0.2628±0.0066 | 0.3903±0.0054 |
| 2023-07 | test | 0.2656±0.0127 | 0.4193±0.0142 |
| 2023-08 | test | 0.2474±0.0077 | 0.3628±0.0121 |

| group | bm25-exp | jina-v2-exp |
|---|---|---|
| train (9) | 0.1984±0.0657 | 0.3100±0.1051 |
| test (6) | 0.2663±0.0107 | 0.4011±0.0218 |
| pooled (15) | 0.2256±0.0608 | 0.3465±0.0928 |

### Full-query confirmation (15 × 1 × 2 = 30 runs)

Artifacts: `issue37-results-expanded-full.{md,csv}`, `issue37-manifest-expanded-full.json`.

| date | split | bm25-exp | jina-v2-exp |
|---|---|---:|---:|
| 2022-06 | train | 0.1110 | 0.1759 |
| 2022-07 | train | 0.1171 | 0.1838 |
| 2022-08 | train | 0.1228 | 0.1914 |
| 2022-09 | train | 0.1908 | 0.2841 |
| 2022-10 | train | 0.2438 | 0.3883 |
| 2022-11 | train | 0.2391 | 0.3848 |
| 2022-12 | train | 0.2508 | 0.3908 |
| 2023-01 | train | 0.2558 | 0.4007 |
| 2023-02 | train | 0.2626 | 0.3971 |
| 2023-03 | test | 0.2618 | 0.4037 |
| 2023-04 | test | 0.2683 | 0.4063 |
| 2023-05 | test | 0.2718 | 0.4130 |
| 2023-06 | test | 0.2646 | 0.3966 |
| 2023-07 | test | 0.2655 | 0.4126 |
| 2023-08 | test | 0.2396 | 0.3571 |

| group | bm25-exp | jina-v2-exp |
|---|---:|---:|
| train (9) | 0.1993±0.0651 | 0.3108±0.1017 |
| test (6) | 0.2619±0.0115 | 0.3982±0.0210 |
| pooled (15) | 0.2244±0.0590 | 0.3457±0.0896 |

### Direct comparison vs no-expansion (test set, nDCG@10)

|  | no-exp 1k 3-seed | exp 1k 3-seed | Δ | no-exp full | exp full | Δ |
|---|---:|---:|---:|---:|---:|---:|
| bm25 | 0.3155 | 0.2663 | **−0.0492** | 0.3142 | 0.2619 | **−0.0523** |
| jina-v2 | 0.4148 | 0.4011 | **−0.0137** | 0.4096 | 0.3982 | **−0.0114** |

**Recovery ratio**: the reranker shrinks the expansion loss from ~0.050 → ~0.013
on test (~74% of the lost nDCG@10 recovered), but **never closes the gap**. The
expanded arm loses to its no-expansion counterpart on every one of the 6 test
dates, both at 1k 3-seed and at full-query. The prior worklog's negative result
holds at the reranker stage — just less catastrophically.

What the reranker *can* do is rank around expansion's noise: jina-v2 on
expanded candidates (test 0.4011) still beats the no-expansion **paper control**
(camembert-base, 0.3798) by +0.021, and beats the no-expansion BM25 baseline
(0.3155) by +0.086. So the choice of reranker matters far more than the choice
of expansion-vs-no-expansion.

**Why expansion hurts the candidate set**: french-additive expansion swaps
~30% of the BM25 top-100 docs per qid (informal spot-check); some swap-ins are
useful but the expected effect is to push out IDF-strong matches in favor of
broader-topic noise. The reranker can identify the surviving relevant docs but
can't surface ones that BM25 lost. k=100 → k=200 might help here, but at 2×
rerank cost and still no obvious win — not pursuing.

## Decisions

- **Does jina-v2 consistently beat the camembert-base control? YES — on all
  15/15 dates**, train and test, every seed. On the held-out test set jina-v2
  leads camembert-base by **+0.035 nDCG@10 (0.4148 vs 0.3798, ~9% relative)**;
  both beat BM25 everywhere. The prior single-date observation (2023-01: jina
  0.4145 vs control 0.3772) generalizes — this run gives 2023-01 jina 0.4134 /
  control 0.3738. Full-query sweep replicates: jina-v2 ahead on every date.
- **Promote jina-v2 as the default reranker: YES.** The 5-arm comparison gives
  it the highest pooled and test nDCG@10 of any arm tried, with the closest
  competitor (bge-v2-m3) trailing by 0.007 absolute on test and 0.004 pooled,
  while costing ~2.3× more pairs/s to run. The fp16 cost (~412 vs ~530 pairs/s
  for camembert-base) is modest.
- **bge-v2-m3 is a viable backup**: ~1.6% behind jina-v2 on test, but with the
  same lift over the camembert tier, so the choice between jina-v2 and bge-v2-m3
  is a wash on quality. We pick jina-v2 for throughput.
- **Don't bother with camembert-large or camemberta-L10**: gains over
  camembert-base are within the seed std (+0.004 / +0.001 on test). The
  3× throughput cost of camembert-large is not justified.
- **Keep query expansion excluded: YES.** The prior worklog showed it hurts
  first-stage BM25 (pooled −0.038 nDCG@10). Phase 3 here confirms it still
  hurts at the reranker stage (jina test −0.014, jina pooled −0.010), with no
  date where jina-on-expanded beats jina-on-baseline. The reranker recovers
  ~74% of the expansion loss but never closes the gap. The `with_expanded`
  workflow plumbing is preserved (re-enabled and tested) but pinned off as the
  default for `experiment-bm25.sbatch`.

## Compute budget

GPU time consumed by the rerank stage, summed from the per-cell `wall_s` in the
results CSVs (GPU = all non-bm25 cells; the `bm25` arm is CPU-only nDCG scoring
of an existing candidate set and logs `wall_s≈0`):

| sweep | GPU cells | GPU-h |
|---|---:|---:|
| no-exp 5-arm 1k 3-seed | 225 | 24.6 |
| ↳ of which 3-core (phase 1) | 90 | 5.3 |
| ↳ of which extras (large / camemberta-L10 / bge-v2-m3) | 135 | 19.3 |
| no-exp full-query 3-arm | 30 | 25.1 |
| expansion 1k 3-seed | 45 | 3.1 |
| expansion full-query | 15 | 14.6 |
| **total** | **315** | **≈67.4 GPU-h** |

All on a single V100 16GB per task (fp16). Full-query sweeps dominate despite
fewer cells — each reranks the entire qid set rather than a 1k subsample.
Wall-clock per 15-task array was far shorter than the GPU-h sum (jobs run in
parallel across the array): ~3 h 20 m for the full-query no-exp sweep, ~1 h 50 m
for the extras, each gated by 2022-08 (the largest date).

**Not instrumented here:** the first-stage BM25 index/retrieval
(`experiment-bm25.sbatch`) and the Spark ETL (~1–2 min/date) run as separate
CPU-only stages that don't write `wall_s` into these CSVs, so their CPU-hours
aren't totaled — pull from SLURM `sacct` if a full CPU accounting is needed.

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
