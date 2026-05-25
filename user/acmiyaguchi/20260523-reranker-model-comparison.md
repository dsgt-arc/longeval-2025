# Reranker model comparison — CLEF Working Notes

**Date opened:** 2026-05-23 · **Owner:** acmiyaguchi (driven via Claude Code)

Reproduces the rerank stage of the CLEF Working Notes pipeline
(`../longeval-2025-working-notes`, `results_system.tex`) and sweeps alternative
French / multilingual cross-encoders over the same BM25 candidate set, to answer:
(a) does the rerank arm reproduce the paper's 0.379 on 2023-01, and (b) does a
bigger French-native or a modern multilingual reranker beat the paper's model?

Follows up the reranker-unpinned gap flagged in
`20260522-bm25-baseline-repro.md` (reproducibility tier table, bm25-reranked row).

---

## Which model is the paper's "French-specific reranker"?

The pipeline config (`user/acmiyaguchi/rerank/reranking-config.yml`) only names a
generic `model_name: "cross-encoder"`, `model_type: "cross-encoder"`, `lang: "fr"`.
The actual checkpoint is resolved by the **`rerankers` library** (pinned
`rerankers==0.10.0` in `uv.lock`). Its `DEFAULTS` map:

```
"cross-encoder": { "en": <minilm>, "fr": "antoinelouis/crossencoder-camembert-base-mmarcoFR",
                   "other": "corrius/cross-encoder-mmarco-mMiniLMv2-L12-H384-v1" }
```

So `"cross-encoder" + lang="fr"` → **`antoinelouis/crossencoder-camembert-base-mmarcoFR`**
(CamemBERT-base cross-encoder fine-tuned on mMARCO-French). This is *not* pinned in
repo config — it depends on the lib version. Pinning resolved here.

## Models swept (single date, 2023-01 first)

| # | Model | Backbone / params | Role |
|---|---|---|---|
| 1 | `antoinelouis/crossencoder-camembert-base-mmarcoFR` | CamemBERT-base ~110M | **control = paper model** |
| 2 | `antoinelouis/crossencoder-camembert-large-mmarcoFR` | CamemBERT-large ~335M | French-native, bigger |
| 3 | `antoinelouis/crossencoder-camemberta-L10-mmarcoFR` | CamemBERTa (DeBERTa-v3) 10-layer | French-native, stronger arch |
| 4 | `BAAI/bge-reranker-v2-m3` | XLM-R-large ~560M | multilingual SOTA |
| 5 | `jinaai/jina-reranker-v2-base-multilingual` | ~280M | multilingual, fast |

Same training data (mMARCO-FR) holds across 1–3, so those isolate backbone size/arch;
4–5 test whether a generic multilingual reranker beats a French-native one on French web.

## Result (2023-01, 1000-query subsample seed=42, seq-512) — DONE

Paper reference: bm25 2023-01 = 0.312, bm25-reranked = **0.379**.

| Model | nDCG@10 | std | vs control |
|---|---|---|---|
| bm25 (candidate-set baseline) | 0.3182 | 0.356 | — (paper 0.312 ✅) |
| crossencoder-camembert-base (control) | 0.3772 | 0.375 | — (paper 0.379 ✅) |
| crossencoder-camemberta-L10 | 0.3846 | 0.373 | +0.007 |
| crossencoder-camembert-large | 0.3964 | 0.378 | +0.019 |
| bge-reranker-v2-m3 (560M) | 0.4116 | 0.376 | +0.034 |
| **jina-reranker-v2-base-multilingual** (280M) | **0.4145** | 0.380 | **+0.037 ⭐** |

bge-v2-m3 added 2026-05-24 (batch-32, ~75 min). It's the heaviest model yet
*not* the best — **jina-v2 wins at half bge's params and ~2x faster**, the clear
practical pick.

### Findings

1. **Harness is faithful.** Control 0.3772 ≈ paper 0.379; bm25 0.3182 ≈ paper 0.312.
   (The 200-query smoke gave 0.3511 — pure sample noise; n=1000 converged.)
2. **A modern multilingual reranker beats the French-native one.** jina-v2 wins by
   **+0.037 (+9.9% rel)** over the paper's camembert-base, at *fewer* params
   (280M vs camembert-large's 335M) — and ran fine on Pascal (no flash-attn).
3. **Among French-native models, bigger helps:** large (0.3964) > camemberta-L10
   (0.3846) > base (0.3772). All beat BM25 (0.3182).

### Query expansion + rerank (2026-05-24)

Does reranking recover the expansion loss? (Best expansion = French-additive
[[project_query_expansion_redesign]], `~/scratch/longeval/query_expansion/french`;
expanded query drives both retrieval and the CE — paper's expanded-reranked
semantics. 1000 queries, seed=42.)

| stage | non-expanded | expanded | Δ |
|---|---|---|---|
| bm25 | 0.3182 | 0.2522 | −0.066 |
| camembert-base | 0.3772 | 0.3309 | −0.046 |
| jina-v2 | 0.4145 | 0.3606 | −0.054 |

**No — the reranker does not recover it; expansion hurts at every stage.** If
expansion had only reordered relevant docs *within* the top-100, the CE would
pull them back and expanded-reranked ≈ non-expanded-reranked. It's ~0.05 lower
under both rerankers, so expansion pushed relevant docs *out* of the top-100
(unrecoverable — first-stage recall, not just ranking, is the bottleneck).
Consistent with the paper's own 2023-01 per-date (reranked 0.379 → expanded-
reranked 0.352, −0.027); extends the negative-expansion result through rerank.
Practical takeaway: **don't expand; jina-v2 on original queries (0.4145) is the
config to beat.** Driver: `--expanded-root`.

> Single date, single subsample. To promote any of these to the paper, the next
> step is the full 15-date eval (DAG) on the winner(s) — bearing in mind full-16k
> per date is ~10 h/model on this GPU (see throughput below), so a better GPU or a
> committed subsample protocol is needed for the full longitudinal sweep.

## Throughput reality (GTX 1080 Ti, fp32, seq-512)

The 1080 Ti reranks camembert-base at **~110 query-doc pairs/sec** — genuinely
compute-bound (Pascal has no usable fp16; web docs fill the 512-token window).
This is the binding constraint, not query dispatch:

- Full 16k × top-100 = 1.6M pairs ⇒ **~10 h for camembert-base alone** → full-set
  reproduction of the paper's 0.379 is **infeasible on this GPU**. A full-16k
  control run was attempted and killed at 82 min, ~15% done.
- Speed knobs are query-count, params, and `max_length` — NOT batching.
- Flattened `CrossEncoder.predict` (below) did not beat the per-query async path
  on speed (both GPU-bound); it's kept only for simplicity.

## Method

- **Driver:** `scripts/validate-rerank-single-date.py` (mirrors the BM25 single-date
  validator). Shared prep runs ONCE; the model list is swept over the same candidates.
  1. **Index:** `BM25IndexFromTrecTask` from raw TREC (reused if present).
  2. **Retrieve:** `run_search`, top-100, raw per-slice query TSV.
  3. **Contents join:** `TrecCollection(dates=[date]).documents` → `docid, contents`,
     dedup to one row per docid (longest, `len>50`), inner-joined onto the top-100.
     Replaces the pipeline's `join_retrieval.py` (which needs the full parquet ETL).
  4. **Rerank:** `sentence_transformers.CrossEncoder(model, max_length=512,
     trust_remote_code=True)`, all (query, contents) pairs flattened into one
     `.predict(batch_size=64)`. (The pipeline's `reranker.py` uses
     `rerankers.Reranker` per-query `rank_async`; scores are identical, so the
     framework choice is a throughput detail, not a fidelity one.)
  5. **Score:** strip `^doc` from docid, `score_search` (pytrec_eval, NDCG@10) vs raw
     `qrels_processed.txt`.
- **Faithfulness note:** cross-encoder scores are batch/concurrency-invariant, so async
  overlap and `batch_size` change wall-clock only, not the metric.
- **GPU memory:** each `Reranker` is `del`'d + `torch.cuda.empty_cache()` between models
  so weights don't accumulate across the sweep (one model on-device at a time).

### Environment gotcha (NixOS)

The PyPI torch wheel (`2.7.0+cu126`) can't find the driver's `libcuda.so` by default —
`torch.cuda.is_available()` is False and `nvidia-smi` works, which looks like a sandbox
issue but is the NixOS driver-path problem. Fix: run with
`LD_LIBRARY_PATH=/run/opengl-driver/lib`. The GTX 1080 Ti (Pascal sm_61, 11 GB) is
supported by this build once the lib is found.

## Runtime reality

2023-01 has **16,007 queries** × top-100 ≈ 1.6M query-doc pairs. Even GPU-saturated
(100% util, ~6.7 GB at concurrency 16 / batch 50), camembert-base takes ~22 min for the
rerank stage alone. Full 5-model sweep ≈ **3–4 h**, scaling ~linearly with param count
(bge-v2-m3 is the long pole). Prep (index + retrieve + join) is ~4 min and shared.

## Artifact map

| What | Path |
|---|---|
| Single-date sweep driver | `scripts/validate-rerank-single-date.py` |
| Run log (2023-01) | `/mnt/data/tmp/rerank-sweep-2023-01.log` |
| TREC index (reused) | `/mnt/data/tmp/longeval-bm25-validate/index_trec/date=2023-01/` |
| HF model cache | `~/.cache/huggingface/hub/` |
