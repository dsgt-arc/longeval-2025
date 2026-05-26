# LDA K=4 work — running worklog / artifact index

**Date opened:** 2026-05-19 · **Owner:** acmiyaguchi (driven via Claude Code)

This is the **tracker** for the K=4 topic-drift line of work (and its K=20
companion): status, where every artifact lives, what's been established and
verified, and what's still open. The **formal analysis** (numbers, tables,
argument) lives in [`20260518-lda-k-selection.md`](20260518-lda-k-selection.md);
this doc does not duplicate it — it points at it.

---

## Status snapshot

- **K=4 converged model — DONE & archived.** The CLEF Working Notes model is
  the converged `maxIter=50` retrain. All three referee gaps (under-training,
  volatile artifacts, reproducibility) closed.
- **K=20 converged companion — DONE & archived** (2026-05-19, ~2 h 12 m
  wall, NPMI 0.3723 / div 0.95). Matched 1:1 to the K=4 config (identical
  training params except K); held-out 15-slice reproduces the Dec-2022 step.
- **K4×K20 document-level attribution — DONE** (2026-05-19, ~16 m wall, 4×20
  crosstab over 16.26 M docs; idx-keyed via `monotonically_increasing_id`,
  join landed at exactly N=16,263,471). Writeup:
  [`20260519-lda-k4xk20-attribution.md`](20260519-lda-k4xk20-attribution.md).
  Decisively settles the Dec-2022 mechanism (Finding #7 below).
- **Analysis-doc fold-ins — PENDING** (3 items, see Open).
- **GCS off-host push — PENDING explicit go-ahead.**

## Artifact map (canonical "where is everything")

| What | Path |
|---|---|
| **K=4 converged model (PAPER)** | `/mnt/data/tmp/lda-k4-converged/` (log `lda-k4-converged.log`, script `lda-k4-converged-relaunch.sh`) |
| **K=4 durable archive** | `/mnt/data/research/arc/longeval-lda-k4-converged-archive/` (`MANIFEST.md`, `SHA256SUMS`, 43 M) |
| K=4 top-500 words/topic | `/mnt/data/tmp/lda-k4-converged/k4/topicWords_top500.txt` |
| **K=20 converged model** | `/mnt/data/tmp/lda-k20-converged/` (log `lda-k20-converged.log`, script `lda-k20-converged-relaunch.sh`) |
| **K=20 durable archive** | `/mnt/data/research/arc/longeval-lda-k20-converged-archive/` (`MANIFEST.md`, `SHA256SUMS`, 45 M) |
| **K4×K20 crosstab analysis** | [`user/acmiyaguchi/20260519-lda-k4xk20-attribution.md`](20260519-lda-k4xk20-attribution.md) |
| K4×K20 crosstab JSON + intermediates | `/mnt/data/tmp/k4xk20-{crosstab.json,k4-dom.parquet,k20-dom.parquet,crosstab.sh,crosstab.log}` |
| Selection sweep K=2..10 (`maxIter=10`) | `/mnt/data/tmp/lda-k2-10-sweep/` |
| Validated preprocess root (untouched) | `/mnt/data/tmp/lda-all9-clean-sweep/` (K=5/10/20 `maxIter=10` + held-out) |
| Conv-sweep K=5/10/20 (`maxIter=50`, **offset=64**) | `/mnt/data/tmp/lda-conv-sweep/` (NOT config-matched to K=4) |
| Formal analysis writeup | `user/acmiyaguchi/20260518-lda-k-selection.md` |
| Train corpus (9 slice, 16,263,471) | `/mnt/data/tmp/longeval-train-parquet` |
| Test corpus (6 slice, 10,909,437; 15-slice total 27,172,908) | `/mnt/data/tmp/longeval-test-parquet` |

NB `/mnt/data/tmp` here is a plain dir on durable nvme (not tmpfs) — survives
reboot; archive guards against scratch housekeeping, not power loss.

## Findings established (this session) — one-liners, detail in the analysis doc

1. **Converged K=4 ≈ maxIter=10 ⇒ robustness, not a caveat.** NPMI 0.3542 /
   div 1.000; train drift T0 +0.049 / T3 −0.054; held-out 15-slice T0 +0.036 /
   T3 −0.042; identical Dec-2022 step; topics *cleaner* (T0 GDPR-token leak
   gone). Drift conclusion invariant to training depth 10↔50.
2. **"More English articles" is FALSE.** Corpus is **100% French-labeled at
   every one of the 15 slices** — no English partition exists, language mix is
   perfectly invariant. T0 is English-language *content inside the French
   crawl*, not English documents. The Dec-2022 step is a per-document
   extraction/crawl change, not a language-composition shift. Topic step (Dec)
   is also decoupled from the doc-volume step (Aug→Oct) — reinforces pipeline,
   not organic.
3. **Topics hold at depth-500.** 4 registers verified to rank 500; inter-topic
   Jaccard ≤0.06 for all pairs **except T1∩T3 = 0.157** (the expected soft
   French-French seam). T2 is the **scrape/SEO + spam quarantine sink** (pharma
   at top; country/month/name razor chains in tail) — its existence protects
   the other three; it's the drop-candidate if topics feed retrieval.
4. **Exact corpus proportions (doc-weighted, N=16,263,471), two views:**
   - Mean-θ mass: T0 **25.24%** · T1 **10.38%** · T2 **4.12%** · T3 **60.23%**
   - Dominant-topic (argmax) doc share: T0 24.36% (3.96 M) · T1 8.80%
     (1.43 M) · T2 3.89% (0.63 M) · T3 **62.95% (10.24 M)**
   - Mass≈argmax ⇒ documents are concentrated, not blended. T1 mass>argmax
     (frequent *secondary* topic inside T3-dominant docs).
   - 15-slice (27.17 M): T0 26.39 / T1 10.29 / T2 4.23 / T3 59.06 — the
     persistent Dec-2022 step showing through 2023.
5. **Time series = three distinct shapes.** T0↔T3: discrete one-time Dec-2022
   step then a flat 9-slice plateau to Aug-2023 (no decay). T1: stationary
   (±0.005). T2: slow secular creep (0.037→0.045 over 2022) — independent
   spam/scrape accumulation, not a step. 2022-09 is an all-topic anomaly
   co-located with the low-volume slice (1.08 M) — artifact, not trend.
6. **K=20 localizes the K=4 mechanism (paper-relevant refinement).** The
   K=4 T3 −5.4 pp decomposes mainly into **T16 generic French editorial/
   narrative prose (Δ −0.048)** and T3 French politico-economic editorial
   (Δ −0.024), offset by smaller gains in T14 e-commerce (+0.018) and T13
   admin/legal/municipal (+0.012). The K=4 T0 +5 pp comes mainly from
   **T8 generic English web prose (Δ +0.030)** plus T11 English country-
   names (+0.008). **GDPR/cookie boilerplate (T19, 2.5 % mass) is
   stationary (Δ +0.002)** — the obvious "boilerplate-stripping" hypothesis
   is *wrong*. The shift is between substantive prose registers, not
   boilerplate. Combined with the language-invariance finding, this points
   to a crawl/extraction change that started surfacing English-language
   pages and de-emphasizing substantive-FR-prose pages within the same
   100 %-French-labeled crawl. K=20 held-out L1 mass-shift = 0.2011; T16↔T8
   alone is 39 % of it. Junk-quarantine confirmed at ~3 topics (T15
   FR-month-chains, T12 DB-field/debug, T18 EN-month-chains), ~5 % combined.
7. **K4×K20 document-level attribution (paper headline).** The 10.24 M
   K=4-T3-dominant docs (63 % of the corpus) decompose empirically as:
   **38 % K=20-T16 substantive FR editorial prose**, 20 % T14 FR e-commerce,
   17 % T3 FR politico-economic editorial, 11 % T13 FR admin/legal/municipal,
   5 % T10 forum/classified, 3 % T4 cookie/cart, **3 % T19 GDPR/privacy**,
   ~4 % other. So **K=4 T3 is ~70 % substantive French content and only ~6 %
   boilerplate** — the boilerplate-dominated reading is wrong by an order of
   magnitude. K=4 T0 is 76 % K=20-T8 (single tight register); K=4 T1 is the
   most heterogeneous macro (top fine topic only 22 %); K=4 T2 is the
   confirmed scrape-junk quarantine. K=4↔K=20 is a legitimate coarsening:
   11 of 20 fine topics have ≥85 % purity in a single K=4 macro. Detail:
   [`20260519-lda-k4xk20-attribution.md`](20260519-lda-k4xk20-attribution.md).

## Verification log

- Converged-vs-iter10 A/B: reproduced (Δ ≤ ~0.005). ✅
- Hardlink preprocess reuse: `_config.json` inode identical → MinePhrases/
  BuildFeatures short-circuited (K=4 and K=20 both). ✅
- Language×date count over train+test: 100% French, 15/15 slices. ✅
- describeTopics(500) × 20k vocab: registers + depth-diversity. ✅
- Doc-weighted mass via two independent paths (slice-aggregated vs raw per-doc
  avg) agree to the decimal. ✅
- Archive integrity: `SHA256SUMS` over model + vector_model + phrases + all
  parquet parts. ✅

## K=20 companion — rationale

We already have converged K=20 in `lda-conv-sweep`, but at
`learning_offset=64` (the conv-probe schedule), **not** matched to
`lda-k4-converged` (default `offset=1024`). A fresh K=20 at the *identical*
K=4 config makes the pair directly comparable (required for a meaningful
K4×K20 crosstab) and gets the same archive/provenance treatment. The
`conv-sweep` K=20 and the `all9-clean` `maxIter=10` K=20 remain as
independent cross-checks.

## Open / next

1. ~~**K=20 converged:** train → verify → archive~~ — **DONE 2026-05-19**
   (Findings #6 above + `longeval-lda-k20-converged-archive/MANIFEST.md`).
2. ~~**K4×K20 attribution crosstab** — global (no date)~~ — **DONE
   2026-05-19** (Finding #7 + writeup `20260519-lda-k4xk20-attribution.md`).
   Date-stratified version (pre/post Dec-2022 crosstab at document level)
   still open but largely pre-empted by the K=20 mean-θ movers analysis —
   would only add per-document re-routing detail; defer pending need.
3. **Fold into `20260518-lda-k-selection.md`:** (a) language-invariance
   evidence into §5/§6 (kills "more English" reading); (b) depth-500
   separation + T2-as-scrape-sink into §5; (c) exact doc-weighted
   proportions + dominant-topic column — §5 table currently lists rounded
   25.0/10.4/4.1/60.5, replace with exact 25.24/10.38/4.12/60.23.
4. **GCS push** of both archives to `gs://dsgt-longeval-2025/` — outward-
   facing, awaiting explicit go-ahead.
5. *(deferred, orthogonal)* retrieval-augmentation IR scoring/metric design.

## Decision log

- K=4 = primary/headline (div 1.000, 4 separable registers, clean drift).
- K=20 = supplementary lens only (coherence-basin region, div ~0.93, partly
  junk-inflated NPMI) — to decompose the 60% T3 blob, never the headline.
- Converged retrain (not ship iter-10): closes the under-training referee gap;
  turned out to *strengthen* the result (robustness).
- Fresh K=20 at K=4 config (not reuse conv-sweep offset=64): comparability.
