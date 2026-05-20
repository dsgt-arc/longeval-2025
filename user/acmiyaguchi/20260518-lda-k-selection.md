# LDA topic-count (K) selection for the pooled 9-slice LongEval corpus

**Date:** 2026-05-18
**Author:** acmiyaguchi (analysis run via Claude Code)
**TL;DR:** Swept K = 2…10 on the full 16.26 M-doc pooled corpus. **K = 4 is the
recommended topic count** for the drift analysis: it is the global coherence /
diversity optimum, every topic is a distinct non-redundant register, and it
gives the cleanest decomposition of the Dec-2022 regime step. The earlier
"best of {5,10,20}" pick of K = 5 is superseded. K = 6–8 is a confirmed
coherence basin; the K ≥ 9 NPMI uptick is a scrape-junk artifact, not real
structure.

**Model presented (paper):** the reported K = 4 model and all §5 numbers are
the **converged production retrain** (`maxIter=50`, full corpus, fixed
`local[8]`, 2026-05-19, root `/mnt/data/tmp/lda-k4-converged`). The K-selection
sweep of §3 ran at `maxIter=10` (comparable to the prior `{5,10,20}` production
grid); the K = 4 choice and the drift conclusion are **invariant to
convergence** — see §5/§6.

---

## 1. Question

The production sweep only evaluated K ∈ {5, 10, 20}. On that coarse grid K = 5
looked best for the longitudinal drift work, but the grid could not say whether
the true optimum was lower (3–4) or whether K = 10's low coherence was a real
basin or noise. This sweep fills in K = 2…10 to settle it.

(K = 1 is excluded: Spark MLlib LDA requires k > 1, and a single topic is
degenerate — every document θ = 1.0, drift identically zero.)

## 2. Method

- **Corpus:** `/mnt/data/tmp/longeval-train-parquet`, 9 monthly slices
  2022-06 … 2023-02, pooled via the `date="all"` sentinel,
  `--sample-fraction 1.0` → 16,263,471 deduped non-empty docs.
- **Model:** the §3 sweep ran online LDA at `maxIter=10`, `seed=42`, default
  online params — identical to the `lda-all9-clean-sweep` production run, so the
  curve is directly comparable to it. The selected **K = 4 was then retrained at
  the converged `maxIter=50`** (the value proven sufficient by the K=5/10/20
  maxIter=50 A/B; K≥50 tail proven unneeded) for the model reported in §5.
- **Incremental-sweep trick (no preprocess recompute):** the expensive
  K-independent half (native FR analyze + CountVectorizer over 16 M docs,
  multi-hour) was reused by hardlinking the validated sweep's `preprocess/`
  into a fresh root:

  ```bash
  mkdir /mnt/data/tmp/lda-k2-10-sweep
  cp -al /mnt/data/tmp/lda-all9-clean-sweep/preprocess \
         /mnt/data/tmp/lda-k2-10-sweep/preprocess
  PYSPARK_DRIVER_CORES=8 PYSPARK_DRIVER_MEMORY=36g \
    longeval etl lda-sweep "2,3,4,5,6,7,8,9,10" \
      /mnt/data/tmp/longeval-train-parquet /mnt/data/tmp/lda-k2-10-sweep \
      --date all --sample-fraction 1.0
  ```

  `_stamp_matches` / `_preprocess_hash` are pure functions of params (not
  paths), so identical stamps make `MinePhrases`/`BuildFeatures`
  short-circuit `complete()` and only per-K Train/Eval/Infer/TopicProportions
  run. Non-destructive: the validated root is untouched.
- **Validity check:** k5 and k10 NPMI reproduced the original {5,10,20} run to
  4 decimals (k5 0.3165 / 0.980 / 0.3102; k10 0.2711 / 0.950 / 0.2575) — the
  hardlink reuse is exact and the run is deterministic.
- **Wall clock:** rc=0 in 4 h 31 m (preprocess reused; the per-K NPMI
  coherence passes over the full corpus dominate).

## 3. Results — the K = 2…10 curve

`mean_npmi` = doc-level NPMI coherence; `diversity` = fraction of unique top
words across topics; `coh×div` = combined; `mean|Δθ|` = mean over topics of
the first→last-slice proportion shift.

| K | mean_npmi | diversity | coh×div | mean\|Δθ\| | max\|Δθ\| |
|---|-----------|-----------|---------|-----------|-----------|
| 2 | 0.245 | 1.000 | 0.245 | 0.051 | 0.051 |
| 3 | 0.330 | 1.000 | 0.330 | 0.036 | 0.053 |
| **4** | **0.351** | **1.000** | **0.351** | 0.030 | 0.059 |
| 5 | 0.317 | 0.980 | 0.310 | 0.032 | 0.080 |
| 6 | 0.224 | 0.967 | 0.217 | 0.020 | 0.048 |
| 7 | 0.211 | 0.971 | 0.205 | 0.018 | 0.048 |
| 8 | 0.233 | 0.963 | 0.224 | 0.016 | 0.044 |
| 9 | 0.273 | 0.956 | 0.261 | 0.019 | 0.083 |
| 10 | 0.271 | 0.950 | 0.258 | 0.015 | 0.071 |

Shape: rise 2→4, **peak at K = 4**, decline into a **basin at K = 6–8**
(global min 0.211 @ K = 7), partial NPMI recovery K = 9–10.

*Converged cross-check:* re-running the selected K = 4 at `maxIter=50` gives
NPMI **0.354** / diversity **1.000** / coh×div **0.354** (vs 0.351 / 1.000 /
0.351 at `maxIter=10`) — the peak and the perfect diversity are unchanged, so
the K = 4 selection does not depend on training depth.

## 4. Why K = 4

1. **Four real registers, zero redundancy.** Diversity = 1.000 → no two
   topics share a top word. Each is a nameable register; no sink, duplicate,
   or scrape-junk topic. This stops being true at K ≥ 5 (diversity falls as
   boilerplate splits into near-duplicates) and collapses at K = 6–8.
2. **Honest global optimum.** K = 4 is the peak of both NPMI and coh×div over
   the whole sweep, *without* the hyper-coherent calendar/country-list junk
   topic that artificially lifts NPMI at K = 9–10 and K = 20.
3. **Resolves what K = 3 leaves merged.** K = 3 mashes the entire French side
   into one blob; K = 4 splits it into the two French registers that drift
   differently (local-services vs editorial/boilerplate) without the
   over-split K = 5 introduces.
4. **Cleanest read of the Dec-2022 step** (Section 5).

**Why not the neighbours:**

- **K = 3** — defensible conservative fallback (also diversity 1.0, NPMI
  0.330) but leaves all French content as one undifferentiated topic; too
  coarse to attribute French drift to local-services vs editorial.
- **K = 5** — splits T3 into editorial vs e-commerce/GDPR boilerplate. That
  split is *real and drifts differently*, but it costs coherence
  (0.351 → 0.317) and introduces the first redundancy (diversity 1.0 → 0.98).
  Use K = 5 only if that extra drift axis is analytically required.
- **K ≥ 6** — ruled out: confirmed coherence basin; the corpus has no 6th–8th
  distinct register, so extra capacity fragments existing topics.
- **K ≥ 9** — the NPMI uptick is the scrape/calendar junk-topic artifact
  (month-pair / country-list razors), not better structure. Ignore it.

**NPMI caveat (honest).** Prior work established `mean_npmi` is *not* a
reliable K-selector on this corpus (it is confounded by hyper-coherent junk
topics — the K ≥ 9 / K = 20 "recovery" is exactly that). The K = 4 call
therefore rests primarily on **diversity = 1.0 + topic content + drift
interpretability**; NPMI only corroborates because at low K there are no
surviving junk razors to confound it.

## 5. The K = 4 model (converged `maxIter=50`, 2026-05-19)

All numbers below are from the converged production retrain
(`/mnt/data/tmp/lda-k4-converged`, NPMI 0.354 / diversity 1.000). Top words
(glued bigrams are NPMI phrases; `done_person`→*données personnelles*,
`mot_pase`→*mot de passe*):

| Topic | Register | Mass | Top words |
|---|---|---|---|
| **T0** | English film/awards + generic EN web | 25.0 % | `one, film, award_best, time, year, united_stat, new_york, academy_award, post, best` (+ `eurovision_song, song_contest, johny_depp`) |
| **T1** | FR local services: hotel / real-estate / municipal | 10.4 % | `nuit_nuit, lire_suit, hotel, centr_vile, sale_bain, conseil_municipal, anul_gratuit, chambr, reserv, petit_dejeun` |
| **T2** | online-pharmacy spam | 4.1 % | `onlin_pharmacy, onlin_apothek, treatment_erectil, farmac_onlin, pharmac_lign, viagra, canadian_pharmacy, ciali` (+ glued month-pair SEO tokens) |
| **T3** | FR editorial + commercial/GDPR boilerplate | 60.5 % | `peut_être, site_web, savoi_plu, produit, doit_être, mot_pase, done_person, travail, entrepris, vie, mond` |

Cleaner than the `maxIter=10` sweep model: at iter=10 a stray `mot_pase` (a T3
GDPR token) leaked into T0's top-25 — gone at convergence. Registers and
diversity (= 1.000) otherwise identical.

### Per-slice mean θ (9 train slices)

| date | T0 EN-film | T1 FR-local | T2 pharma | T3 FR-editorial |
|---|---|---|---|---|
| 2022-06 | 0.237 | 0.105 | 0.037 | 0.621 |
| 2022-07 | 0.238 | 0.104 | 0.037 | 0.620 |
| 2022-08 | 0.237 | 0.104 | 0.037 | 0.621 |
| 2022-09 | 0.222 | 0.110 | 0.036 | 0.632 |
| 2022-10 | 0.233 | 0.104 | 0.041 | 0.622 |
| 2022-11 | 0.231 | 0.104 | 0.041 | 0.624 |
| **2022-12** | **0.283** | 0.102 | 0.045 | **0.569** |
| 2023-01 | 0.282 | 0.102 | 0.045 | 0.570 |
| 2023-02 | 0.286 | 0.102 | 0.045 | 0.566 |

### Drift summary (first → last train slice)

| topic | mean θ | first | last | Δ first→last | std |
|---|---|---|---|---|---|
| T0 EN-film | 0.250 | 0.237 | 0.286 | **+0.049** | 0.024 |
| T1 FR-local | 0.104 | 0.105 | 0.102 | −0.002 | 0.002 |
| T2 pharma | 0.041 | 0.037 | 0.045 | +0.008 | 0.004 |
| T3 FR-editorial | 0.605 | 0.621 | 0.566 | **−0.054** | 0.026 |

**Finding:** a discrete, near-conserved English↑ / French-editorial↓ mass swap.
T0/T3 are flat for 2022-06…11, **step-change at 2022-12**, then hold flat
through 2023-02. T1 and T2 are essentially stationary. The signature is
consistent with a discrete crawl/extraction pipeline change in the collection
at Dec-2022, not organic topic drift.

**Robust to convergence:** the `maxIter=10` sweep model gave T0 +0.051 /
T3 −0.059 (vs +0.049 / −0.054 here) with the identical step at 2022-12 and the
same isolated 2022-09 T0 dip. Direction, sign, step location, and magnitudes
(within ~0.005) are unchanged from 10→50 iters — the longitudinal conclusion
does not depend on training depth.

### Held-out 15-slice validation (2023-03…08) — converged, 2026-05-19

`lda-heldout-drift` applied the **converged** K=4 model/vocab/phrases to the 6
deferred test slices (rc=0, ~36 m;
`/mnt/data/tmp/lda-k4-converged/heldout/topic_{proportions,drift}_15slice.parquet`).

| date | T0 EN-film | T1 FR-local | T2 pharma | T3 FR-editorial |
|---|---|---|---|---|
| 2022-06…11 | ~0.23 | ~0.105 | ~0.04 | ~0.62 |
| **2022-12** | **0.283** | 0.102 | 0.045 | **0.569** |
| 2023-01…02 | 0.28 | 0.102 | 0.045 | 0.57 |
| 2023-03 | 0.287 | 0.100 | 0.044 | 0.568 |
| 2023-04 | 0.282 | 0.101 | 0.044 | 0.573 |
| 2023-05 | 0.284 | 0.101 | 0.044 | 0.571 |
| 2023-06 | 0.273 | 0.104 | 0.044 | 0.578 |
| 2023-07 | 0.283 | 0.101 | 0.044 | 0.572 |
| 2023-08 | 0.273 | 0.104 | 0.044 | 0.579 |

**Confirmed:** the Dec-2022 step is a *one-time discrete shift followed by a
9-slice stationary plateau* (Dec-2022 → Aug-2023, ≤±0.013 per register across
the held-out window). T0/T3 never revert toward their pre-step values; T1/T2
flat throughout. 15-slice drift: T0 +0.036 / T3 −0.042 (near-mirror,
mass-conserving with T2 +0.006), T1 −0.001; std (~0.024–0.026 for T0/T3) is
entirely the single Dec-2022 step, not ongoing variance. 2022-09 T0 dip
remains an isolated single-slice anomaly; a milder T0 dip recurs at 2023-06 and
2023-08 (0.273) but does not break the plateau. Matches the `maxIter=10`
held-out signature (T0 +0.038 / T3 −0.046) and the K=5/10/20 signature
exactly. Conclusion: a discrete crawl/extraction-pipeline change at Dec-2022,
not organic drift, and it does not decay across the 8-month test period.

## 6. Caveats & limitations

- **Convergence (resolved, now a robustness result):** the reported K = 4
  model is the converged `maxIter=50` retrain (§5). The `maxIter=10` sweep
  model gives the same registers, the same Dec-2022 step, and drift within
  ~0.005 (train T0 +0.051 vs +0.049; held-out T0 +0.038 vs +0.036) — the
  longitudinal conclusion is invariant to training depth. Convergence only
  sharpens NPMI/readability (T0 GDPR-token leak removed).
- The K = 4 held-out 15-slice validation (2023-03…08) is **done on the
  converged model** (§5, 2026-05-19): the Dec-2022 step holds with no decay
  across the 8-month test period.
- NPMI is unreliable for K-selection on this corpus (see §4 caveat); do not
  argmax it in isolation.
- Online-LDA mini-batch sampling is not bit-reproducible across core counts;
  every comparison here (sweep, converged retrain, held-out, {5,10,20}
  cross-check) held core count fixed at `local[8]`.

## 7. Artifacts & reproduction

- **Converged production model (the one the paper reports — §5):**
  `/mnt/data/tmp/lda-k4-converged/`
  - `k4/lda_model`, `k4/topicWords_lda.txt`, `k4/docTopicDistribution_lda.parquet`
  - `k4/_{train,eval,inference,topicprop}_config.json` — frozen param record
    (`max_iter=50, seed=42, k=4, sample_fraction=1.0, min_df=50, max_df=0.2,
    vocab=20000, phrases 20/0.5/0.95/99.9, learning_offset=1024,
    learning_decay=0.51, subsampling_rate=0.05, optimize_doc_concentration=True`)
  - `sweep_coherence.parquet`, `topic_proportions.parquet`, `topic_drift.parquet`
  - `heldout/topic_{proportions,drift}_15slice.parquet` — converged 15-slice (§5)
  - preprocess hardlinked from the validated root (same blocks; `_config.json`
    inode verified identical)
  - Log/relaunch: `/mnt/data/tmp/lda-k4-converged.log`,
    `/mnt/data/tmp/lda-k4-converged-relaunch.sh`
  - **Durable archive (paper provenance):**
    `/mnt/data/research/arc/longeval-lda-k4-converged-archive/` — 43 M compact
    bundle on durable nvme (model + frozen vocab/phrase feature space +
    paper-number aggregates + held-out tables + log + `MANIFEST.md` +
    `SHA256SUMS`). The 243 M per-doc θ and 33 G preprocess are excluded
    (regenerable). NB: `/mnt/data/tmp` here is a plain dir on the same durable
    nvme (not tmpfs) — the archive guards against scratch housekeeping, not
    reboot loss. GCS push to `gs://dsgt-longeval-2025/` pending explicit
    go-ahead (off-host copy).
- **Selection sweep root (`maxIter=10`, §3 curve):** `/mnt/data/tmp/lda-k2-10-sweep/`
  - `sweep_coherence.parquet`, `topic_proportions.parquet`, `topic_drift.parquet`
  - `k{2..10}/topicWords_lda.txt`, `k{2..10}/lda_model`, `docTopicDistribution_lda.parquet`
  - `heldout/topic_{proportions,drift}_15slice.parquet` — `maxIter=10` K=4 15-slice
  - Log/relaunch: `/mnt/data/tmp/lda-k2-10-sweep.log`,
    `/mnt/data/tmp/lda-k2-10-relaunch.sh`; held-out
    `/mnt/data/tmp/lda-k2-10-heldout-k4.{log,relaunch.sh}`
- **Validated preprocess root (untouched):** `/mnt/data/tmp/lda-all9-clean-sweep`
- Commands: §3 sweep §2; converged K=4
  `longeval etl lda-sweep "4" /mnt/data/tmp/longeval-train-parquet /mnt/data/tmp/lda-k4-converged --date all --sample-fraction 1.0 --max-iter 50`;
  converged held-out
  `longeval etl lda-heldout-drift /mnt/data/tmp/longeval-test-parquet /mnt/data/tmp/lda-k4-converged --k-values 4`

## 8. Next steps

1. ~~Run `lda-heldout-drift` for K = 4~~ — **DONE (§5, 2026-05-19):** Dec-2022
   step confirmed, holds with no decay across the 6 test slices.
2. Adopt K = 4 as the topic basis for the retrieval-augmentation work
   (4 register features per doc); the IR scoring/metric design hole is still
   open and orthogonal to K choice.
