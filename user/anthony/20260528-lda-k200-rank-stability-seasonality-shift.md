# K=200 — rank stability, seasonality null, why the Dec-2022 step

Date: 2026-05-28

## TL;DR

- **Top-20 is a near-closed set at K=200.** 18 of 20 topics sit in the Top-20 at
  every one of the 15 slices; only 21 distinct topics ever appear in Top-20 at any
  slice. **Exactly one swap** across the whole window (T42 FR given-names out,
  T139 EN hotel/travel commerce in). The Dec-2022 step is rank-reshuffling
  **inside** that closed set, not promotion from below.
- **No monthly seasonality is detectable in this window.** Detrend (3-month
  rolling median) → residual → year-over-year co-spike test returns 0/3 confirmed
  candidates. Even maximally seasonal-vocabulary topics (T101 saint-valentin,
  T68 fête-mère, T155 academy_award, T82 père_noël) show flat-plateau θ inside
  each regime, no calendar peaks. With 14 monthly slices and a dominant Dec-2022
  step, monthly seasonality lives below the noise floor.
- **The Dec-2022 step is *inside* the training pool, not at the train/test
  boundary** (train = 2022-06..2023-02, test = 2023-03..2023-08). That rules out
  "dataset construction at the split boundary" as the cause. Leading hypotheses
  are now (A) Qwant crawler/extractor update, (B) Qwant ranking/index change,
  (C) intra-batch LongEval release switchover.

See [`20260527-lda-k200-worklog.md`](20260527-lda-k200-worklog.md) for the K=200
training run + register-pattern table; see
[`20260528-lda-k-granularity-saturation.md`](20260528-lda-k-granularity-saturation.md)
for the K=4 / K=20 / K=200 saturation framing; see
[`20260528-corpus-diagnostics-numbers.md`](20260528-corpus-diagnostics-numbers.md)
for the consolidated per-slice number tables (rank/θ matrices, B1/B2/B3 full,
top-hash counts).

## Top-20 / Top-50 rank stability at K=200

Computed from the held-out 15-slice inference at
`/mnt/data/tmp/lda-k200-converged/heldout/topic_proportions_15slice.parquet`,
ranking topics by mean θ within each slice.

| metric | value |
|---|---|
| Top-20 *core* (in Top-20 at every one of 15 slices) | **18 / 20** |
| Top-20 *union* (in Top-20 at any one slice) | **21 / 200** |
| Top-50 *core* | 42 / 50 |
| Top-50 *union* | 62 / 200 |
| Topic at rank 1 in every slice | T47 (FR generic prose: `peut_être, quelqu_chos, faut, alor`) |
| T47 θ drift through the window | 0.0544 → **0.0405** at Dec-2022 → 0.0416 plateau |

**Top-5 trajectory** (the visible step is internal):

```
2022-06 to 2022-08:  T47 T16  T198 T48  T122
2022-09:             T47 T16  T198 T122 T48
2022-10 to 2022-11:  T47 T16  T198 T48  T89    ← T122 climate leaves Top-5
2022-12 onward:      T47 T198 T48  T89  T16   ← T16 history demoted #2 → #5; locked for 9 mo
```

**The only Top-20 swap across 15 slices** (rank 21 vs 20 boundary):

| direction | topic | rank 2022-06 → 2023-08 | top words |
|---|---|---|---|
| ↓ out | **T42** | 14 → 29 | `jean, jean_pier, mar, jean_claud, michel, jean_francoi` (FR given-name biographical) |
| ↑ in | **T139** | 21 → 8 | `pric, home, room, sale, city, area, park, view, hous` (EN hotel/travel commerce) |

**Within-Top-20 reshufflers** (no entry/exit, but big rank moves):

- T122 climate / sustainable-dev: rank 5 → 12, θ 0.0281 → 0.0169
- T44 FR energy-transition / Élisabeth Borne: rank 9 → 19, θ 0.0192 → 0.0150
- T16 FR history/literature: rank 2 → 5, θ 0.0346 → 0.0254
- T40 EN medical/pharma: rank 20 → 7, θ 0.0127 → 0.0183
- T89 EN business/government news: rank 6 → 4, θ 0.0274 → 0.0313
- T185 EN cookie/privacy boilerplate: rank 19 → 14, θ 0.0132 → 0.0164

Outside Top-20, larger rank moves happen (T145 FR hotel-booking boilerplate
183 → 34, T84 EN geographic enumeration 52 → 26) but on small absolute mass —
peripheral, not part of the head-of-leaderboard story.

## Monthly seasonality probe — negative result

Question: do any K=200 topics show calendar-month spikes (Saint-Valentin Feb,
Fête des Mères May, Oscars Mar, Noël Dec, etc.)?

### The algorithmic recipe (reusable)

```
INPUT: heldout topic_proportions_15slice.parquet  (K × 15 monthly means)

1. DETREND
   For each topic: local_baseline = rolling_median(θ, window=3, center=True)
                   residual       = θ − local_baseline
   (rolling median is robust to single-month spikes AND absorbs the Dec-2022
    step over 3 slices, unlike a simple pre/post-regime mean which over-detects
    Sep–Nov 2022 as "seasonal".)

2. SCORE candidates
   For each topic:
     peak_date     = argmax_d |residual(d)|
     rel_excursion = |peak_residual| / max(θ)
   Filter: typical θ > 0.001  AND  rel_excursion > 0.20

3. DISCRIMINATE seasonal vs step-transition
   YoY co-spike test: for each candidate peaking in {Jun, Jul, Aug},
     require sign(resid_2022) == sign(resid_2023)
            AND  |resid_2022|, |resid_2023| > 10% of typical θ
   (kills Dec-2022-transition false positives because Sep–Nov peaks in 2022
    have no 2023 counterpart.)

4. SEMANTIC SANITY CHECK
   For each surviving candidate, read topicWords_lda.txt top-15 terms.
   Also flag the inverse — high-seasonal-vocab topics that FAIL the residual
   test — that's the interesting null finding.

5. (Requires N ≥ 24 months) STL decomposition
   statsmodels.tsa.seasonal_decompose(θ_t, period=12, model='additive')
   → clean trend/seasonal/residual split, no regime-step kludges.
```

### Result on K=200

| step | output |
|---|---|
| Naive pre/post-regime detrend | 70 candidates, **35 peaking in Sep 2022 alone** — clearly misclassified step-transition |
| Rolling-median detrend (window=3) | 18 candidates, all still clustered in Sep 2022 |
| YoY co-spike test on Jun/Jul/Aug pairs | **0 hits** out of 3 testable months |

### Label-driven probe (the clincher)

Topics whose top words are maximally seasonal — checked against θ-by-month:

| topic | vocabulary | expected peak | observed θ shape |
|---|---|---|---|
| T101 | `fleu, bouquet_fleu, saint_valentin` | Feb | step DOWN at Dec-2022 (0.0043 → 0.0019), **flat through Feb 2023** |
| T68  | `fête_mère, maman` | late May | step DOWN (0.0026 → 0.0005), **flat through May 2023** |
| T155 | `academy_award, film_wining` | Mar | step UP (0.0009 → 0.0028), then **0.0028 ± 0.0001 for 9 months** including Mar 2023 |
| T82  | `père_noël, xvie_siecl` | Dec | step DOWN at the Dec we observe; no Dec bump either year |
| T78/T190 | `vote, election_legislatif` | once-off | sharp drop after Sep 2022 — **French legislative election (Jun 2022), single event**, not annual |

The vocabulary encodes seasonality because the *documents* have seasonal content.
But the document **mix** at the corpus level is dominated by always-on web content,
so the topic's θ is set by steady-state mass and barely flexes month to month.

### Scope limit (the honest part)

15 monthly slices spanning Jun 2022 → Aug 2023 with a one-month transition at
Dec 2022:

- **Only Jun/Jul/Aug have year-over-year pairs** → only 3 calendar months are
  even testable for true annual periodicity
- **Sep–Nov are 1 observation each** → cannot distinguish "September seasonal"
  from "single-month event" from "step lead-in"
- **Dec 2022 — Aug 2023** are all 1 observation each on the post-step plateau →
  no within-regime seasonal signal observable

Verdict: **K=200 resolves register/content shifts but not calendar cycles, given
this window**. Recoverable signal would need ≥ 24 months and STL/X-13 decomposition.

## Why would topic registers shift over time? — hypothesis ranking

The shape that needs explaining:

- **Discrete one-month step at Dec 2022**, flat plateau on both sides
- **No `language` label change** — 100 % FR-labelled at every slice (see Finding #2
  in [`../acmiyaguchi/20260519-lda-k4-worklog.md`](../acmiyaguchi/20260519-lda-k4-worklog.md))
- **Doc and query sides decoupled** — document θ steps at Dec-2022; query θ shifts
  at Sep-2022 along different topics (Figure 2 of the paper draft)
- **Down-movers are substantive FR prose** (literature, history, climate, politics,
  cinema, biographical)
- **Up-movers are EN web content + commerce/legal boilerplate** (hotel booking,
  cookie banners, privacy policies)

Ranked mechanisms:

1. **Qwant crawler/extractor update (leading)** — a new HTML extractor deployed
   around Dec 2022 that retains chrome (booking forms, GDPR banners, footer
   privacy text) where the previous version stripped it. The 10× jump in T145
   (`centr_vile, anul_gratuit, recomandon_reserv`) from 0.0008 → 0.0081 is exactly
   an extractor-tell. Single-month discreteness fits a deploy event.

2. **Qwant ranking / index change (close second)** — Qwant publicly restructured
   in late 2022 (leadership exit, Microsoft renegotiation). If the ranker started
   surfacing more EN/Bing-syndicated results for the same FR queries, doc mix
   would shift even with constant queries. **Argues against:** would usually be
   gradual across multiple deploys, not a one-month step.

3. **LongEval 2024 → 2025 re-collection pipeline difference** — confirmed by the
   2024 Readme PDFs: LongEval 2024 covered **exactly 3 of our 15 slices**
   (Jan 2023 train: 2,049,729 docs; Jun + Aug 2023 test: 4,321,642 docs total —
   matches `id_urls_2023_{01,06,08}.txt` line counts to within 1). The other 12
   slices in our 2025 corpus (incl. Jun–Dec 2022) were re-collected by LongEval
   ~12–18 months later for the 2025 release. If the 2025 re-collection used a
   different click-model snapshot, query log slice, or HTML extractor than the
   original 2024 collection, that could plant a step somewhere in the 12 new
   slices. **Does not directly explain Dec-2022 specifically** — would expect
   the step at the 2024/2025 batch boundary, which is *not* Dec-2022 (Jan 2023
   was the 2024 train month). But could explain why the post-step plateau is so
   stable across the 9 newer slices.

4. ~~User-query mix shift~~ — **rejected**. Query θ shifts at Sep-2022 (3 months
   earlier) along different topics. If users drove document drift, the two sides
   would move in sync.

5. ~~Real-world content drift~~ — **rejected**. Genuine content drift is gradual
   and partial. A one-month step held flat for 9 months is not what content drift
   looks like.

6. ~~Dataset construction at train/test boundary~~ — **rejected by exploration
   today**. The boundary is Feb→Mar 2023, not Dec-2022. The step is inside the
   training pool.

## Diagnostics that would discriminate — results

Three source-data probes were designed alongside this note. Each is a single
Spark scan over the existing partitioned parquets at
`/mnt/data/tmp/longeval-{train,test}-parquet/`. All outputs to
`/mnt/data/tmp/corpus-diagnostics/<probe>/summary_by_date.csv`. Both runs used the
fused-groupBy pattern described in `corpus-diagnostics/run_diagnostics.py` (single
persist of a thin projection — `contents` dropped before cache so the budget is
~1.4 GB rather than 60–80 GB; total wall ~11 min for B1+B2+B3 fused, +12 min for
the B3 redo described below).

| probe | what it measures | expected if (A) extractor | expected if (B) ranking | expected if (C) batch switch |
|---|---|---|---|---|
| **B1** doc-length distribution per slice (mean/median/p5/p25/p75/p95) | shape change at Dec-2022 | step + new short-doc mode | weak | possible |
| **B2** within-slice content-hash uniqueness (`approx_count_distinct(sha256)/count`) | boilerplate retention rate | unique fraction drops sharply | no | possible |
| **B3** EN-stopword density per doc (`the, and, for, with, you, this, …`) | EN-content infiltration in FR-labelled docs | weak (most chrome is FR-language) | step up | possible |

### Observed (Oct/Nov 2022 vs Dec-2022 plateau)

| signal | pre-step (Oct/Nov 2022) | post-step (Dec-2022 onward) | step polarity |
|---|---|---|---|
| B1 char_mean | 5418 | 5054 | **−7 %** |
| B1 char_p50 | 4838 | 4364 | **−10 %** |
| B1 word_p50 | 760 | 685 | **−10 %** |
| B1 char_p95 | 10080 | 10080 | flat — long-tail unchanged |
| B2 unique_frac | 0.873 | 0.887 | +1.5 pp — **no boilerplate change** |
| B3 en_density_mean | 0.0244 | 0.0295 | **+21 %** |
| B3 frac_above_05 | 0.198 | 0.241 | **+22 %** |
| B3 frac_above_10 | 0.133 | 0.160 | **+20 %** |

The pre-step row deliberately uses Oct/Nov 2022 — they bracket the step in time and
are the slices with the closest sample sizes (~2.15 M docs) to the post-step
slices (~2.0 M docs), so the comparison is not confounded by N.

### Verdict

The triple-signal pattern is: **B1 step DOWN (more short docs, long tail unchanged) +
B3 step UP (more EN content) + B2 flat (no extra boilerplate)**. Mapping back to the
ranked hypothesis set:

- (A) extractor change: B1 fits, B3 unexpected for a pure extractor swap, B2 fits.
  Partial match. Survives only if the extractor change is specifically
  *"start retaining English-language results that were previously filtered."*
- (B) Qwant ranking / index-expansion (Bing/EN syndication uptake): fits all three.
  Search-result snippets are shorter (B1↓), Bing's index is EN-skewed (B3↑), and
  each syndicated URL is content-unique (B2 flat). **Leading interpretation.**
- (C) intra-batch release switchover: unfalsifiable from this evidence — any batch
  boundary that draws from a different source distribution could produce all three.
  Listed because it remains a viable mechanism, but the diagnostics don't actively
  rule it out *or* in.

Discriminating B vs C cleanly would need either (i) a per-doc TLD distribution
(blocked: no URL field for pre-step slices), (ii) per-doc language classifier (≈10×
B3 cost; ordering would have to be requested separately if needed), or (iii) the
LongEval organizers confirming a release-batch boundary at Dec-2022 (cheapest path).

### Note on B3 regex bug + fix

The first B3 pass returned 0.0 for every slice. Cause: the regex `\b(the|and|...)\b`
was assembled in Python and interpolated into a SQL expression via
`F.expr("regexp_count(contents, '(?i)\\b...\\b')")`. Spark SQL string-literal parsing
turns `\b` into the backspace character, so the regex never matched anything in
normal text. Fix: use the Column API form `F.regexp_count(F.col("contents"),
F.lit(raw_regex_string))`, which routes the regex string directly to the Pattern
compiler without SQL escape processing. The fixed B3 ran in 12 min wall on the same
hardware; results above. See `run_b3_fix.py` for the patched job.

## Lineage / files used

| What | Where |
|---|---|
| K=200 held-out topic proportions | `/mnt/data/tmp/lda-k200-converged/heldout/topic_proportions_15slice.parquet` |
| K=200 topic words (200 × 100 terms) | `/mnt/data/tmp/lda-k200-converged/k200/topicWords_lda.txt` |
| K=200 worklog + register pattern | [`20260527-lda-k200-worklog.md`](20260527-lda-k200-worklog.md) |
| Cross-K saturation summary | [`20260528-lda-k-granularity-saturation.md`](20260528-lda-k-granularity-saturation.md) |
| K=4 worklog (Finding #2: 100% FR labelled) | [`../acmiyaguchi/20260519-lda-k4-worklog.md`](../acmiyaguchi/20260519-lda-k4-worklog.md) |
| Source parquets (no URL field) | `/mnt/data/tmp/longeval-{train,test}-parquet/Documents/...` |
| Partial post-step URL mapping | `/mnt/fortis/clef-data/longeval/raw/id_urls_2023_{01,06,08}.txt` |
| Aggregation pattern to reuse | `longeval/etl/parquet/tokens.py:36-56` (`TokenTask`) |
| Diagnostic job (fused B1+B2+B3 + top-hashes) | `/mnt/data/tmp/corpus-diagnostics/run_diagnostics.py` |
| B3 regex-bug fix job | `/mnt/data/tmp/corpus-diagnostics/run_b3_fix.py` |
| Diagnostic CSVs | `/mnt/data/tmp/corpus-diagnostics/{doc-length,dedup,en-density}/summary_by_date.csv` |
| Top-20 boilerplate hashes per slice | `/mnt/data/tmp/corpus-diagnostics/dedup/top_hashes_by_date.parquet` |
