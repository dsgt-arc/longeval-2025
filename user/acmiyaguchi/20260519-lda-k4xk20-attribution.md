# K=4 → K=20 document-level attribution on the pooled LongEval corpus

**Date:** 2026-05-19
**Author:** acmiyaguchi (analysis run via Claude Code)
**Companion analyses:**
- K-selection / K=4 model: [`20260518-lda-k-selection.md`](20260518-lda-k-selection.md)
- Running worklog: [`20260519-lda-k4-worklog.md`](20260519-lda-k4-worklog.md)

**TL;DR.** Joint inference of the matched-config converged **K=4** and **K=20**
models on the full 16,263,471-doc training corpus produces a 4×20 document-
level crosstab that *empirically* decomposes each K=4 macro register into K=20
fine sub-registers. The headline result settles the mechanism interpretation
of the Dec-2022 step: **the 60% K=4-T3 "French-editorial" macro is not a
boilerplate register** — it is ~70% substantive French content (editorial
prose 38% + politico-economic 17% + admin/legal 11% + classified 5%) and only
~6% boilerplate (GDPR 3% + cookie/cart 3%). The Dec-2022 5-point drop in T3
is concentrated in its **substantive-prose** sub-population (K=20 T16, −0.048
mean θ at K=20), with the boilerplate sub-population (K=20 T19 GDPR)
**stationary**. The "boilerplate-stripping" hypothesis is empirically wrong by
roughly an order of magnitude.

K=4 is a legitimate coarsening of K=20: 11/20 fine topics align with one K=4
macro at ≥85% purity. The 5 weakly-aligned (<60%) K=20 topics are exactly
the registers you'd expect to span boundaries (proper-name dumps, place-name
registers, month-chains, film-awards SEO).

---

## 1. Question

The K=4 model gives the **interpretable headline drift story** (Dec-2022 mass
swap between T0 EN-content ↑ and T3 FR-editorial ↓, both mass-conserving and
held-out-validated). The K=20 model's *topic-level* movers analysis pinned
the step on K=20 T16 (FR editorial prose) ↔ T8 (EN web prose), with T19
(GDPR) explicitly **stationary**. That topic-level attribution is a
mean-θ statement: *"the average document's T16 share dropped 4.8 points."* It
does not tell us about *document populations* — how the 10.24 M documents
that K=4 calls T3-dominant actually decompose into K=20 fine registers, or
whether the K=4 T3 macro is dominated by prose or by boilerplate at the
document level.

This crosstab provides that document-level decomposition: a 4×20 contingency
table where row = K=4 dominant macro, column = K=20 dominant fine register,
and cell (i, j) = count of documents the K=4 model calls macro Tᵢ AND the
K=20 model calls fine Tⱼ.

## 2. Method

- **Models:** both converged `maxIter=50` LocalLDAModels with byte-identical
  configs (default online params, `learning_offset=1024`, `learning_decay=0.51`,
  `subsampling_rate=0.05`, `seed=42`, fixed `local[8]/36g`).
  - K=4: `/mnt/data/tmp/lda-k4-converged/k4/lda_model` ([archive](/mnt/data/research/arc/longeval-lda-k4-converged-archive/))
  - K=20: `/mnt/data/tmp/lda-k20-converged/k20/lda_model` ([archive](/mnt/data/research/arc/longeval-lda-k20-converged-archive/))
- **Input:** `features.parquet` (CountVectorizer output over the validated
  preprocess, 16,263,471 rows). Same physical file used by both models'
  training — guaranteed apples-to-apples document set.
- **Pairing problem:** the saved `docTopicDistribution_lda.parquet` outputs
  carry only `docid` (not `(docid, date)`), and `docid` is non-unique across
  months (3,415,926 distinct docids vs 16,263,471 rows ⇒ ~4.76 dates per
  docid on average). So a `docid`-only join between the two saved outputs
  would m-to-n explode and double-count. Solution: a **stable per-row index
  via `monotonically_increasing_id()`**, evaluated separately for each
  inference. Because both inferences read the same `features.parquet` file
  with the same partition layout, the generated indices match by row
  position. Verified empirically: the resulting inner join landed at
  **16,263,471 rows exactly**, i.e. zero leftover/extra — alignment perfect.
- **Pipeline:** two **sequential** single-K inferences (each at the proven-
  working memory profile from the original sweeps; chained dual-transform
  blew the heap on the per-document Breeze allocations during the variational
  E-step). For each model: read `features.parquet` → add `idx` → apply
  `LocalLDAModel.transform` → take argmax of the topic distribution → write
  `(idx, k{4,20}d)` to a tiny intermediate parquet. Then join on `idx` and
  groupBy.
- **Spark config:** `spark.sql.parquet.enableVectorizedReader=false` (the
  pipeline-standard fix for vectorized-reader heap OOMs on the sparse
  feature column — see [`project_lda_jvm_memory`]).
- **Wall clock:** K=4 inference 224 s + K=20 inference 721 s + join + agg =
  **~16 min total** at `local[8]/36g`, `rc=0`, `CROSSTAB_OK`.
- **Marginal sanity check:** the row sums (K=4 macro shares) and column
  sums (K=20 fine shares) of the joint count matrix reproduce the
  independently-reported dominant-topic shares to two decimals
  (T0 24.36/T1 8.80/T2 3.89/T3 62.95 %; K=20 T16 24.15 / T8 19.02 / T14 14.10
  /…). Confirms the joint matrix is internally consistent with the
  marginals.

## 3. Decomposition tables

Notation: cells in **thousands of documents**; row-norm % = fraction of the
K=4 macro's documents that have this K=20 fine topic as their argmax;
purity = column-normalized share of the K=20 fine in its K=4 home (≥85% =
"tight"; <60% = "boundary").

### K=4 T3 — FR editorial/GDPR (10,238 k docs · 62.95 %) — the 60 % blob, decomposed

| K=20 fine register | Top words (representative) | docs (k) | % of T3 | purity in T3 |
|---|---|---|---|---|
| **T16** generic FR editorial / narrative prose | `peut_être, mond, vie, quelqu, alor, plu_tard, foi, aprè_avoi, histoir, feme, livr, jeu` | **3,853** | **37.6 %** | **98.1 %** |
| **T14** FR e-commerce / transactional | `produit, livraison, comand, sale_bain, ofre, vent, client, qualit, cadeau` | 2,013 | 19.7 % | 87.8 % |
| **T3** FR politico-economic editorial | `chang_climat, projet, entrepris, econom, lute_contr, union_europen, milion_euro, energ_renouvelabl` | 1,704 | 16.6 % | 99.4 % |
| **T13** FR administrative / legal / municipal | `conseil_municipal, loi, impot, contrat, asuranc, profesion, credit_impot, 1er_janvi` | 1,102 | 10.8 % | 88.7 % |
| **T10** forum / classified / automotive | `date_inscription, mesag, aimej_aime, forum, voitur, vehicul, jeu, auto` | 545 | 5.3 % | 88.6 % |
| **T4** cookie / cart boilerplate | `ajout_pani, gogl_analytic, gdpr_cok, aceptez_util, user_consent, websit` | 307 | 3.0 % | 93.0 % |
| **T19** GDPR / privacy boilerplate | `site_web, mot_pase, done_person, polit_confidentialit, protection_done, mention_legal, exerc_droit` | **293** | **2.9 %** | **87.8 %** |
| T17 pharma (leak) | pharma spam | 48 | 0.5 % | — |
| Remaining 12 K=20 topics | — | ~378 | 3.7 % | — |
| **Totals** | | **10,238** | **100 %** | — |

**The key fact:** K=4 T3 — labelled "FR editorial / GDPR boilerplate" — is
empirically about **70 % substantive French content** (T16 prose 37.6 % + T3
politico-economic 16.6 % + T13 admin/legal 10.8 % + T10 classified 5.3 % =
70.3 %) and **only ~6 % boilerplate** (T4 cookie/cart 3.0 % + T19 GDPR 2.9 % =
5.9 %). The remaining ~20 % is FR e-commerce/transactional (T14) plus
small components. The original §5 (K=4 only) characterisation of T3 as
"editorial + commercial/GDPR boilerplate" was misleading — *boilerplate is a
minor sub-register, not the dominant one*. The macro is dominated by
substantive prose.

### K=4 T0 — EN film/web (3,962 k docs · 24.36 %) — the tightest macro

| K=20 fine | Top words | docs (k) | % of T0 | purity in T0 |
|---|---|---|---|---|
| **T8** generic EN web prose | `one, time, year, also, like, may, united_stat, day, post, work, year_old, said` | **3,000** | **75.7 %** | **97.0 %** |
| T18 EN month-chains / forum-time | `day_ago, year_ago, may_april, april_march, june_may, originaly_posted` | 259 | 6.5 % | 74.9 % |
| T0 FR film / forum-names | `film, jean, jean_pier, johny_depp, los_angel, star_war, jacqu` | 248 | 6.3 % | 55.8 % (boundary) |
| T11 EN country-names / geography | `island, virgin_island, new_zealand, czech_republic, puerto_rico` | 168 | 4.2 % | 84.2 % |
| T12 DB-field / mailing-list scrape | `parent_id_name, mailing_list, email_adres, amp_amp, boko` | 115 | 2.9 % | 65.7 % |
| Remaining | — | ~172 | 4.3 % | — |

K=4 T0 is the cleanest macro: **76 % is a single fine register** (K=20 T8
generic EN web prose). The +5 pt T0 rise at Dec-2022 is concentrated in this
one population (K=20 T8 Δ +0.030).

### K=4 T1 — FR local-services (1,431 k docs · 8.80 %) — the most heterogeneous

| K=20 fine | Top words | docs (k) | % of T1 |
|---|---|---|---|
| T1 hotel / booking | `nuit_nuit, hotel, chambr, taxe_sejou, petit_dejeun, location_voitur` | 317 | 22.1 % |
| T14 FR e-commerce / transactional | `produit, livraison, sale_bain` | 271 | 18.9 % |
| T9 hotel cancellation boilerplate | `anul_gratuit, period_incertitud, recomandon_reserv, coronaviru_covid` | 153 | 10.7 % |
| T13 FR admin / legal / municipal | `conseil_municipal, loi, impot` | 140 | 9.8 % |
| T0 FR film / forum-names | `jean_pier, johny_depp` | 120 | 8.4 % |
| T7 FR regions / place-names | `saint_deni, rhon_alpe, bouch_rhon, ile_franc` | 106 | 7.4 % |
| T18 EN month-chains | — | 70 | 4.9 % |
| T16 generic FR prose | — | 57 | 4.0 % |
| Remaining | — | ~198 | 13.8 % |

Top fine topic is only **22 %**. K=4 T1 is not a single register — it pools
hotel/booking, e-commerce, regions, admin, and proper-name dumps into a
"transactional-French / local" cluster. **Worth flagging as a caveat for
downstream uses:** treating K=4 T1 as a homogeneous feature is unsafe.

### K=4 T2 — spam / scrape quarantine sink (632 k docs · 3.89 %)

| K=20 fine | Top words | docs (k) | % of T2 |
|---|---|---|---|
| T17 pharma spam | `onlin_pharmacy, viagra, ciali, treatment_erectil` | 203 | 32.1 % |
| T2 FR country-names (alphabetical chains) | `île, pay_bas, afriqu_sud, royaum_uni, republ_democrat, arab_saoudit` | 147 | 23.2 % |
| T15 FR month-chains (calendar razor) | `janvi_decembr, juin_mai, novembr_octobr…` | 65 | 10.3 % |
| T10 forum / classified / automotive | — | 50 | 8.0 % |
| T7 FR regions / place-names | — | 47 | 7.4 % |
| T5 FR misc tail | — | 31 | 4.9 % |
| T11 EN country-names | — | 29 | 4.5 % |
| T6 FR film-awards (SEO) | — | 25 | 4.0 % |
| Remaining | — | ~37 | 5.8 % |

Confirms K=4 T2 = scrape-junk + spam aggregator: ~80 % of its mass is the
four expected junk families (pharma + country-chains + month-chains +
classified). It's a *bucket of distinct junks*, not a coherent register —
and that's the right behaviour for a 4 % "everything-else" macro.

## 4. Purity diagnostics — is K=4 a legitimate coarsening of K=20?

Counting K=20 fine topics by how cleanly they live inside one K=4 macro
(column-max purity from the col-normalised matrix):

| purity bucket | count | K=20 topics |
|---|---|---|
| **≥95 %** (very tight) | 4 | T3 (99.4), T8 (97.0), T16 (98.1), T2 (94.6 — just under) |
| **85–95 %** (tight) | 6 | T1 (91.9), T4 (93.0), T10 (88.6), T13 (88.7), T14 (87.8), T19 (87.8) |
| **80–85 %** | 1 | T11 (84.2) |
| **70–80 %** | 2 | T17 (79.4), T18 (74.9) |
| **60–70 %** | 2 | T9 (60.9), T12 (65.7) |
| **<60 %** (boundary) | 5 | T0 (55.8), T5 (44.8), T6 (45.0), T7 (49.4), T15 (47.5) |

**11/20 K=20 topics align with a single K=4 macro at ≥85 % purity.** That's
a solid coarsening signature — the two models are *not* just independent
clusterings of the same docs; the K=4 macros are approximately unions of
K=20 fine sub-populations. The hierarchy is "soft-nested," not strict.

### The 5 boundary topics

These are the K=20 topics whose document population is split across multiple
K=4 macros (purity < 60 %). They're not failures — they're the registers
that genuinely *span* macro boundaries:

| K=20 | What it is | T0 % | T1 % | T2 % | T3 % | what's going on |
|---|---|---|---|---|---|---|
| T0 | FR film / forum proper-names | 55.8 | 27.1 | 0.1 | 17.0 | proper-name register — jean / johny_depp / jean_pier appear in EN-film contexts, FR-hotel contexts, and FR-prose contexts |
| T5 | FR misc tail (coca_cola, etc.) | 22.4 | 44.8 | 25.4 | 7.3 | low-coherence tail topic, splits everywhere |
| T6 | FR film-awards (SEO listings) | 45.0 | 4.1 | 38.2 | 12.7 | **interesting:** ~38 % land in T2 spam-sink — heavy SEO-list film-awards pages look junky enough to quarantine |
| T7 | FR regions / place-names | 2.7 | 49.4 | 21.7 | 26.2 | place-names appear both transactionally (hotels) and editorially (prose) |
| T15 | FR month-chains (calendar razor) | 1.0 | 41.2 | 47.5 | 10.3 | month chains live in hotel calendars (T1) AND in SEO junk (T2) — not pure junk |

T15 in particular is informative: 41 % of "FR-month-chain" documents are
K=4-T1-dominant (i.e., legitimate hotel/local-services pages whose
secondary content includes calendar tables), not just spam. The
calendar-razor junk-topic interpretation is real but not exhaustive.

## 5. Dec-2022 mechanism — now at the document-population level

Combining this crosstab with the K=20 mean-θ movers (held-out 15-slice):

| K=20 sub-register inside K=4 T3 | docs (k) | % of T3 | Δ15slice (mean θ) | role at Dec-2022 |
|---|---|---|---|---|
| T16 substantive FR editorial prose | 3,853 | 37.6 | **−0.048** | **the dominant moving population** |
| T3 FR politico-economic editorial | 1,704 | 16.6 | **−0.024** | also drops |
| T14 FR e-commerce/transactional | 2,013 | 19.7 | +0.018 | partially compensates upward |
| T13 FR admin/legal/municipal | 1,102 | 10.8 | +0.012 | partially compensates upward |
| T19 GDPR / privacy boilerplate | 293 | 2.9 | **+0.002** | **stationary** |
| T4 cookie/cart boilerplate | 307 | 3.0 | +0.001 | stationary |

**Reading this:** the K=4 T3 −5.4 pt step is dominantly **prose-population
shrinkage** — substantive French editorial (T16) plus politico-economic
prose (T3) account for −0.072 of mass loss together. The e-commerce and
admin sub-populations (T14, T13) *rise* by +0.030 combined, partially
offsetting. **The two boilerplate sub-populations (T19 + T4 = 5.9 % of T3)
do not move at all.**

So at document-population resolution: at the Dec-2022 boundary, the corpus
**stopped surfacing substantive French editorial/prose pages** (or
equivalently, started surfacing more English-content pages — the EN T0 +5pt
rise is dominantly K=20 T8 substantive EN web prose). It did **not** strip
French GDPR or cookie boilerplate — those continued to be extracted at the
same per-document rate. Combined with the **100 %-French language label
invariance** across all 15 slices (no English partition, no language-mix
shift), the most parsimonious mechanism is a crawl/extraction-pipeline change
at Dec-2022 that altered *which kind of pages* were collected from the
same French-labeled crawl — favouring more English-content pages and fewer
substantive-FR-prose pages, while boilerplate-collection behaviour stayed
fixed.

This is a much sharper claim than the K=4-only analysis could support, and
it directly preempts the most likely referee question
("isn't this just GDPR-stripping?").

## 6. Caveats & limitations

- **Argmax discretisation.** Cells count documents by *dominant* topic only.
  A doc with K=4 θ = (0.45, 0.0, 0.0, 0.55) is "T3-dominant" but barely. A
  soft-mass crosstab (E[θ⁴ᵢ · θ²⁰ⱼ]) would be more principled but was the
  cause of the v1/v2 Catalyst OOMs; deferred. Argmax purity ≥ 85 % suggests
  documents are concentrated, not heavily blended (consistent with the
  earlier mass≈argmax finding for the K=4 marginals), so the soft version
  would tell a substantively similar story.
- **No date stratification.** This is the *global* crosstab over all
  16.26 M training documents. A pre-vs-post-Dec-2022 crosstab would let us
  watch the document populations literally re-route between cells at the
  boundary, which would close the mechanism story by document tracking. It
  requires re-routing date through the inference pipeline (BuildFeatures
  drops it); a separate effort. Mean-θ movers at K=20 (already done) cover
  the equivalent claim at topic resolution.
- **`monotonically_increasing_id()` reliance.** Stable only because both
  inferences read the same parquet with the same default partition layout.
  Verified post-hoc by the join landing at exactly 16,263,471. If anything
  re-partitions `features.parquet`, the trick breaks.
- **Boundary K=20 topics.** The 5 sub-60 % topics are split across K=4
  macros; doc counts in those rows are *one* possible attribution. For the
  T3 decomposition this is benign (no boundary topic has a major presence
  in T3) but for T1 it matters (boundary T0 and T7 contribute ~16 % of T1).

## 7. Artifacts & reproduction

- **Crosstab JSON** (counts, row-norm, col-norm):
  `/mnt/data/tmp/k4xk20-crosstab.json`
- **Intermediate dominant-pair parquets** (16.26 M rows, `idx + dom`):
  `/mnt/data/tmp/k4xk20-{k4-dom,k20-dom}.parquet`
- **Launch script** (full reproducer): `/mnt/data/tmp/k4xk20-crosstab.sh`
- **Run log:** `/mnt/data/tmp/k4xk20-crosstab.log` (`START 2026-05-19
  23:09:32 PT → EXIT rc=0 23:25:28 PT, CROSSTAB_OK`)
- **Models:** the converged K=4 and K=20 archives at
  `/mnt/data/research/arc/longeval-lda-k{4,20}-converged-archive/`
- **Command (one-liner):** `bash /mnt/data/tmp/k4xk20-crosstab.sh`

## 8. Pointers / open

- The T3-decomposition table belongs in the formal analysis writeup
  ([`20260518-lda-k-selection.md`](20260518-lda-k-selection.md)) §5 as a
  K=20 refinement — it materially upgrades the "what does T3 actually
  contain?" answer. Pending an explicit fold-in.
- Open: date-stratified (pre vs post Dec-2022) crosstab. Requires plumbing
  date through inference; the mean-θ K=20 movers analysis already covers
  the equivalent claim and may be sufficient for the paper.
- Worklog ([`20260519-lda-k4-worklog.md`](20260519-lda-k4-worklog.md))
  updated to flip the K4×K20 crosstab from "open" to "done" and reference
  this document.
