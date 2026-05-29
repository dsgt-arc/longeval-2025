# LDA K-granularity saturation: K=4 → K=20 → K=200

Date: 2026-05-28

## TL;DR

We now have three converged LDA models on the same pooled 9-slice corpus
(16,263,471 docs, identical vocab + phrases via hardlinked preprocess) and
their held-out 15-slice inferences against the 6 test slices: **K=4** (CLEF
paper headline), **K=20** (granular companion), **K=200** (granularity anchor,
trained 2026-05-27 → 28).

The Dec-2022 document regime change has a **fixed structural magnitude that
K=20 already captures, and K=200 does not add to**. Held-out pre→post Dec-2022
L1 mass shift saturates around K=20:

| K  | held-out L1 mass shift (Σ\|Δ\|) | max single-topic Δ | substantive interpretation |
|---:|---:|---:|---|
|  4 | 0.106  | T3 −0.054 | macro register swap (T0 ↑ / T3 ↓) |
| 20 | 0.2011 | T16 −0.048 | fine register swap (T16 substantive FR ↓ / T8 generic EN ↑) |
| **200** | **0.2115** | **T47 −0.013** | **same swap diffused over many fine registers** |

K=20 = the right granularity for the writeup. K=200 = corroboration anchor,
not additive evidence. The K=200 register pattern reads the same as K=20 at
finer resolution: **substitution of substantive French prose with
English-language web content + commerce/booking boilerplate** — a
crawl/extraction regime change, not a language-mix shift (the corpus is
100 % French-labelled at every one of the 15 slices, see `20260519-lda-k4-worklog.md`
Finding #2).

## What's new (today, 2026-05-28)

1. **K=200 converged model trained** (~7 h LDA fit; pipeline end-to-end
   ~10 h; coherence/Eval and held-out drift both validated).
   Worklog: [`20260527-lda-k200-worklog.md`](20260527-lda-k200-worklog.md).
2. **K=200 held-out 15-slice drift ran clean** (2026-05-27 23:29 → 28 01:29,
   2 h wall, no errors). Inference Stage 20 hit a swap-pressure cliff at
   16/40 parts (driver RSS 60 GB, swap 12 → 24 GiB) but cleared on its own.
3. **L1 mass shift saturation between K=20 and K=200 established** (0.201 →
   0.211). The granularity question is now settled.
4. **Figure 2 panel (a) extended to all 15 slices** in the paper repo using
   the held-out K=4 inference values (commit pending). The held-out test
   window holds flat as a plateau through 2023-08 — no decay, no reversion.

## Why this matters for the paper

We had three open questions in the topic-modeling section:

- **Q1.** Does the Dec-2022 step survive into the test window, or does it
  decay back? — **A:** flat plateau through 2023-08, at every K we measured.
- **Q2.** Is the step a coarse artifact of the K=4 binning, or does it
  reproduce at finer granularity? — **A:** reproduces at K=20 (8 sub-registers
  move), reproduces at K=200 (top-15 movers ≈ 45 % of the same total mass
  shift), saturates by K=20. Not a binning artifact.
- **Q3.** Is the step a language-composition shift ("more English documents")
  or a content shift inside French-labelled documents? — **A:**
  language-composition is constant (100 % FR labelled at all 15 slices;
  Finding #2 in `20260519-lda-k4-worklog.md`). It is a content shift:
  substantive French prose registers lose mass; English-language content
  registers + booking/cookie boilerplate gain mass. The crawler/extractor
  started surfacing more EN content and stopped surfacing as much substantive
  FR prose, within the same French-labelled crawl.

The K=200 top-mover register table makes Q3 unambiguous. Down-movers (lose
mass post-Dec-2022) are uniformly substantive French registers; up-movers are
uniformly English-language web content + commerce boilerplate:

| direction | register | top words |
|---|---|---|
| ↓ T47 (Δ −0.013) | FR generic narrative prose / fillers | peut_être, quelqu_chose, faut, alors, dit, vie, dire, beaucoup |
| ↓ T122 (Δ −0.011) | climate / sustainable-development discourse | chang_climat, develop_durabl, nation_unie, environ, polit |
| ↓ T16 (Δ −0.009) | FR history / literature | histoir, livr, auteu, siecl, guer, dieu, roman |
| ↓ T44 (Δ −0.005) | FR politico-economic transitions | projet, trans_ecolog, elisabeth_born, trans_energet, econom |
| ↓ T164 (Δ −0.004) | FR film / audiovisual credits | jean, production, catalogu_audiovisuel, francofol_rochel |
| ↓ T121 (Δ −0.004) | FR cultural events / festivals | musiqu, artist, spectacl, festival, theatr, concert |
| ↓ T42 (Δ −0.004) | FR given-name biographical listings | jean, pier, mar, michel, claud |
| ↑ T139 (Δ +0.006) | EN hotel / travel commerce | pric, home, room, sale, hotel, travel, review |
| ↑ T84 (Δ +0.006) | EN country / island geographic enumeration | island, virgin_island, cayman_island, falkland_island |
| ↑ T198 (Δ +0.006) | EN forum / blog prose | like, one, get, would, post, know, think, want |
| ↑ T89 (Δ +0.005) | EN business / government news | stat, year, president, government, busines, manag |
| ↑ T40 (Δ +0.005) | EN medical / pharma | may, side_efect, patient, treatment, diseas, drug, canc |
| ↑ T48 (Δ +0.005) | EN narrative / biographical | year_old, life, said, man, war, book |
| ↑ T145 (Δ +0.005) | FR hotel-booking boilerplate | centr_vile, anul_gratuit, hotel, recomandon_reserv |
| ↑ T185 (Δ +0.004) | EN web-form / cookie boilerplate | email_adres, privacy_policy, cok, personal_data, pasword |

## Per-K table (training-side, 9-slice pooled corpus)

| metric | K=4 | K=20 | K=200 |
|---|---:|---:|---:|
| mean NPMI | 0.3542 | **0.3723** | 0.2586 |
| topic diversity | 1.0000 | 0.9500 | 0.8690 |
| coherence × diversity | **0.3542** | 0.3536 | 0.2247 |
| topics with NPMI > 0.5 | 1 | 5 | 25 |
| topics with NPMI < 0 (junk band) | 0 | 0 | 17 (8.5 %) |
| max per-topic NPMI (tightest specialist) | 0.629 | 0.949 | **0.986** |
| train-side Σ\|Δ\| first→last slice | 0.113 | 0.209 | 0.251 |
| held-out Σ\|Δ\| pre→post Dec-2022 | 0.106 | 0.201 | 0.212 |

**Reads.**
- K=4 wins diversity, K=20 wins NPMI, the combined coh×div ties (K=4 marginal
  optimum on the K=2..10 sweep, paper headline).
- Tightest single topic gets tighter as K grows — fine granularity finds
  hyper-specific registers (Paris museums at K=200 topic 0, English country
  names at T84, etc.) — but the mean drops because the same vocabulary mass
  has to be spread thinner.
- **Held-out drift saturates between K=20 and K=200** (0.201 → 0.212). Train
  drift keeps growing (0.21 → 0.25) because K=200 spreads the step across
  more topics each with a small fragment; the held-out total mass that moves
  is fixed.

## Lineage / where everything lives

| What | Where |
|---|---|
| K=4 worklog (CLEF paper model) | [`user/acmiyaguchi/20260519-lda-k4-worklog.md`](../acmiyaguchi/20260519-lda-k4-worklog.md) |
| K=4 × K=20 attribution crosstab | [`user/acmiyaguchi/20260519-lda-k4xk20-attribution.md`](../acmiyaguchi/20260519-lda-k4xk20-attribution.md) |
| K-selection sweep (K=2..10) analysis | [`user/acmiyaguchi/20260518-lda-k-selection.md`](../acmiyaguchi/20260518-lda-k-selection.md) |
| K=200 worklog (this work) | [`20260527-lda-k200-worklog.md`](20260527-lda-k200-worklog.md) |
| K=2..20 dense sweep scan | [`20260527-lda-k2-20-sweep-scan.md`](20260527-lda-k2-20-sweep-scan.md) (PR #39, branch `issue-36-lda-k-sweep`) |
| Topic register names (LLM) | [`20260525-lda-topic-register-names.md`](20260525-lda-topic-register-names.md) |
| K=4 converged model + archive | `/mnt/data/tmp/lda-k4-converged/` · `/mnt/data/research/arc/longeval-lda-k4-converged-archive/` |
| K=20 converged model + archive | `/mnt/data/tmp/lda-k20-converged/` · `/mnt/data/research/arc/longeval-lda-k20-converged-archive/` |
| K=200 converged model | `/mnt/data/tmp/lda-k200-converged/` (archive PENDING) |
| Held-out drift parquets (K=4/K=20/K=200) | `/mnt/data/tmp/lda-k{4,20,200}-converged/heldout/topic_{proportions,drift}_15slice.parquet` |

## Operational notes (lessons for K ≥ 100)

- **Hardlink preprocess** (`cp -al /mnt/data/tmp/lda-k4-converged/preprocess
  /mnt/data/tmp/lda-kN-converged/preprocess`) — MinePhrases/BuildFeatures
  short-circuit on inode-identical `_config.json` / `_phrases_config.json`,
  saving the per-K vocab build entirely. Pattern from
  `project_incremental_k_sweep`.
- **Spark driver memory ceiling ≈ 36–40 g** on this host (62 GiB total). K=4
  and K=20 fit comfortably; K=200 *barely* fits at 40 g and spilled into swap
  during held-out Stage 20 (driver RSS 60 GB > Xmx 40 g). Cleared on its own
  this time. For the next K ≥ 100 run, prefer `--driver-memory 32g` or drop
  `local[8] → local[4]` to halve Python-side parallelism. The historic
  kernel-OOM-kill mode at 50 g over-commit is a hard ceiling not to cross.
- **EvaluateLDAModel Python worker** crashed at K=200 attempt 1 (no Python
  traceback because `spark.python.worker.faulthandler.enabled` was off), then
  succeeded on Luigi auto-retry (same task hash, no config change). Marginal
  memory pressure in `coherence.py`'s `_restrict(...).cache()`, not a
  fundamental scaling break. Prophylactic config for next run:
  ```
  --conf spark.python.worker.faulthandler.enabled=true
  --conf spark.executor.pyspark.memory=2g
  --conf spark.master='local[4]'
  --conf spark.sql.shuffle.partitions=400
  ```
- **SPARK_LOCAL_IP=127.0.0.1** still mandatory (wifi-drop kill mode).
- **`/mnt/data/tmp` is durable nvme**, not tmpfs — survives reboot. Archive
  guards against scratch housekeeping, not power loss.

## Open

1. **Archive K=200** to `longeval-lda-k200-converged-archive` matching the
   K=4/K=20 pattern (MANIFEST.md, SHA256SUMS, ~50 MB), then GCS push to
   `gs://dsgt-longeval-2025/` once both K=4 and K=20 archives are also
   pushed (still pending explicit go-ahead).
2. **Commit the paper repo edits** for the held-out 15-slice figure
   extension (Figure 2 panel (a) and §4 text in
   `../longeval-2025-best-of-labs`).
3. **K4×K200 / K20×K200 attribution crosstabs** — *defer*. K=20 already
   saturates the held-out mass shift; only worth doing if a reviewer asks
   for per-document re-routing across the Dec-2022 boundary at K=200
   resolution.
4. **Held-out K=2..20 dense drift** — the issue #36 PR #39 sweep results
   will populate this once it finishes. Will give a smooth granularity
   curve for the saturation claim (rather than the three-point K=4/20/200
   step we have now).
