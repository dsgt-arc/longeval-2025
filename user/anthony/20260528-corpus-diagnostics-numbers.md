# Numbers reference — K=200 rank stability + seasonality + Dec-2022 corpus diagnostics

Date: 2026-05-28
Companion to [`20260528-lda-k200-rank-stability-seasonality-shift.md`](20260528-lda-k200-rank-stability-seasonality-shift.md) (narrative)
and [`20260527-lda-k200-worklog.md`](20260527-lda-k200-worklog.md) (K=200 training).

Single-file dump of every per-slice number mined this session: K=200 rank/θ
matrices, seasonality probe θ traces, source-data B1/B2/B3 per slice, and
top-3 boilerplate-hash counts per slice.

---

## 1. K=200 rank matrix — 21 topics ever in Top-20

Source: `/mnt/data/tmp/lda-k200-converged/heldout/topic_proportions_15slice.parquet`,
ranked by `mean_theta` per slice. Rows ordered by mean rank across the window.
**Bold** = Dec-2022 transition value for topics whose rank shifted ≥3 positions
across the step (T139 also bolds the pre-step rank=21 marking out-of-Top-20).

| topic | top words (8) | 22-06 | 22-07 | 22-08 | 22-09 | 22-10 | 22-11 | 22-12 | 23-01 | 23-02 | 23-03 | 23-04 | 23-05 | 23-06 | 23-07 | 23-08 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T47  | peut_être, quelqu_chos, peu, quand, bien_sûr, donc, quelqu, faut | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 |
| T198 | like, one, get, time, would, post, make, know | 3 | 3 | 3 | 3 | 3 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 |
| T48  | year_old, one, year, time, first, world, peopl, day | 4 | 4 | 4 | 5 | 4 | 4 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 |
| T16  | histoir, livr, auteu, vie, home, mond, siecl, plu_tard | 2 | 2 | 2 | 2 | 2 | 2 | **5** | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 |
| T89  | stat, year, said, president, vice_president, government, work, ofic | 6 | 6 | 6 | 6 | 5 | 5 | 4 | 4 | 4 | 4 | 4 | 4 | 4 | 4 | 4 |
| T111 | doit_être, peut_être, peuvent_être, doivent_être, consider_come, doit, cas, peuvent | 12 | 12 | 12 | 8 | 12 | 12 | 7 | 7 | 8 | 8 | 7 | 8 | 6 | 7 | 6 |
| T59  | polit, contr, premi_ministr, gouvern, publ_champ, obligatoir_indiqu, president, parti | 7 | 7 | 7 | 7 | 8 | 8 | 12 | 12 | 11 | 10 | 11 | 11 | 10 | 11 | 11 |
| T122 | chang_climat, lute_contr, develop_durabl, climat, develop, projet, mise_œuvr, econom | 5 | 5 | 5 | 4 | 6 | 6 | **13** | 13 | 13 | 13 | 14 | 14 | 12 | 13 | 12 |
| T110 | travail, pris_charg, entrepris, profesion, social, mise_plac, sala, travaileu | 11 | 11 | 11 | 12 | 11 | 11 | 10 | 9 | 9 | 9 | 9 | 9 | 9 | 9 | 10 |
| T40  | may, side_efect, use, patient, cell, treatment, diseas, pric | 20 | 20 | 20 | 18 | 16 | 16 | **6** | 6 | 6 | 6 | 6 | 6 | 7 | 6 | 7 |
| T175 | livraison_gratuit, couleu, produit, aci_inoxydabl, haut, tail, noir, acesoir | 13 | 13 | 13 | 13 | 13 | 13 | 9 | 10 | 12 | 12 | 10 | 10 | 11 | 10 | 9 |
| T72  | jeu, premier_foi, plu_tard, joueu, equip, coup, mesag, saison | 10 | 10 | 10 | 11 | 10 | 10 | 14 | 14 | 14 | 14 | 13 | 13 | 13 | 14 | 13 |
| T139 | pric, home, room, sale, city, area, park, view | **21** | 21 | 21 | 21 | 20 | 20 | **8** | 8 | 7 | 7 | 8 | 7 | 8 | 8 | 8 |
| T121 | week_end, musiqu, rue, even, artist, spectacl, ateli, anim | 8 | 8 | 8 | 10 | 7 | 7 | **18** | 18 | 18 | 18 | 18 | 18 | 18 | 18 | 18 |
| T44  | projet, entrepris, trans_ecolog, elisabeth_born, trans_energet, transport, develop, econom | 9 | 9 | 9 | 9 | 9 | 9 | **19** | 19 | 19 | 19 | 19 | 19 | 19 | 19 | 19 |
| T185 | email_adres, email, privacy_policy, websit, pleas, use, right_reserved, pleas_ent | 19 | 19 | 19 | 20 | 21 | 21 | 11 | 11 | 10 | 11 | 12 | 12 | 14 | 12 | 14 |
| T94  | mot_pase, adres_mail, adres_email, mail, mot, pase_oubl, pase, email | 15 | 15 | 15 | 15 | 17 | 17 | 15 | 15 | 15 | 15 | 15 | 15 | 15 | 15 | 15 |
| T14  | produit, comand, livraison, client, delai_livraison, cadeau, achat, paie_securis | 17 | 17 | 17 | 14 | 14 | 14 | 16 | 16 | 16 | 17 | 16 | 16 | 16 | 16 | 16 |
| T98  | 1er_janvi, revenu, impot, impot_revenu, financi, tau, fiscal, entrepris | 16 | 16 | 16 | 17 | 15 | 15 | 17 | 17 | 17 | 16 | 17 | 17 | 17 | 17 | 17 |
| T90  | site_web, cok, utilis, reseau_social, web, peuvent_être, utilison_cok, navig | 18 | 18 | 18 | 19 | 19 | 19 | 20 | 20 | 20 | 20 | 20 | 20 | 20 | 20 | 20 |
| T42  | jean, jean_pier, pier, mar, jean_claud, michel, jean_francoi, jacqu | 14 | 14 | 14 | 16 | 18 | 18 | **28** | 28 | 28 | 28 | 30 | 28 | 29 | 28 | 29 |

Top-20 *core* (Top-20 at every slice) = 18 / 20. Top-20 *union* = 21 / 200.
Only swap: **T42 out → T139 in** at Dec-2022.

## 2. K=200 θ matrix (×10⁻⁴) — same 21 topics

| topic | 22-06 | 22-07 | 22-08 | 22-09 | 22-10 | 22-11 | 22-12 | 23-01 | 23-02 | 23-03 | 23-04 | 23-05 | 23-06 | 23-07 | 23-08 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T47  | 543.5 | 543.3 | 543.9 | 564.3 | 503.1 | 502.5 | 404.6 | 405.2 | 403.2 | 408.1 | 408.5 | 408.5 | 416.0 | 409.3 | 415.9 |
| T198 | 329.7 | 329.5 | 328.7 | 290.3 | 307.6 | 305.3 | 382.0 | 381.7 | 386.1 | 382.9 | 375.5 | 378.2 | 361.3 | 377.0 | 360.7 |
| T48  | 310.4 | 310.4 | 309.7 | 267.4 | 301.9 | 299.8 | 346.2 | 346.0 | 349.9 | 352.3 | 345.9 | 348.6 | 334.8 | 347.4 | 334.9 |
| T16  | 346.1 | 344.7 | 345.6 | 342.1 | 324.0 | 324.2 | 244.3 | 245.3 | 243.3 | 248.6 | 250.9 | 250.3 | 253.9 | 250.0 | 253.6 |
| T89  | 274.4 | 274.1 | 273.3 | 234.5 | 276.5 | 274.5 | 322.3 | 322.0 | 326.1 | 329.6 | 323.7 | 325.6 | 313.6 | 324.7 | 313.1 |
| T111 | 167.1 | 166.9 | 166.8 | 186.8 | 175.0 | 174.9 | 187.0 | 187.3 | 186.3 | 183.4 | 184.9 | 184.5 | 190.3 | 185.0 | 189.5 |
| T59  | 218.6 | 218.4 | 217.6 | 195.4 | 191.2 | 191.8 | 166.3 | 166.9 | 168.8 | 174.2 | 172.6 | 172.6 | 176.1 | 173.3 | 175.2 |
| T122 | 280.7 | 279.5 | 281.2 | 273.7 | 270.2 | 269.9 | 162.8 | 163.3 | 162.8 | 165.1 | 164.8 | 164.4 | 170.0 | 165.4 | 169.0 |
| T110 | 171.3 | 170.4 | 170.2 | 173.6 | 178.3 | 178.7 | 173.1 | 174.7 | 174.4 | 176.0 | 175.9 | 175.6 | 176.2 | 175.6 | 175.6 |
| T40  | 127.1 | 127.3 | 126.9 | 135.1 | 142.1 | 140.9 | 188.8 | 188.7 | 191.1 | 192.0 | 188.6 | 190.1 | 181.5 | 189.7 | 182.8 |
| T175 | 165.4 | 165.4 | 166.1 | 170.6 | 170.0 | 171.3 | 173.8 | 173.8 | 168.0 | 170.0 | 175.9 | 173.9 | 174.6 | 174.1 | 177.4 |
| T72  | 177.3 | 176.7 | 175.3 | 181.6 | 179.5 | 179.8 | 162.1 | 161.6 | 162.6 | 165.1 | 164.9 | 165.2 | 164.8 | 165.1 | 165.0 |
| T139 | 121.9 | 121.8 | 121.5 | 113.7 | 118.7 | 117.9 | 185.3 | 185.2 | 187.7 | 186.8 | 183.7 | 184.7 | 176.7 | 184.0 | 177.6 |
| T121 | 198.1 | 197.5 | 198.2 | 182.7 | 192.1 | 193.1 | 150.8 | 150.7 | 151.2 | 153.6 | 155.1 | 154.4 | 155.0 | 154.6 | 155.5 |
| T44  | 192.5 | 193.4 | 194.2 | 183.4 | 190.5 | 190.4 | 144.8 | 145.8 | 146.2 | 148.8 | 149.2 | 148.3 | 150.4 | 148.5 | 150.0 |
| T185 | 131.6 | 131.5 | 131.3 | 115.6 | 118.5 | 117.9 | 169.2 | 169.0 | 171.1 | 171.4 | 168.9 | 169.8 | 164.3 | 169.2 | 164.1 |
| T94  | 151.7 | 151.7 | 150.7 | 151.5 | 139.1 | 139.5 | 161.3 | 161.4 | 160.5 | 159.4 | 161.0 | 160.7 | 163.4 | 160.8 | 163.1 |
| T14  | 140.6 | 140.5 | 140.6 | 151.6 | 162.8 | 163.8 | 159.7 | 157.9 | 154.1 | 155.2 | 160.0 | 158.4 | 158.1 | 158.5 | 160.0 |
| T98  | 141.3 | 140.8 | 141.2 | 141.2 | 150.0 | 150.2 | 151.1 | 152.5 | 152.5 | 156.9 | 158.0 | 157.5 | 157.2 | 156.5 | 158.1 |
| T90  | 134.3 | 134.2 | 134.0 | 119.8 | 123.0 | 123.1 | 139.7 | 140.4 | 139.5 | 141.9 | 143.8 | 143.0 | 143.0 | 142.8 | 143.1 |
| T42  | 154.2 | 154.0 | 153.3 | 147.3 | 127.0 | 127.3 |  96.6 |  96.7 |  97.4 |  97.1 |  96.7 |  96.7 |  97.0 |  96.8 |  96.6 |

## 3. Seasonality probe θ traces (×10⁻⁴)

Topics chosen for max-seasonal vocabulary. See narrative note for the YoY co-spike
algorithm; here just the raw monthly θ.

| topic | top words | 22-06 | 22-07 | 22-08 | 22-09 | 22-10 | 22-11 | 22-12 | 23-01 | 23-02 | 23-03 | 23-04 | 23-05 | 23-06 | 23-07 | 23-08 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T68  | fête_mère, maman                       | 25.73 | 25.77 | 25.66 | 25.54 | 20.73 | 20.69 |  5.34 |  5.34 |  5.29 |  5.20 |  5.24 |  5.34 |  5.44 |  5.25 |  5.29 |
| T78  | vote, election_legislatif              | 56.43 | 56.34 | 56.13 | 17.66 | 13.88 | 13.81 | 13.45 | 13.59 | 13.71 | 13.42 | 13.40 | 13.52 | 13.15 | 13.42 | 13.30 |
| T82  | père_noël, xvie_siecl                  | 27.74 | 27.71 | 27.71 | 33.34 | 25.72 | 25.64 | 13.99 | 13.99 | 13.99 | 13.31 | 13.30 | 13.38 | 13.98 | 13.40 | 13.66 |
| T101 | fleu, bouquet_fleu, saint_valentin     | 42.75 | 42.73 | 42.56 | 42.81 | 56.72 | 57.16 | 18.95 | 19.01 | 18.95 | 18.97 | 19.40 | 19.38 | 19.38 | 19.25 | 19.53 |
| T155 | academy_award, film_wining             |  9.01 |  9.01 |  8.97 | 11.61 | 27.25 | 27.07 | 27.76 | 27.90 | 28.31 | 28.54 | 28.09 | 27.98 | 28.04 | 27.90 | 28.12 |
| T190 | election_legislatif (variant)          | 86.08 | 85.87 | 85.41 | 28.67 | 30.59 | 30.52 | 33.39 | 33.76 | 34.35 | 35.54 | 35.18 | 35.30 | 34.81 | 35.41 | 34.90 |

Funnel: naive pre/post detrend → 70 candidates (35 peaking Sep-2022); rolling-median
detrend (window=3) → 18 candidates (still Sep-2022-clustered); YoY co-spike on
Jun/Jul/Aug → **0 / 3 confirmed**.

## 4. B1 — document length distribution per slice

Source: `/mnt/data/tmp/corpus-diagnostics/doc-length/summary_by_date.csv`.
`n_docs` is exact count (post-dedup). `*_pNN` are `percentile_approx`, accuracy=10000.
**Bold** = Oct/Nov 2022 highs and Dec-2022 step values; ↓ marks the Dec-2022
step direction relative to Oct/Nov.

| date | n_docs | char_mean | char_stddev | char_p05 | char_p25 | char_p50 | char_p75 | char_p95 | word_mean | word_p25 | word_p50 | word_p75 | word_p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2022-06 | 1,590,024 | 5229 | 3437 | 545 | 2158 | 4596 | 9037 | 10080 | 827 | 343 | 728 | 1347 | 1692 |
| 2022-07 | 1,591,964 | 5225 | 3436 | 545 | 2155 | 4594 | 9029 | 10080 | 826 | 342 | 728 | 1345 | 1692 |
| 2022-08 | 1,600,735 | 5235 | 3443 | 544 | 2152 | 4606 | 9073 | 10080 | 828 | 342 | 730 | 1352 | 1692 |
| 2022-09 | 1,080,369 | 5300 | 3455 | 560 | 2185 | 4676 | 9269 | 10082 | 843 | 345 | 743 | 1401 | 1705 |
| 2022-10 | 2,137,543 | **5421** | 3411 | 626 | 2380 | 4840 | 9501 | 10081 | 860 | 379 | 760 | 1419 | 1704 |
| 2022-11 | 2,153,685 | **5418** | 3410 | 626 | 2377 | 4837 | 9484 | 10081 | 859 | 379 | 760 | 1417 | 1703 |
| **2022-12** | 2,046,071 | **5054** ↓ | 3413 | **489** | **2046** | **4364** ↓ | **8538** | 10080 | **801** ↓ | **323** | **682** ↓ | 1323 | 1688 |
| 2023-01 | 2,048,378 | 5056 | 3413 | 489 | 2048 | 4372 | 8539 | 10080 | 802 | 323 | 683 | 1323 | 1688 |
| 2023-02 | 2,014,702 | 5058 | 3418 | 479 | 2040 | 4382 | 8554 | 10080 | 802 | 322 | 685 | 1323 | 1688 |
| 2023-03 | 2,008,495 | 5060 | 3418 | 480 | 2042 | 4385 | 8563 | 10080 | 802 | 322 | 685 | 1324 | 1688 |
| 2023-04 | 1,993,335 | 5049 | 3418 | 477 | 2031 | 4365 | 8539 | 10080 | 800 | 321 | 682 | 1321 | 1687 |
| 2023-05 | 2,033,525 | 5050 | 3419 | 476 | 2031 | 4368 | 8544 | 10080 | 800 | 320 | 682 | 1321 | 1688 |
| 2023-06 | 1,444,601 | 5087 | 3420 | 486 | 2060 | 4427 | 8626 | 10080 | 806 | 325 | 692 | 1332 | 1687 |
| 2023-07 | 1,940,277 | 5057 | 3419 | 479 | 2037 | 4379 | 8559 | 10080 | 801 | 321 | 684 | 1323 | 1688 |
| 2023-08 | 1,489,204 | 5080 | 3421 | 486 | 2052 | 4417 | 8615 | 10080 | 805 | 324 | 690 | 1331 | 1687 |

p95 char is essentially flat at ~10080 across the whole window (a likely soft cap at
~10K chars). The Dec-2022 step is in lower percentiles only.

## 5. B2 — content-hash uniqueness per slice

Source: `/mnt/data/tmp/corpus-diagnostics/dedup/summary_by_date.csv`.
`n_unique_contents` from `approx_count_distinct(sha256(contents), rsd=0.01)`.

| date | n_docs | n_unique_contents | unique_frac |
|---|---:|---:|---:|
| 2022-06 | 1,590,024 | 1,421,939 | 0.894 |
| 2022-07 | 1,591,964 | 1,424,084 | 0.895 |
| 2022-08 | 1,600,735 | 1,440,703 | 0.900 |
| 2022-09 | 1,080,369 |   985,906 | 0.913 |
| 2022-10 | 2,137,543 | 1,865,743 | 0.873 |
| 2022-11 | 2,153,685 | 1,879,165 | 0.873 |
| 2022-12 | 2,046,071 | 1,815,365 | 0.887 |
| 2023-01 | 2,048,378 | 1,822,010 | 0.889 |
| 2023-02 | 2,014,702 | 1,778,464 | 0.883 |
| 2023-03 | 2,008,495 | 1,771,891 | 0.882 |
| 2023-04 | 1,993,335 | 1,750,625 | 0.878 |
| 2023-05 | 2,033,525 | 1,804,830 | 0.888 |
| 2023-06 | 1,444,601 | 1,290,387 | 0.893 |
| 2023-07 | 1,940,277 | 1,715,607 | 0.884 |
| 2023-08 | 1,489,204 | 1,315,611 | 0.883 |

unique_frac stays in 0.87–0.91 across the whole window. No discontinuity at
Dec-2022.

## 6. B3 — EN-stopword density per slice

Source: `/mnt/data/tmp/corpus-diagnostics/en-density/summary_by_date.csv` (regex-fixed).
Stopwords: `{the, and, for, with, you, this, that, from, have, not}`, word-bounded,
case-insensitive. **Bold** row = Dec-2022 step values; ↑ marks direction
relative to Oct/Nov 2022.

| date | en_density_mean | en_density_p50 | en_density_p95 | frac_above_05 | frac_above_10 |
|---|---:|---:|---:|---:|---:|
| 2022-06 | 0.0250 | 0.0 | 0.1359 | 0.2017 | 0.1388 |
| 2022-07 | 0.0251 | 0.0 | 0.1360 | 0.2027 | 0.1396 |
| 2022-08 | 0.0250 | 0.0 | 0.1360 | 0.2022 | 0.1393 |
| 2022-09 | 0.0236 | 0.0 | 0.1341 | 0.1920 | 0.1304 |
| 2022-10 | 0.0245 | 0.0 | 0.1354 | 0.1990 | 0.1345 |
| 2022-11 | 0.0244 | 0.0 | 0.1352 | 0.1975 | 0.1335 |
| **2022-12** | **0.0295** ↑ | 0.0 | **0.1400** ↑ | **0.2410** ↑ | **0.1607** ↑ |
| 2023-01 | 0.0294 | 0.0 | 0.1400 | 0.2407 | 0.1604 |
| 2023-02 | 0.0298 | 0.0 | 0.1402 | 0.2436 | 0.1624 |
| 2023-03 | 0.0298 | 0.0 | 0.1403 | 0.2440 | 0.1627 |
| 2023-04 | 0.0293 | 0.0 | 0.1399 | 0.2397 | 0.1598 |
| 2023-05 | 0.0295 | 0.0 | 0.1400 | 0.2413 | 0.1610 |
| 2023-06 | 0.0284 | 0.0 | 0.1392 | 0.2322 | 0.1547 |
| 2023-07 | 0.0294 | 0.0 | 0.1399 | 0.2406 | 0.1604 |
| 2023-08 | 0.0284 | 0.0 | 0.1392 | 0.2322 | 0.1546 |

Median is 0.0 throughout — most FR docs contain no EN stopwords at all. The signal
is entirely in the right tail. `frac_above_05` step: 0.198 → 0.241 (+22 % rel).

## 7. Top-3 boilerplate hashes per slice

Source: `/mnt/data/tmp/corpus-diagnostics/dedup/top_hashes_by_date.parquet`.
`n` is per-slice occurrence count of an exact-duplicate document text. Threshold
filter `n ≥ 50` was applied before ranking.

| date | r1 hash | r1 n | r2 hash | r2 n | r3 hash | r3 n |
|---|---|---:|---|---:|---|---:|
| 2022-06 | b6d12ab… | **13,288** | eccfc75… | 1,508 | d892a81… | 1,058 |
| 2022-07 | b6d12ab… | **13,277** | eccfc75… | 1,507 | d892a81… | 1,058 |
| 2022-08 | b6d12ab… | **13,273** | eccfc75… | 1,498 | d892a81… | 1,053 |
| 2022-09 | eccfc75… | 1,496 | 9518830… | 759 | 0cdc276… | 535 |
| 2022-10 | 78a2652… | 5,032 | 6d9e78b… | 3,116 | 5e21fba… | 2,152 |
| 2022-11 | 78a2652… | 5,030 | 6d9e78b… | 3,115 | dc3e3d6… | 2,504 |
| 2022-12 | 78a2652… | 5,045 | dc3e3d6… | 3,442 | 6d9e78b… | 3,084 |
| 2023-01 | 78a2652… | 5,021 | 6d9e78b… | 3,082 | dc3e3d6… | 2,536 |
| 2023-02 | 78a2652… | 4,937 | 0d5767c… | 3,425 | 6d9e78b… | 3,085 |
| 2023-03 | 78a2652… | 4,909 | 0d5767c… | 3,429 | 6d9e78b… | 3,082 |
| 2023-04 | 78a2652… | 4,764 | 0d5767c… | 3,344 | 6d9e78b… | 3,103 |
| 2023-05 | 78a2652… | 4,888 | 0d5767c… | 3,403 | 6d9e78b… | 3,093 |
| 2023-06 | 78a2652… | 3,393 | 6d9e78b… | 2,688 | 0d5767c… | 2,362 |
| 2023-07 | 78a2652… | 4,637 | 0d5767c… | 3,168 | 6d9e78b… | 2,995 |
| 2023-08 | 78a2652… | 3,427 | 6d9e78b… | 2,668 | 0d5767c… | 2,402 |

**Discovery surfaced by the table** (not yet folded into narrative): the dominant
boilerplate hash *changes identity* at Sep→Oct 2022, not at Dec 2022.
`b6d12ab…` (13K copies/slice in Jun–Aug 2022) vanishes from the top-3 entirely
after Aug, replaced by `78a2652…` (5K copies) from Oct 2022 onward. The
intermediate Sep 2022 slice (smaller N at 1.08M) has neither hash dominant.
B2 unique_frac doesn't change much across this boundary — total boilerplate volume
is similar, but the *fingerprint* shifted. Could be a separate Sep/Oct boundary
that's independent of the Dec-2022 topic step; worth a follow-up.

## 8. Aggregate corpus size

| collection | n_docs | unique_frac | char_mean |
|---|---:|---:|---:|
| train (9 slices 2022-06 → 2023-02) | 16,263,471 | ~0.88 | ~5215 |
| test (6 slices 2023-03 → 2023-08) | 10,909,437 | ~0.88 | ~5063 |
| **total** | **27,172,908** | | |

## 9. Lineage / files

Full file inventory (sources, diagnostic jobs, companion worklogs) in the
narrative note's "Lineage / files used" section. The number tables above were
generated from:

| What | Where |
|---|---|
| K=200 held-out topic proportions | `/mnt/data/tmp/lda-k200-converged/heldout/topic_proportions_15slice.parquet` |
| K=200 topic words | `/mnt/data/tmp/lda-k200-converged/k200/topicWords_lda.txt` |
| B1/B2/B3 CSVs | `/mnt/data/tmp/corpus-diagnostics/{doc-length,dedup,en-density}/summary_by_date.csv` |
| Top-20 boilerplate hashes per slice | `/mnt/data/tmp/corpus-diagnostics/dedup/top_hashes_by_date.parquet` |
