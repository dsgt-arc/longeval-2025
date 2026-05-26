# LDA converged-model topic register names (K=4, K=20)

**Date:** 2026-05-25
**Author:** anthony (generated via `user/anthony/topic-naming/name_topics.py`)
**Companion:** [`../acmiyaguchi/20260519-lda-k4xk20-attribution.md`](../acmiyaguchi/20260519-lda-k4xk20-attribution.md)

## Why this exists

The topic labels used across the LDA worklogs ("FR editorial / GDPR",
"EN film/web", ...) were written ad hoc and drift between documents. This doc
replaces them with **reproducible** names generated from each topic's top terms
by a pinned model at temperature 0.

The naming is deliberately **free-form** — the model describes each topic in its
own words with no fixed taxonomy. An earlier version constrained the label to a
hand-authored register enum, but that enum was derived from the very topics being
named (circular) and forced a wrong label on the month-chain topic by making the
model pick an adjacent bucket. The top-word list is the canonical topic identity;
the name is just a lossy gloss, so we let the model write the most accurate gloss
it can rather than snap it to a prior.

## Method (all pinned for reproducibility)

- **Model:** `anthropic/claude-sonnet-4.6` via OpenRouter (the repo's
  established LLM path — same as `user/acmiyaguchi/query-expansion/main.py`).
- **temperature = 0** — this, not any taxonomy, is what makes reruns identical;
  one isolated call per topic (no cross-topic context, so naming one topic cannot
  bias the next).
- **Evidence:** top **100** terms per topic — the full list saved in
  `topicWords_lda.txt`, identical count for K=4 and K=20 (symmetric evidence).
- **Inputs:** the archived converged models
  `longeval-lda-k{4,20}-converged-archive/k{4,20}/topicWords_lda.txt`.
- **Output fields:** `label` and `rationale`, both free text. No controlled
  vocabulary, no imposed categories.
- **Reproduce:** `python user/anthony/topic-naming/name_topics.py && python
  user/anthony/topic-naming/render_doc.py`

**Document shares.** `% docs (dominant)` = fraction of the 16,263,471 training
documents whose argmax topic is this one (from the K4×K20 crosstab marginals;
columns sum to ~100%). `mean θ` = corpus document-weighted average topic mass
(from `topicProportions.parquet`). The two track closely because topics are
concentrated (mass ≈ argmax), as noted in the attribution doc.

## K=4 (the CLEF-paper model)

| T | label | % docs (dominant) | mean θ | rationale |
|---|---|--:|--:|---|
| 3 | **French Web Boilerplate & General Content** | 62.95% | 60.23% | Terms like `site_web`, `mot_pase`, `done_person`, `polit_confidentialit`, `cok`, `hor_lign`, `ajout_pani`, and `adres_mail` point to generic website interface and privacy/cookie boilerplate, while a broad mix of everyday French words (`travail`, `enfant`, `chang_climat`, `reseau_social`, `produit`) suggests this is a low-coherence catch-all topic absorbing miscellaneous French web text rather than any single coherent theme. |
| 0 | **English Web Forum & Entertainment Mix** | 24.36% | 25.24% | Terms like `originaly_posted`, `join_date`, `day_ago`, `email_adres`, and `disponibl_disponibl` point to web forum/comment metadata, while `academy_award`, `film_wining`, `eurovision_song`, `johny_depp`, and `song_contest` indicate entertainment content — together forming a low-coherence grab-bag of English-language forum posts and entertainment discussion scraped from the web. |
| 1 | **French Hotel & Real Estate Listings** | 8.80% | 10.38% | Terms like `hotel`, `chambr`, `petit_dejeun`, `sale_bain`, `nuit`, `reserv`, `sejou`, `taxe_sejou`, `location_voitur`, and `apart` point strongly to accommodation/hotel booking content, while `diagnostic_imobili`, `taxe_foncier`, `taxe_habit`, `acquereu_potentiel`, `comisair_priseu`, and `rez_chaus` indicate real estate listings — both domains sharing French property and lodging vocabulary. |
| 2 | **Online Pharmacy Spam (Multilingual)** | 3.89% | 4.12% | Terms like `onlin_pharmacy`, `viagra`, `ciali`, `levitra`, `without_prescription`, `buy_cheap`, `lowest_pric`, and `canadian_pharmacy` are classic markers of pharmaceutical spam content, while multilingual variants (`farmac_onlin`, `pharmac_lign`, `onlin_apothek`, `farmacia_línea`) and date-pair sequences (`juin_mai`, `janvi_decembr`) suggest scraped forum or comment-spam posts across French, English, Spanish, and German web pages. |

## K=20 (fine companion)

| T | label | % docs (dominant) | mean θ | rationale |
|---|---|--:|--:|---|
| 16 | **French Narrative & Everyday Life Writing** | 24.15% | 21.07% | Terms like 'vie', 'histoir', 'feme', 'enfant', 'famil', 'premier_foi', 'guer_mondial', 'auteu', and 'livr' point to personal storytelling and literary/historical narrative, while conversational markers like 'peut_être', 'quelqu_chos', 'alor', 'donc', and 'comentair' suggest informal French web writing (blogs, forums, reviews). |
| 8 | **General English Web Corpus Noise** | 19.02% | 17.84% | The terms are a broad, incoherent mix of high-frequency English function-like words ('one', 'time', 'year', 'like', 'get'), generic web/commerce vocabulary ('email_adres', 'privacy_policy', 'product', 'pric', 'websit', 'right_reserved'), and scattered proper nouns ('new_york', 'united_stat', 'duel_deck'), suggesting this is a low-coherence catch-all topic absorbing residual common English web text rather than any specific subject. |
| 14 | **French E-commerce & Home Products Shopping** | 14.10% | 13.17% | Terms like `livraison`, `delai_livraison`, `comand`, `achat`, `cart_bancair`, `paie_securis`, `code_promo`, `boutique`, and `cond_general` point squarely to online retail transactions, while `sale_bain`, `cuisin`, `pomp_chaleu`, `jardin`, `chambr`, and `maison` indicate the product domain is home goods and household equipment. |
| 3 | **French Climate & Economic Policy** | 10.54% | 10.17% | Terms like `chang_climat`, `develop_durabl`, `energ_renouvelabl`, `trans_ecolog`, `efet_sere`, and `gaz_efet` anchor a strong climate/sustainability thread, while `milion_euro`, `miliard_euro`, `emploi`, `entrepris`, `gouvern`, `premi_ministr`, and `union_europen` situate it firmly in French-language political and economic policy discourse. |
| 13 | **French Municipal & Tax Administration** | 7.64% | 7.97% | Terms like `conseil_municipal`, `taxe_foncier`, `taxe_habit`, `impot_revenu`, `credit_impot`, `mair`, `proc_verbal`, `colectivit_teritorial`, and `pôle_emploi` point strongly to French local government administration combined with personal taxation, employment, and social insurance topics. |
| 10 | **French Auto Forum Community** | 3.78% | 4.07% | Terms like `forum`, `date_inscription`, `nombr_mesag`, `sujet`, `post`, `crez_compt`, and `parcourant_forum` clearly indicate a forum/community platform, while `voitur`, `vehicul`, `moteu`, `moto`, `cart_gris`, `boit_vites`, `frein`, `piec_detach`, `disqu_frein`, `huil`, and `esenc` point specifically to a French-language automotive discussion and parts/repair forum. |
| 0 | **Film & Celebrity Names (Mixed FR/EN)** | 2.73% | 4.22% | Terms like `film`, `johny_depp`, `amb_heard`, `star_war`, `blu_ray`, `box_ofic`, `scienc_fiction`, and `cinema` anchor this in movie/entertainment discussion, while the dense mix of French first names (`jean_pier`, `jean_claud`, `jean_francoi`, `alain`, `bernard`) and English celebrity/place names (`new_york`, `los_angel`, `las_vega`, `gety_imag`) alongside forum artifacts (`join_date`, `originaly_posted`, `quot_originaly`, `item_item`) suggest a bilingual film/celebrity forum or comment thread corpus. |
| 18 | **Online Auction Forum & Eurovision Mixed Content** | 2.13% | 2.58% | Terms like `ordr_achat`, `encher`, `adjudic_prononc`, `vasari_auction`, `acquereu_potentiel`, and `coup_marteau` point to an online auction/bidding platform, while `eurovision_song`, `song_contest`, `album`, `rock`, `hip_hop` indicate music discussion; the many month-pair bigrams (`may_april`, `june_may`, etc.) and forum navigation terms (`post`, `originaly_posted`, `click_expand`, `post_repl`) suggest this is a low-coherence grab-bag mixing a French auction site, a music/Eurovision forum, and generic timestamped web forum content. |
| 1 | **Hotel & Tourism Booking (French)** | 2.12% | 2.44% | Terms like `nuit_nuit`, `petit_dejeun`, `chambr_hôte`, `taxe_sejou`, `location_voitur`, `reserv`, `heberg`, `demi_pension`, and `ofic_tourism` clearly dominate, pointing to French-language hotel/accommodation booking and tourism; a long tail of country-name compounds (`togo_tokelau`, `vanuatu_vatican`, etc.) likely reflects a country-selector dropdown in a booking form. |
| 19 | **French GDPR / Privacy Policy Boilerplate** | 2.05% | 2.51% | Terms like `done_person`, `polit_confidentialit`, `protection_done`, `loi_informat`, `informat_libert`, `exerc_droit`, `accè_rectific`, `mention_legal`, and `cok` (cookies) are the canonical vocabulary of French legal privacy notices and GDPR compliance pages, covering personal data rights, cookie policies, and legal mentions. |
| 4 | **French E-commerce Cookie Consent & Products** | 2.03% | 2.77% | Terms like `gdpr_cok`, `user_consent`, `ajout_pani`, `livraison_gratuit`, `poursuivant_navig`, and `utilison_cok` point strongly to French e-commerce cookie consent banners and shopping cart interactions, while scattered product terms (`complement_alimentair`, `huil_esentiel`, `vin`, `piec_detach`) suggest a heterogeneous mix of product pages from various French online shops. |
| 17 | **Online Pharmacy Spam (Multilingual)** | 1.57% | 1.73% | Terms like `onlin_pharmacy`, `viagra`, `ciali`, `without_prescription`, `cheap_viagra`, `canadian_pharmacy`, `free_shiping`, `lowest_pric`, and multilingual variants (`farmacia_línea`, `onlin_apothek`, `pharmac_lign`) overwhelmingly point to multilingual spam content advertising online pharmacies selling erectile dysfunction drugs and prescription medications without a prescription. |
| 9 | **Hotel Booking & Currency Options (French)** | 1.55% | 1.51% | Terms like 'reserv', 'hotel', 'anul_gratuit', 'option_anul', 'tarif_flexibl', 'nuit', and 'centr_vile' point to a hotel/accommodation booking interface, while the extensive list of currencies ('sheqel_israelien', 'dola_americain', 'yuan_chinoi', 'yen_japonai', 'couron_danois', etc.) and language selectors ('english_español', 'deutsch_english', 'italiano') indicate a multilingual booking platform's UI, with 'coronaviru_covid', 'period_incertitud', and 'mesur_gouvernemental' reflecting COVID-era cancellation policy messaging. |
| 7 | **French Departments and Regions Geography** | 1.32% | 1.94% | The topic is overwhelmingly dominated by French administrative divisions — departments (bas_rhin, haut_rhin, seine_maritime, val_de_marne, etc.) and regions (rhône_alpes, île_de_france, provence_alpes_côte_azur, nouvelle_aquitaine, etc.) — along with related geographic terms like code_postal, depart, and region, indicating a geographic/administrative reference topic focused on French territorial organization. |
| 11 | **World Countries and Territories List** | 1.23% | 1.26% | The terms are dominated by country names, territories, and island nations (e.g., 'virgin_island', 'costa_rica', 'czech_republic', 'papua_new', 'falkland_island') often appearing as alphabetically-adjacent pairs or sequences (e.g., 'bahama_bahrain', 'finland_franc', 'chil_china'), strongly suggesting this topic captures text from enumerated lists or tables of world countries and territories. |
| 12 | **Boko Price Comparison & Mailing List (Mixed Noise)** | 1.08% | 1.22% | The topic is dominated by terms from what appears to be a book/product price-comparison platform called 'Boko' (boko_newslet, boko_stor, pric_comparison, incorect_pric, loading_graph, mailing_list, subscrib_send), mixed with noisy grab-bag terms (eurovision_song, huil_oliv, hary_pot, magic_singl) suggesting low coherence beyond the Boko site's UI and newsletter subscription context. |
| 2 | **Country/Territory Name List (French)** | 0.95% | 1.11% | The terms are overwhelmingly pairs or sequences of country and territory names in French (e.g., 'royaum_uni', 'afriqu_sud', 'emirat_arab_uni', 'nouvel_zeland', 'côte_ivoir'), suggesting this topic captures documents containing exhaustive alphabetical or enumerated lists of world countries, likely from reference pages, banking/financial institution country selectors (supported by 'credit_agricol', 'cais_epargn', 'banqu_populair'), or geographic directories. |
| 15 | **French Administrative & Geographic Records** | 0.84% | 0.83% | The top terms are paired month names (janvi_decembr, juin_mai, etc.) alongside French département names (haut_rhin, bouch_rhon, val_oise, alpe_maritim) and common French given names (jean, pierre, bernard, michel), pointing to administrative or legal registry documents—reinforced by terms like mandatair_judiciair, asoc_selarl, actif_incorporel, and circonscription, suggesting business/legal filings or electoral records organized by date and region. |
| 5 | **Incoherent Mixed-Content Grab-Bag** | 0.75% | 0.99% | The terms span wildly unrelated domains: French personal names (e.g., 'churlaud_raphael', 'vianey_despr'), real-estate diagnostics ('diagnostic_imobili'), Ivory Coast ('côte_ivoir'), Hollywood/Los Angeles tourism ('walk_fame', 'holywod', 'los_angel'), German e-commerce ('den_warenkorb'), and Coca-Cola/novelty items ('bobl_head', 'snow_glob'), indicating a very low-coherence topic with no dominant theme. |
| 6 | **Film Awards & Festival Categories** | 0.41% | 0.57% | Terms like `academy_award`, `bafta_award`, `wining_academy`, `nominated_bafta`, `cesa_award`, `anie_award`, `sxsw_festival`, `cane_camera`, and genre labels (`comedy_drama`, `animated_featur`, `best_documentary`, `spy_film`, `scienc_fiction`) dominate, pointing clearly to a topic about cinema awards, film festival nominations, and movie genre classifications. |

## Top-10 terms per topic (canonical identity)

The labels above are a lossy human gloss; these top terms are the actual model
output. Topic-id order, for lookup.

### K=4

| T | top 10 terms |
|---|---|
| 0 | `one`, `film`, `award_best`, `time`, `year`, `may`, `united_stat`, `like`, `post`, `day` |
| 1 | `nuit_nuit`, `lire_suit`, `hotel`, `centr_vile`, `sale_bain`, `conseil_municipal`, `resultat`, `anul_gratuit`, `nuit`, `invit_invit` |
| 2 | `onlin_pharmacy`, `onlin`, `onlin_apothek`, `treatment_erectil`, `indicated_treatment`, `farmac_onlin`, `pharmac_lign`, `pharmacy`, `viagra`, `date_inscription` |
| 3 | `peut_être`, `site_web`, `savoi_plu`, `produit`, `doit_être`, `utilis`, `mot_pase`, `done_person`, `travail`, `mesag` |

### K=20

| T | top 10 terms |
|---|---|
| 0 | `film`, `jean`, `join_date`, `new_york`, `jean_pier`, `johny_depp`, `pier`, `mar`, `david`, `john` |
| 1 | `nuit_nuit`, `nuit`, `location_voitur`, `petit_dejeun`, `hotel`, `chambr`, `credit_agricol`, `sale_bain`, `taxe_sejou`, `rue` |
| 2 | `île`, `credit_agricol`, `état_uni`, `pay_bas`, `afriqu_sud`, `royaum_uni`, `republ_democrat`, `republ`, `nouvel_zeland`, `arab_saoudit` |
| 3 | `chang_climat`, `projet`, `entrepris`, `econom`, `develop`, `lute_contr`, `social`, `climat`, `energ`, `état_uni` |
| 4 | `savoi_plu`, `ajout_pani`, `cok`, `ajout`, `livraison_gratuit`, `pani`, `savoi`, `poursuivant_navig`, `gogl_analytic`, `aceptez_util` |
| 5 | `lire_suit`, `côte_ivoir`, `lire`, `diagnostic_imobili`, `suit`, `recomanderai_location`, `ami_propret`, `der`, `die`, `cap_emploi` |
| 6 | `award_best`, `hor_lign`, `academy_award`, `film`, `film_wining`, `wining_academy`, `award`, `nominated_academy`, `film_nominated`, `best` |
| 7 | `saint`, `état_uni`, `saint_deni`, `rhon_alpe`, `haut`, `loir`, `franch_comt`, `bouch_rhon`, `côte_azur`, `haut_savo` |
| 8 | `one`, `time`, `year`, `also`, `like`, `may`, `united_stat`, `day`, `use`, `get` |
| 9 | `anul_gratuit`, `centr_vile`, `moyen_nuit`, `âge_enfant`, `hotel`, `invit_invit`, `option_anul`, `recomandon_reserv`, `reserv_option`, `franchis` |
| 10 | `date_inscription`, `mesag`, `aimej_aime`, `nombr_mesag`, `age_local`, `mesag_age`, `inscription_sujet`, `local`, `sujet`, `forum` |
| 11 | `island`, `united_stat`, `virgin_island`, `republic`, `united_kingdom`, `new_zealand`, `costa_rica`, `czech_republic`, `south_africa`, `saint` |
| 12 | `parent_id_name`, `pome_tere`, `mailing_list`, `email_adres`, `amp_amp`, `would_like`, `heartstop_volum`, `path_catalog`, `subscrib_send`, `sent_several` |
| 13 | `peut_être`, `conseil_municipal`, `doit_être`, `conseil`, `travail`, `cas`, `peuvent_être`, `1er_janvi`, `doivent_être`, `loi` |
| 14 | `produit`, `peut_être`, `sale_bain`, `client`, `livraison`, `numero_telephon`, `maison`, `ofre`, `comand`, `gratuit` |
| 15 | `janvi_decembr`, `juin_mai`, `novembr_octobr`, `avril_mar`, `octobr_septembr`, `mar_fevri`, `decembr_novembr`, `mai_avril`, `fevri_janvi`, `juilet_juin` |
| 16 | `peut_être`, `mond`, `peu`, `vie`, `quelqu`, `alor`, `quelqu_chos`, `plu_tard`, `home`, `foi` |
| 17 | `onlin_pharmacy`, `onlin`, `onlin_apothek`, `treatment_erectil`, `indicated_treatment`, `farmac_onlin`, `pharmac_lign`, `pharmacy`, `viagra`, `canadian_pharmacy` |
| 18 | `day_ago`, `year_ago`, `post`, `originaly_posted`, `ago`, `ordr_achat`, `may_april`, `april_march`, `june_may`, `septemb_august` |
| 19 | `site_web`, `mot_pase`, `done_person`, `polit_confidentialit`, `protection_done`, `adres_mail`, `site_internet`, `done_caract`, `caract_personel`, `utilis` |

## Important caveat — top-word register ≠ document-population composition

These names describe each topic's **top-term signature**, which is *not* the
same as what the documents it dominates actually contain. The clearest case is
**K=4 T3**: named here a low-coherence boilerplate/catch-all because its
highest-probability terms are privacy/cart/login chrome (`site_web`, `mot_pase`,
`done_person`, `ajout_pani`). But the document-level crosstab in the companion
attribution doc shows T3 is empirically **~70 % substantive French content**
(editorial prose + politico-economic + admin/legal) and only **~6 %
boilerplate**. The boilerplate terms rank high in the topic-word distribution
without dominating the document population. Use these names for *labelling*; use
the attribution crosstab for claims about *what the corpus is made of*.

Raw per-topic JSON (audit trail): `user/anthony/topic-naming/raw/{k4,k20}.jsonl`.
