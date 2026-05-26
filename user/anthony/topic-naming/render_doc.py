"""Render raw/{k4,k20}.jsonl into the markdown register-name doc.

Pure formatting; no network. Run after name_topics.py:
    python user/anthony/topic-naming/render_doc.py
"""

import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

HERE = Path(__file__).parent
DOC = HERE.parent / "20260525-lda-topic-register-names.md"

# Document-share inputs. dom% = fraction of the 16.26M training docs whose
# argmax topic is this one (from the K4xK20 crosstab marginals); mean θ% = the
# corpus document-weighted average topic mass (from topicProportions.parquet).
CROSSTAB = "/mnt/data/tmp/k4xk20-crosstab.json"
PROP = {
    "k4": "/mnt/data/research/arc/longeval-lda-k4-converged-archive/k4/topicProportions.parquet",
    "k20": "/mnt/data/research/arc/longeval-lda-k20-converged-archive/k20/topicProportions.parquet",
}


def load(key):
    return [json.loads(l) for l in (HERE / "raw" / f"{key}.jsonl").read_text().splitlines()]


def proportions(key):
    """{topic: (dominant_doc_pct, mean_theta_pct)} for the given model."""
    ct = json.load(open(CROSSTAB))
    N, M = ct["N"], ct["counts"]
    if key == "k4":
        dom = [sum(row) for row in M]
    else:
        dom = [sum(M[i][j] for i in range(4)) for j in range(20)]
    df = pq.read_table(PROP[key]).to_pandas()
    out = {}
    for t in range(len(dom)):
        s = df[df.topic == t]
        theta = (s.mean_theta * s.n_docs).sum() / s.n_docs.sum()
        out[t] = (100 * dom[t] / N, 100 * theta)
    return out


def table(records, props):
    """Descriptive table, sorted by descending document share (headline view)."""
    lines = [
        "| T | label | % docs (dominant) | mean θ | rationale |",
        "|---|---|--:|--:|---|",
    ]
    for d in sorted(records, key=lambda r: -props[r["topic"]][0]):
        dom, theta = props[d["topic"]]
        lines.append(
            f"| {d['topic']} | **{d['label']}** | {dom:.2f}% | {theta:.2f}% "
            f"| {d['rationale']} |"
        )
    return "\n".join(lines)


def terms_table(records):
    """Canonical top-10-terms lookup, in topic-id order."""
    lines = ["| T | top 10 terms |", "|---|---|"]
    for d in sorted(records, key=lambda r: r["topic"]):
        terms = ", ".join(f"`{t}`" for t in d["top_terms"][:10])
        lines.append(f"| {d['topic']} | {terms} |")
    return "\n".join(lines)


def main():
    k4, k20 = load("k4"), load("k20")
    p4, p20 = proportions("k4"), proportions("k20")
    body = f"""# LDA converged-model topic register names (K=4, K=20)

**Date:** {date.today().isoformat()}
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
  `longeval-lda-k{{4,20}}-converged-archive/k{{4,20}}/topicWords_lda.txt`.
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

{table(k4, p4)}

## K=20 (fine companion)

{table(k20, p20)}

## Top-10 terms per topic (canonical identity)

The labels above are a lossy human gloss; these top terms are the actual model
output. Topic-id order, for lookup.

### K=4

{terms_table(k4)}

### K=20

{terms_table(k20)}

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

Raw per-topic JSON (audit trail): `user/anthony/topic-naming/raw/{{k4,k20}}.jsonl`.
"""
    DOC.write_text(body)
    print(f"wrote {DOC} ({len(k4)} K=4 + {len(k20)} K=20 topics)")


if __name__ == "__main__":
    main()
