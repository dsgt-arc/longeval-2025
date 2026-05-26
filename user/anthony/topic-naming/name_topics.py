"""Deterministic register naming for the converged LDA topics (K=4, K=20).

Motivation: the topic labels scattered through the worklogs (e.g. "FR editorial /
GDPR", "EN film/web") were hand-written ad hoc and drift between documents. This
script produces *reproducible, consistent* names by feeding each topic's top
words to a fixed model (Claude Sonnet 4.6) at temperature=0, one topic per
independent call (no cross-topic context, so naming topic 7 cannot bias topic
8), and constraining the output to a controlled taxonomy rather than free text.

Naming is FREE-FORM by design: the model describes each topic in its own
words, unconstrained by any taxonomy. An earlier version forced the label into
a hand-authored register enum, but that enum was derived from the very topics
being named (circular) and actively produced a wrong label on the month-chain
topic by forcing an adjacent-bucket choice. The top-word list is the canonical
topic identity; the name is just a lossy human gloss, so we let the model write
the most accurate gloss it can rather than snapping it to a prior.

Reproducibility levers, all pinned:
  - model = anthropic/claude-sonnet-4.6  (exact, via OpenRouter — the repo's
    established LLM path, mirroring user/acmiyaguchi/query-expansion/main.py)
  - temperature = 0  (this, not any taxonomy, is what makes reruns identical)
  - one isolated call per topic (no cross-topic context)
  - top-N words frozen at 100 (the count saved in topicWords_lda.txt for BOTH
    K=4 and K=20, so the two models are named from symmetric evidence)

Inputs are the archived converged models' topic-word dumps. Outputs:
  - raw/<k>.jsonl  — one model response per line (audit trail)
  - the rendered markdown table is produced by render_doc.py from those.

Run from the repo root (loads .env.local for OPENROUTER_API_KEY):
    python user/anthony/topic-naming/name_topics.py
"""

import ast
import csv
import json
import os
import re
from pathlib import Path

import dotenv
import requests

dotenv.load_dotenv(".env.local", override=True)

MODEL = "anthropic/claude-sonnet-4.6"
TOP_N = 100  # words per topic fed to the model; 100 = full saved list for both K

# Archived converged models (the CLEF-paper K=4 and its K=20 companion).
INPUTS = {
    "k4": "/mnt/data/research/arc/longeval-lda-k4-converged-archive/k4/topicWords_lda.txt",
    "k20": "/mnt/data/research/arc/longeval-lda-k20-converged-archive/k20/topicWords_lda.txt",
}

def load_topics(path: str):
    """Yield (topic_id:int, [words]) from a topicWords_lda.txt CSV."""
    with open(path) as fh:
        rows = list(csv.reader(fh))
    for tid, words_literal in rows[1:]:  # skip "Topic,Words" header
        yield int(tid), ast.literal_eval(words_literal)


def prompt(words):
    word_str = ", ".join(words[:TOP_N])
    return f"""You are describing a single topic from an LDA topic model trained on a \
mixed French/English web-document corpus (the LongEval web collection). You are \
given the topic's top {TOP_N} terms by probability, most probable first. Terms \
were lowercased and stemmed/compounded by the preprocessing pipeline (e.g. \
"conseil_municipal", "petit_dejeun", "gdpr_cok"), so they may look truncated.

Describe what this topic is, in your own words. There is NO fixed list of \
categories — do not snap to a predefined taxonomy. Name the topic by what the \
terms actually show, even if it is heterogeneous, low-coherence, or doesn't fit \
a tidy register. If the terms are dominated by a clear theme, say so; if they're \
a grab-bag, say that instead of forcing a single label.

`label` — a concise human-readable name, 2-6 words, as accurate as the terms \
allow (mention the dominant language if it's clearly French or English).

`rationale` — ONE or two sentences naming the specific terms that drove your \
description.

Respond with ONLY a JSON object, no prose, no code fence:
{{"label": "...", "rationale": "..."}}

Top terms:
{word_str}
"""


def call(words):
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
        },
        json=dict(
            model=MODEL,
            temperature=0,  # determinism lever
            messages=[{"role": "user", "content": prompt(words)}],
        ),
        timeout=120,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    # Defensive parse: strip an accidental ```json fence, else grab first {...}.
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", content, re.S)
        if not m:
            raise ValueError(f"no JSON object in response: {content!r}")
        return json.loads(m.group(0))


def main():
    out_dir = Path(__file__).parent / "raw"
    out_dir.mkdir(exist_ok=True)
    for key, path in INPUTS.items():
        out = out_dir / f"{key}.jsonl"
        print(f"== {key} ({path}) -> {out}")
        with out.open("w") as fh:
            for tid, words in load_topics(path):
                rec = call(words)
                rec["model"] = key
                rec["topic"] = tid
                rec["top_terms"] = words[:12]  # for the rendered table
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                print(f"  T{tid:>2}  {rec['label']}")
    print("done")


if __name__ == "__main__":
    main()
