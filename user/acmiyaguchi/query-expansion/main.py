import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import dotenv
import requests
import tqdm

# Secrets/overrides (OPENROUTER_API_KEY) live in the gitignored .env.local; load
# it relative to the repo root (run from there). Expansion only needs the query
# set, so there is no Spark/parquet dependency here.
dotenv.load_dotenv(".env.local", override=True)

# Raw query sources on this host (the parquet collection only exists on PACE).
TRAIN_QUERIES = Path(
    "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
    "French/queries.trec"
)
TEST_QUERIES_DIR = Path(
    "/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
    "LongEval Test Collection/queries"
)


def load_queries():
    """Unique (qid, query) across train + test, sorted by integer qid.

    Train is the TREC topic file (<num>/<title>); test is one TSV per date.
    First text seen for a qid wins, matching the dedup in the PACE pipeline.
    """
    rows = {}
    text = TRAIN_QUERIES.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"<num>(.*?)</num>\s*<title>(.*?)</title>", text, re.S):
        rows.setdefault(m.group(1).strip(), m.group(2).strip())
    for f in sorted(TEST_QUERIES_DIR.glob("*_queries.txt")):
        for line in f.read_text(encoding="utf-8").splitlines():
            if "\t" not in line:
                continue
            qid, query = line.split("\t", 1)
            rows.setdefault(qid.strip(), query.strip())
    return [{"qid": k, "query": v} for k, v in sorted(rows.items(), key=lambda kv: int(kv[0]))]


@dataclass(frozen=True)
class Variant:
    """One expansion configuration. The retrieval query is a flat, unweighted
    Lucene bag of words (retrieval.py:20), so term count is the dilution dial and
    there is no lambda to protect the original. ``max_terms`` bounds how many
    terms the model may add; ``original_weight`` repeats the original query text N
    times to boost its query-side term frequency so a larger ``max_terms`` does
    not outvote the core intent. ``allow_english`` permits English terms — note
    the index is French-analyzed (set_language('fr')), so English mostly stems to
    inert tokens; this arm exists to measure that, not because we expect a win.
    All knobs are meant to be swept against qrels rather than guessed.
    """

    term_floor: int
    max_terms: int
    original_weight: int
    allow_english: bool


VARIANTS = {
    "french": Variant(term_floor=3, max_terms=12, original_weight=2, allow_english=False),
    "english": Variant(term_floor=3, max_terms=12, original_weight=2, allow_english=True),
}


def get_schema(cfg: Variant):
    # Additive expansion: the model returns only NEW terms per query, never the
    # original text. The original is preserved verbatim downstream so BM25 term
    # weights are not diluted (the 100-word replacement approach lost them).
    desc = (
        "New French and English terms to append to the original query."
        if cfg.allow_english
        else "New French terms to append to the original query."
    )
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "qid": {
                    "type": "string",
                    "description": "The unique identifier for the query.",
                },
                "terms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": desc,
                },
            },
            "required": ["qid", "terms"],
            "additionalProperties": False,
        },
    }


def prompt(queries, cfg: Variant):
    query_text = "\n".join([f"{row['qid']}: {row['query']}" for row in queries])
    if cfg.allow_english:
        kind = "additional terms — French terms plus their English equivalents, synonyms, and related English terms"
        lang_rule = "- Provide French terms AND relevant English equivalents/synonyms (the corpus may contain English loanwords and names)."
    else:
        kind = "additional French terms"
        lang_rule = "- Use French only. Proper-noun, brand, and international entity names may keep their native spelling."
    return f"""You are expanding French web-search queries for a BM25 lexical retrieval engine.

For each query below, suggest {cfg.term_floor} to {cfg.max_terms} {kind} that a relevant document is likely to contain but that are NOT already in the query: synonyms, morphological/inflectional variants, closely related named entities, and common abbreviations or expansions.

Rules:
- Do NOT restate the original query words. Return only the new terms.
- Single words or short noun phrases only. No full sentences.
{lang_rule}
- No stopwords (le, la, de, et, à, ...), no punctuation, no duplicates.
- Stay tightly on the query's topic; do not drift to broader themes.
- If the query is already specific enough that expansion would only add noise, return an empty list.

Output a JSON array of objects, each with 'qid' (unchanged) and 'terms' (an array of the suggested terms, possibly empty).

{query_text}
"""


def chat_complete(
    queries,
    cfg: Variant,
    api_key=os.environ.get("OPENROUTER_API_KEY"),
    model="deepseek/deepseek-v4-flash",
    # model="deepseek/deepseek-chat-v3-0324",
    # model="google/gemini-2.5-flash-preview-05-20",
):
    completion = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=dict(
            model=model,
            # Alibaba/Baidu (Chinese platforms) apply an output content filter that
            # 502s on adult query terms — not the model refusing, just the host.
            # Prefer DeepInfra (cheap, ~25s normal); fall back to other Western
            # hosts, never the moderated ones.
            provider={
                "order": ["DeepInfra"],
                "allow_fallbacks": True,
                "ignore": ["Alibaba", "Baidu"],
            },
            messages=[
                {"role": "user", "content": [{"type": "text", "text": prompt(queries, cfg)}]}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "longeval",
                    "strict": True,
                    "schema": get_schema(cfg),
                },
            },
        ),
        # v4-flash is slow (~25s normal, but adult-content batches run ~180-240s
        # on every Western provider). Cap generously so those still complete but a
        # truly dead connection can't block a worker forever; killed batches raise,
        # are logged+skipped, and retried on the next resume run.
        timeout=300,
    )
    return completion.json()


def query_expansion(queries, output, cfg: Variant):
    # add the response to a logfile
    output = Path(output).expanduser()
    logfile = Path(output) / "completion/log.txt"
    # name is start-end range
    start = queries[0]["qid"]
    end = queries[-1]["qid"]
    output = Path(output) / f"expansion/{start}-{end}.json"
    if output.exists():
        # print(f"Output file {output} already exists, skipping.")
        return
    logfile.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    resp = chat_complete(queries, cfg)
    with logfile.open("a") as f:
        f.write(json.dumps(resp) + "\n")

    msg = resp["choices"][0]["message"]["content"]
    try:
        data = json.loads(msg)
    except Exception as e:
        raise ValueError(
            f"Failed to parse JSON response: {msg}\nError: {e}\nResponse: {resp}"
        )
    # check that all of the quids are present
    input_qids = {q["qid"] for q in queries}
    output_qids = {d["qid"] for d in data}
    if input_qids != output_qids:
        raise ValueError(
            f"Input and output qids do not match. {input_qids - output_qids}", data
        )

    # Merge the model's new terms onto the original query text. The original is
    # kept verbatim and dominates; we defensively drop any term whose words are
    # already present so the model can't restate (and dilute) the query.
    original_by_qid = {q["qid"]: q["query"] for q in queries}
    records = []
    for d in data:
        qid = d["qid"]
        original = original_by_qid[qid]
        present = {w.lower() for w in original.split()}
        seen = set()
        terms = []
        for t in d.get("terms", []):
            t = t.strip()
            key = t.lower()
            if not t or key in seen or all(w in present for w in key.split()):
                continue
            seen.add(key)
            terms.append(t)
        terms = terms[: cfg.max_terms]
        # Repeat the original original_weight times to boost its query-side term
        # frequency, so the appended terms cannot outvote the core intent.
        records.append(
            {
                "qid": qid,
                "query": " ".join([*([original] * cfg.original_weight), *terms]).strip(),
                "terms": terms,
                "original": original,
            }
        )

    # Write atomically: a kill mid-dump must not leave a truncated file that the
    # exists()-skip would then treat as done forever. Temp + os.replace (atomic on
    # the same filesystem) guarantees the file is either absent or complete.
    tmp = output.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    os.replace(tmp, output)

    # return the data
    return records


def main():
    variant_name = sys.argv[1] if len(sys.argv) > 1 else "french"
    if variant_name not in VARIANTS:
        raise SystemExit(
            f"unknown variant {variant_name!r}; choose one of {sorted(VARIANTS)}"
        )
    cfg = VARIANTS[variant_name]
    # Per-variant output dir so arms never collide; point workflow.py's
    # `expanded_path` at the one being scored.
    out_dir = f"~/scratch/longeval/query_expansion/{variant_name}"

    deduped_rows = load_queries()
    print(f"variant={variant_name} cfg={cfg} queries={len(deduped_rows)} -> {out_dir}")

    # Small batches: one bad/missing qid fails the round-trip check and discards
    # the whole batch, so 25 bounds the blast radius (vs. 100 previously).
    batch_size = 25
    batches = [
        deduped_rows[i : i + batch_size]
        for i in range(0, len(deduped_rows), batch_size)
    ]

    def process_batch(batch, idx):
        try:
            query_expansion(batch, out_dir, cfg)
        except Exception as e:
            print(batch)
            print(
                f"Error processing batch {idx * batch_size}-{(idx + 1) * batch_size}: {e}"
            )

    # Pure network-wait work; v4-flash is ~36s/call, so over-subscribe to keep
    # wall-clock down (24 workers ~= 1h/arm for the full set).
    with ThreadPoolExecutor(max_workers=24) as executor:
        futures = [
            executor.submit(process_batch, batch, idx)
            for idx, batch in enumerate(batches)
        ]
        for _ in tqdm.tqdm(as_completed(futures), total=len(futures)):
            pass


if __name__ == "__main__":
    main()
