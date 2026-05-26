"""Query-set statistics for the LongEval-Web French collection.

Mirrors the paper's document-stats methodology (whitespace + tiktoken
tokenizers) but for queries, which the dataset section currently omits.
Emits a per-date + pooled table (markdown and LaTeX) plus a JSON dump.
"""

import glob
import json
import os
import statistics
from collections import Counter

import tiktoken
import typer

app = typer.Typer()

TRAIN = (
    "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
    "French/LongEval Train Collection/queries"
)
TEST = (
    "/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
    "LongEval Test Collection/queries"
)


def _read(f):
    out = []
    for ln in open(f, encoding="utf-8"):
        p = ln.rstrip("\n").split("\t")
        if len(p) < 2 or not p[1].strip():
            continue
        out.append((p[0], p[1]))
    return out


@app.command()
def main(
    out_json: str = typer.Option("/mnt/data/tmp/longeval-query-stats.json"),
    long_word_threshold: int = typer.Option(50, help="Flag queries longer than this."),
):
    enc = tiktoken.get_encoding("cl100k_base")
    files = sorted(glob.glob(f"{TRAIN}/*_queries.txt"), key=os.path.basename)
    files += sorted(glob.glob(f"{TEST}/*_queries.txt"), key=os.path.basename)

    rows = []
    pooled_w, pooled_t, pooled_c = [], [], []
    uniq = {}  # qid -> text (identical text per qid across dates)
    outliers = []

    for f in files:
        date = os.path.basename(f).split("_")[0]
        split = "train" if "Train" in f else "test"
        qs = _read(f)
        w = [len(q.split()) for _, q in qs]
        for qid, q in qs:
            uniq[qid] = q
            if len(q.split()) > long_word_threshold:
                outliers.append((date, qid, len(q.split())))
        t = [len(enc.encode(q)) for _, q in qs]
        c = [len(q) for _, q in qs]
        pooled_w += w
        pooled_t += t
        pooled_c += c
        rows.append({
            "date": date, "split": split, "n": len(qs),
            "avg_words": statistics.mean(w), "med_words": statistics.median(w),
            "std_words": statistics.pstdev(w),
            "avg_tokens": statistics.mean(t), "avg_chars": statistics.mean(c),
            "max_words": max(w),
        })

    uw = [len(q.split()) for q in uniq.values()]
    ut = [len(enc.encode(q)) for q in uniq.values()]
    uc = [len(q) for q in uniq.values()]

    def agg(name, w, t, c, n):
        return {
            "scope": name, "n": n,
            "avg_words": statistics.mean(w), "med_words": statistics.median(w),
            "std_words": statistics.pstdev(w),
            "avg_tokens": statistics.mean(t), "avg_chars": statistics.mean(c),
            "max_words": max(w),
        }

    pooled = agg("pooled (instances)", pooled_w, pooled_t, pooled_c, len(pooled_w))
    unique = agg("unique queries", uw, ut, uc, len(uw))
    dist = Counter(uw)

    # ---- markdown ----
    print("\n### Query statistics (LongEval-Web French)\n")
    print("| date | split | queries | avg words | med | std | avg tokens | avg chars |")
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(f"| {r['date']} | {r['split']} | {r['n']:,} | {r['avg_words']:.2f} | "
              f"{r['med_words']:.0f} | {r['std_words']:.2f} | {r['avg_tokens']:.2f} | "
              f"{r['avg_chars']:.1f} |")
    for r in (pooled, unique):
        print(f"| **{r['scope']}** | — | {r['n']:,} | **{r['avg_words']:.2f}** | "
              f"{r['med_words']:.0f} | {r['std_words']:.2f} | {r['avg_tokens']:.2f} | "
              f"{r['avg_chars']:.1f} |")
    print(f"\nword-count distribution (unique): "
          f"{ {k: dist[k] for k in sorted(dist) if k <= 10} }")
    print(f"queries > {long_word_threshold} words (degenerate): {len(outliers)} "
          f"-> {outliers}")

    # ---- LaTeX (paper-ready, matches results_dataset.tex style) ----
    print("\n% --- LaTeX ---")
    print(r"\begin{tabular}{@{}llrrrr@{}}")
    print(r"\toprule")
    print(r"Scope & & Count & Avg Words & Avg Tokens & Avg Chars \\")
    print(r"\midrule")
    print(f"pooled & (query instances) & {pooled['n']} & {pooled['avg_words']:.2f} & "
          f"{pooled['avg_tokens']:.2f} & {pooled['avg_chars']:.1f} \\\\")
    print(f"unique & (distinct qids) & {unique['n']} & {unique['avg_words']:.2f} & "
          f"{unique['avg_tokens']:.2f} & {unique['avg_chars']:.1f} \\\\")
    print(r"\bottomrule")
    print(r"\end{tabular}")

    json.dump(
        {"per_date": rows, "pooled": pooled, "unique": unique,
         "word_dist_unique": dict(dist), "outliers": outliers},
        open(out_json, "w"), indent=2,
    )
    print(f"\nwrote {out_json}")


if __name__ == "__main__":
    app()
