"""Aggregate the per-(date,seed,model) rerank summaries into issue-#37 outputs.

Reads every `{rerank_root}/date=*/seed=*/model=*/summary.parquet` and emits:
  * results.csv         — tidy one-row-per-(date,seed,model) table
  * results.md          — per-date nDCG@10 table (mean±std over seeds) + the
                          across-dates summary (train-9 / test-6 / pooled-15)
  * manifest.json       — pinned versions, k, seeds, git commit, per-model
                          throughput, to satisfy the reproducibility ask

Usage:
    scripts/aggregate-rerank-results.py --rerank-root ~/scratch/longeval/2025/rerank
"""

import json
import subprocess
from pathlib import Path

import pandas as pd
import typer

app = typer.Typer()

# short display labels for the arms; COL_ORDER controls table column order.
# Extras (large/L10/bge) added in issue #37 phase 2 — same 1k 3-seed protocol,
# results land in the same rerank/ root.
LABELS = {
    "bm25": "bm25",
    "antoinelouis/crossencoder-camembert-base-mmarcoFR": "camembert-base",
    "antoinelouis/crossencoder-camembert-large-mmarcoFR": "camembert-large",
    "antoinelouis/crossencoder-camemberta-L10-mmarcoFR": "camemberta-L10",
    "BAAI/bge-reranker-v2-m3": "bge-v2-m3",
    "jinaai/jina-reranker-v2-base-multilingual": "jina-v2",
}
COL_ORDER = [
    "bm25", "camembert-base", "camemberta-L10", "camembert-large", "bge-v2-m3", "jina-v2",
]
TRAIN_DATES = [
    "2022-06", "2022-07", "2022-08", "2022-09", "2022-10",
    "2022-11", "2022-12", "2023-01", "2023-02",
]


def _label(model: str) -> str:
    return LABELS.get(model, model)


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).parent
        ).decode().strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def _load(rerank_root: Path) -> pd.DataFrame:
    rows = [pd.read_parquet(p) for p in rerank_root.glob("date=*/seed=*/model=*/summary.parquet")]
    if not rows:
        raise typer.BadParameter(f"No summary.parquet under {rerank_root}")
    df = pd.concat(rows, ignore_index=True)
    df["arm"] = df["model"].map(_label)
    return df


def _fmt(mean: float, std: float) -> str:
    # single-seed cells have undefined std (groupby std on n=1 → NaN); for a
    # cleaner table just show the mean.
    if std is None or pd.isna(std) or std == 0.0:
        return f"{mean:.4f}"
    return f"{mean:.4f}±{std:.4f}"


@app.command()
def main(
    rerank_root: Path = typer.Option(
        Path("~/scratch/longeval/2025/rerank").expanduser(), help="Rerank results root."
    ),
    out_dir: Path = typer.Option(
        Path("~/scratch/longeval/2025/rerank").expanduser(), help="Where to write outputs."
    ),
):
    rerank_root = rerank_root.expanduser()
    out_dir = out_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    df = _load(rerank_root)

    df.sort_values(["date", "arm", "seed"]).to_csv(out_dir / "results.csv", index=False)

    # per-date cell = mean±std of the per-seed ndcg10_mean values across seeds
    cell = (
        df.groupby(["date", "split", "arm"])["ndcg10_mean"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    dates = sorted(df["date"].unique())
    # Only show columns that actually have data in this rerank root — keeps the
    # full-query rerank-full/ table (3 arms) from showing empty extras columns
    # and vice-versa.
    present = set(df["arm"].unique())
    col_order = [c for c in COL_ORDER if c in present]
    lines = ["# Issue #37 — reranker nDCG@10 across LongEval-Web dates", ""]
    seeds = sorted(int(s) for s in df["seed"].unique())
    seed_blurb = f"mean±std over seeds {seeds}" if len(seeds) > 1 else f"single seed={seeds[0]}, no std"
    sample = int(df["n_queries"].median())
    sample_blurb = f"{sample}-query subsample" if sample <= 2000 else "full query set"
    lines.append(f"Per-date cells are {seed_blurb}; {sample_blurb}, k=100.")
    lines.append("")
    header = "| date | split | " + " | ".join(col_order) + " |"
    lines.append(header)
    lines.append("|" + "---|" * (len(col_order) + 2))
    for date in dates:
        split = "train" if date in TRAIN_DATES else "test"
        cells = []
        for arm in col_order:
            row = cell[(cell["date"] == date) & (cell["arm"] == arm)]
            cells.append(_fmt(row["mean"].iloc[0], (row["std"].iloc[0] or 0.0)) if len(row) else "—")
        lines.append(f"| {date} | {split} | " + " | ".join(cells) + " |")

    # across-dates summary: per-date value = mean over seeds; then mean±std over dates
    per_date_mean = cell.set_index(["date", "arm"])["mean"]
    lines += ["", "## Across-dates summary (mean±std over dates)", ""]
    lines.append("| group | " + " | ".join(col_order) + " |")
    lines.append("|" + "---|" * (len(col_order) + 1))
    groups = {
        "train (9)": TRAIN_DATES,
        "test (6)": [d for d in dates if d not in TRAIN_DATES],
        "pooled (15)": dates,
    }
    for gname, gdates in groups.items():
        cells = []
        for arm in col_order:
            vals = [per_date_mean.get((d, arm)) for d in gdates if (d, arm) in per_date_mean.index]
            vals = pd.Series([v for v in vals if v is not None])
            cells.append(_fmt(vals.mean(), vals.std()) if len(vals) else "—")
        lines.append(f"| {gname} | " + " | ".join(cells) + " |")

    (out_dir / "results.md").write_text("\n".join(lines) + "\n")

    # reproducibility manifest
    vcols = [c for c in ("torch", "transformers", "sentence_transformers", "ir_datasets") if c in df]
    versions = {c: sorted(df[c].dropna().unique().tolist()) for c in vcols}
    tput = {
        k: float(v)
        for k, v in (
            df[df["model"] != "bm25"]
            .groupby("arm")["pairs_per_s"]
            .mean()
            .round(1)
            .to_dict()
            .items()
        )
    }
    manifest = {
        "git_commit": _git_commit(),
        "dates": dates,
        "seeds": seeds,
        "sample_queries": int(df["n_queries"].max()),
        "k_candidates": int(df["k_candidates"].median()),
        "max_seq_len": int(df["max_seq_len"].iloc[0]),
        "batch_size": int(df["batch_size"].iloc[0]),
        "models": sorted(df["model"].unique().tolist()),
        "versions": versions,
        "mean_pairs_per_s_by_arm": tput,
        "total_runs": int(len(df)),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    typer.echo((out_dir / "results.md").read_text())
    typer.echo(f"\nwrote results.csv, results.md, manifest.json to {out_dir}")


if __name__ == "__main__":
    app()
