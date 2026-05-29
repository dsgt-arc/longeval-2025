"""Multi-seed cross-encoder rerank evaluation for one date (issue #37).

Reads BM25 candidates + document/query/qrels parquet produced by the rebuilt
PACE pipeline (NOT raw TREC — that's `validate-rerank-single-date.py`), then for
each (seed, arm) scores nDCG@10 over a fixed 1k-query subsample and persists the
result. Works for train AND test dates because Stage-2 wrote `test/Qrels`.

Layout consumed:
  {root}/{train,test}/{Documents,Queries,Qrels}      (ParquetCollection)
  {bm25_root}/retrieval/date=<date>/sample_id=*       (qid, rank, docid, score)

Layout produced (idempotent via _SUCCESS):
  {out}/date=<date>/seed=<seed>/model=<safe>/         per-qid ndcg parquet
                                              /summary.parquet   one summary row

One array task = one date; seeds x arms loop inside so the per-date Spark prep
(candidate join) runs once. Example:
  scripts/rerank-eval.py --date 2023-01 \
      --root ~/scratch/longeval/2025/parquet \
      --bm25-root ~/scratch/longeval/2025/bm25 \
      --out ~/scratch/longeval/2025/rerank
"""

import gc
import sys
import time
from pathlib import Path

import pandas as pd
import typer
from pyspark.sql import Window
from pyspark.sql import functions as F

from longeval.collection import ParquetCollection
from longeval.experiment.bm25.evaluation import score_search
from longeval.spark import get_spark

app = typer.Typer()

CONTROL = "antoinelouis/crossencoder-camembert-base-mmarcoFR"
JINA = "jinaai/jina-reranker-v2-base-multilingual"
DEFAULT_MODELS = f"bm25,{CONTROL},{JINA}"

TRAIN_DATES = {
    "2022-06", "2022-07", "2022-08", "2022-09", "2022-10",
    "2022-11", "2022-12", "2023-01", "2023-02",
}


def _split_for_date(date: str) -> str:
    return "train" if date in TRAIN_DATES else "test"


def _safe(model: str) -> str:
    return model.replace("/", "__")


def _build_candidates(date: str, root: str, bm25_root: str, retrieval_prefix: str = "retrieval"):
    """BM25 top-k for `date` with document `contents` + query text joined on.
    Returns pandas [qid, query, docid, contents, score] and the qrels Spark df
    (train+test unioned, filtered to the date). `retrieval_prefix` switches
    between `retrieval/` (original queries) and `retrieval_expanded/` (LLM-
    expanded queries) — the candidate set differs; the rerank query stays the
    original user query from the queries parquet."""
    spark = get_spark()
    train = ParquetCollection(spark, f"{root}/train")
    test = ParquetCollection(spark, f"{root}/test")

    retrieval = spark.read.parquet(f"{bm25_root}/{retrieval_prefix}/date={date}").select(
        "qid", "docid", "score"
    )

    docs = (train.documents.union(test.documents)).where(F.col("date") == date)
    win = Window.partitionBy("docid").orderBy(F.desc(F.length("contents")))
    docs = (
        docs.where(F.length("contents") > 50)
        .withColumn("_r", F.row_number().over(win))
        .where(F.col("_r") == 1)
        .select("docid", "contents")
    )

    queries = (
        (train.queries.union(test.queries))
        .where(F.col("date") == date)
        .select("qid", "query")
        .dropDuplicates(["qid"])
    )

    base = (
        retrieval.join(docs, on="docid", how="inner")
        .join(queries, on="qid", how="inner")
        .select("qid", "query", "docid", "contents", "score")
        .where(F.length("contents") > 0)
        .toPandas()
    )

    # qrels parquet carries the `date` partition column; filter on it (cheap)
    # rather than on a huge qid list. Union train+test so train and test dates
    # both resolve.
    qrels_parts = [train.qrels]
    if test.qrels is not None:
        qrels_parts.append(test.qrels)
    qrels = qrels_parts[0]
    for extra in qrels_parts[1:]:
        qrels = qrels.unionByName(extra, allowMissingColumns=True)
    qrels = qrels.where(F.col("date") == date).select("qid", "docid", "rel")
    return base, qrels


def _rerank(model_name: str, base: pd.DataFrame, batch_size: int, device: str, fp16: bool):
    """Flattened single-batch CrossEncoder forward; returns [qid, docid, score]."""
    import torch
    from sentence_transformers import CrossEncoder

    model = CrossEncoder(model_name, max_length=512, device=device, trust_remote_code=True)
    if fp16 and device == "cuda":
        # half precision roughly doubles throughput on V100; scores are
        # rank-equivalent for nDCG.
        try:
            model.model.half()
        except Exception as e:  # noqa: BLE001
            print(f"[warn] fp16 unavailable for {model_name}: {e}", file=sys.stderr)

    pairs = list(zip(base["query"].tolist(), base["contents"].tolist()))
    scores = model.predict(
        pairs, batch_size=batch_size, show_progress_bar=False, convert_to_numpy=True
    )
    out = base[["qid", "docid"]].copy()
    out["score"] = [float(s) for s in scores]

    del model
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return out


def _versions() -> dict:
    import sentence_transformers
    import torch
    import transformers

    v = {
        "torch": torch.__version__,
        "transformers": transformers.__version__,
        "sentence_transformers": sentence_transformers.__version__,
    }
    try:
        import ir_datasets

        v["ir_datasets"] = ir_datasets.__version__
    except Exception:  # noqa: BLE001
        pass
    return v


def _persist(out_dir: Path, per_qid: pd.DataFrame, summary: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    per_qid.to_parquet(out_dir / "ndcg_per_qid.parquet", index=False)
    pd.DataFrame([summary]).to_parquet(out_dir / "summary.parquet", index=False)
    (out_dir / "_SUCCESS").touch()


@app.command()
def main(
    date: str = typer.Option(..., help="Snapshot YYYY-MM to evaluate."),
    root: str = typer.Option("~/scratch/longeval/2025/parquet", help="Parquet root."),
    bm25_root: str = typer.Option("~/scratch/longeval/2025/bm25", help="BM25 output root."),
    retrieval_prefix: str = typer.Option(
        "retrieval",
        help="Subdir under bm25_root holding the candidate set "
        "(`retrieval` = original queries; `retrieval_expanded` = expanded).",
    ),
    out: str = typer.Option("~/scratch/longeval/2025/rerank", help="Results root."),
    models: str = typer.Option(DEFAULT_MODELS, help="Comma-separated arms; 'bm25' is the baseline."),
    seeds: str = typer.Option("42,1,2", help="Comma-separated subsample seeds."),
    sample_queries: int = typer.Option(1000, help="Queries per (date,seed); 0 = full set."),
    batch_size: int = typer.Option(64),
    fp16: bool = typer.Option(True, help="Half precision for cross-encoders on CUDA."),
    overwrite: bool = typer.Option(False, help="Recompute even if _SUCCESS exists."),
):
    import random

    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    root = str(Path(root).expanduser())
    bm25_root = str(Path(bm25_root).expanduser())
    out = str(Path(out).expanduser())
    model_list = [m.strip() for m in models.split(",") if m.strip()]
    seed_list = [int(s) for s in seeds.split(",") if s.strip()]
    split = _split_for_date(date)
    versions = _versions()

    base, qrels = _build_candidates(date, root, bm25_root, retrieval_prefix)
    spark = get_spark()
    uniq = sorted(base["qid"].unique())
    print(f"[{date}] {len(uniq)} qids, {len(base)} candidate pairs", file=sys.stderr, flush=True)

    for seed in seed_list:
        if sample_queries > 0:
            keep = set(random.Random(seed).sample(uniq, min(sample_queries, len(uniq))))
        else:
            keep = set(uniq)
        sub = base[base["qid"].isin(keep)].reset_index(drop=True)
        n_queries = sub["qid"].nunique()
        n_pairs = len(sub)

        for model_name in model_list:
            out_dir = Path(out) / f"date={date}" / f"seed={seed}" / f"model={_safe(model_name)}"
            if (out_dir / "_SUCCESS").exists() and not overwrite:
                print(f"[skip] {out_dir} (done)", file=sys.stderr, flush=True)
                continue

            t0 = time.time()
            if model_name == "bm25":
                scored = sub[["qid", "docid", "score"]].copy()
            else:
                try:
                    scored = _rerank(model_name, sub, batch_size, device, fp16)
                except Exception as e:  # noqa: BLE001 — one bad arm shouldn't kill the sweep
                    print(f"!! {model_name} @ {date} seed={seed}: {type(e).__name__}: {e}", file=sys.stderr)
                    continue
            wall_s = time.time() - t0

            results = spark.createDataFrame(scored)
            per_qid = score_search(results, qrels).toPandas()
            ndcg = per_qid["ndcg_cut_10"]
            summary = {
                "date": date,
                "split": split,
                "seed": seed,
                "model": model_name,
                "n_queries": int(n_queries),
                "n_pairs": int(n_pairs),
                "ndcg10_mean": float(ndcg.mean()),
                "ndcg10_std": float(ndcg.std()),
                "wall_s": round(wall_s, 2),
                "pairs_per_s": round(n_pairs / wall_s, 1) if wall_s > 0 else None,
                "k_candidates": int(len(sub) / max(n_queries, 1)),
                "max_seq_len": 512,
                "batch_size": batch_size,
                "fp16": bool(fp16 and device == "cuda" and model_name != "bm25"),
                **versions,
            }
            _persist(out_dir, per_qid, summary)
            print(
                f">> {date} seed={seed} {model_name}: nDCG@10={summary['ndcg10_mean']:.4f} "
                f"(n={n_queries}, {summary['pairs_per_s']} pairs/s)",
                file=sys.stderr,
                flush=True,
            )


if __name__ == "__main__":
    app()
