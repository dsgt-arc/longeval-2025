"""Single-date cross-encoder rerank validation against the working-notes paper.

Mirrors validate-bm25-single-date.py, then adds the rerank stage from the
out-of-DAG pipeline (user/acmiyaguchi/rerank): join document `contents` onto the
BM25 top-k, rerank each query's candidates with a cross-encoder via the
`rerankers` lib, and score the rerank_score ordering with the repo's evaluator.

The expensive prep (index build, retrieval, contents+query join) runs ONCE; the
model list is then swept over the same candidate set so arms are directly
comparable. Paper monthly bm25-reranked values for reference (camembert-base):
2023-01 = 0.379, full-period mean = 0.296.

GPU note (NixOS): the PyPI torch wheel needs the driver's libcuda.so on the
loader path, so run with
    LD_LIBRARY_PATH=/run/opengl-driver/lib .venv/bin/python scripts/validate-rerank-single-date.py ...
"""

import sys
from pathlib import Path

import luigi
import pandas as pd
import typer
from pyspark.sql import Window
from pyspark.sql import functions as F

from longeval.collection import TrecCollection
from longeval.experiment.bm25.evaluation import score_search
from longeval.experiment.bm25.retrieval import run_search
from longeval.experiment.bm25.workflow import BM25IndexFromTrecTask
from longeval.spark import get_spark

app = typer.Typer()

# Default sweep: control (paper model) + French-native upgrades + multilingual SOTA.
DEFAULT_MODELS = [
    "antoinelouis/crossencoder-camembert-base-mmarcoFR",  # control = paper model
    "antoinelouis/crossencoder-camembert-large-mmarcoFR",
    "antoinelouis/crossencoder-camemberta-L10-mmarcoFR",
    "BAAI/bge-reranker-v2-m3",
    "jinaai/jina-reranker-v2-base-multilingual",
]


def _build_candidates(
    date: str,
    train_root: str,
    output_path: str,
    k: int,
    parallelism: int,
    expanded_root: str = None,
):
    """BM25 top-k with document contents + query text joined on. Returns a pandas
    frame [qid, query, docid, contents, score] and the qrels Spark df.

    If `expanded_root` is set, the additive-expansion JSON
    ({expanded_root}/expansion/*.json, field `query`) is left-joined onto the
    queries and coalesced over the original (fallback to original for qids with
    no expansion). The expanded text then drives BOTH the BM25 retrieval and the
    `query` column carried into reranking — i.e. the paper's expanded-reranked
    semantics."""
    ok = luigi.build(
        [
            BM25IndexFromTrecTask(
                input_path=train_root,
                output_path=output_path,
                date=date,
                parallelism=parallelism,
                trec_input_path=train_root,
            )
        ],
        workers=1,
        local_scheduler=True,
        log_level="INFO",
    )
    if not ok:
        raise RuntimeError("indexing failed")

    index_path = f"{output_path}/index_trec/date={date}"
    spark = get_spark()

    queries = spark.read.csv(
        f"{train_root}/queries/{date}_queries.txt",
        sep="\t",
        schema="qid STRING, query STRING",
    ).where(F.col("query").isNotNull())

    if expanded_root:
        expanded = spark.read.json(
            str(Path(expanded_root).expanduser() / "expansion" / "*.json"),
            multiLine=True,
        ).select("qid", F.col("query").alias("expanded"))
        queries = (
            queries.join(expanded, on="qid", how="left")
            .withColumn("query", F.coalesce("expanded", "query"))
            .drop("expanded")
        )

    # BM25 top-k. docid keeps its raw DOCNO (e.g. "doc14290") here so it matches
    # the TREC contents join; we strip the "doc" prefix only for the qrels score.
    results = run_search(queries, index_path, k=k)

    # Document contents straight from raw TREC for this slice; dedup to one row
    # per docid (longest contents), matching the pipeline's join_retrieval.py.
    docs = TrecCollection(spark, train_root, dates=[date]).documents
    win = Window.partitionBy("docid").orderBy(F.desc(F.length("contents")))
    docs = (
        docs.where(F.length("contents") > 50)
        .withColumn("_r", F.row_number().over(win))
        .where(F.col("_r") == 1)
        .select("docid", "contents")
    )

    joined = (
        results.join(docs, on="docid", how="inner")
        .join(queries, on="qid", how="inner")
        .select("qid", "query", "docid", "contents", "score")
        .where(F.length("contents") > 0)
    )
    base = joined.toPandas()

    qrels = spark.read.csv(
        f"{train_root}/qrels/{date}_fr/qrels_processed.txt",
        sep=" ",
        schema="qid STRING, rank INT, docid STRING, rel INT",
    )
    return base, qrels


def _rerank(model_name: str, base: pd.DataFrame, batch_size: int, device: str) -> pd.DataFrame:
    """Rerank with a single flattened large-batch forward; returns
    [qid, docid, score=rerank_score].

    The pipeline's reranker.py dispatches one rank_async per query (≤100 docs);
    on this GPU that's ~3 q/s — dominated by per-query Python/dispatch overhead,
    not compute. Flattening ALL (query, contents) pairs into one CrossEncoder
    .predict with a large batch keeps the GPU saturated and is ~5-10x faster.
    Scores are identical — the cross-encoder sees the exact same pairs."""
    import gc

    import torch
    from sentence_transformers import CrossEncoder

    model = CrossEncoder(
        model_name, max_length=512, device=device, trust_remote_code=True
    )
    pairs = list(zip(base["query"].tolist(), base["contents"].tolist()))
    scores = model.predict(
        pairs, batch_size=batch_size, show_progress_bar=False, convert_to_numpy=True
    )
    out = base[["qid", "docid"]].copy()
    out["score"] = [float(s) for s in scores]

    # Release GPU memory before the next model loads (one model on-device at a time).
    del model
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return out


@app.command()
def main(
    date: str = typer.Option("2023-01"),
    models: str = typer.Option(
        ",".join(DEFAULT_MODELS),
        help="Comma-separated HF reranker model ids to sweep over the same candidates.",
    ),
    train_root: str = typer.Option(
        "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/"
        "release_2025_p1/French/LongEval Train Collection"
    ),
    output_path: str = typer.Option("/mnt/data/tmp/longeval-bm25-validate"),
    k: int = typer.Option(100, help="BM25 candidates per query to rerank."),
    batch_size: int = typer.Option(64, help="Flattened CrossEncoder.predict batch size."),
    parallelism: int = typer.Option(8),
    sample_queries: int = typer.Option(
        0,
        help="If >0, rerank a fixed random sample of this many qids (seed=42) instead "
        "of all queries — for fast model comparison. 0 = full set (faithful).",
    ),
    expanded_root: str = typer.Option(
        None,
        help="If set, retrieve + rerank with additive-expansion queries from "
        "{expanded_root}/expansion/*.json (e.g. ~/scratch/longeval/query_expansion/french).",
    ),
):
    import random

    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model_list = [m.strip() for m in models.split(",") if m.strip()]

    base, qrels = _build_candidates(
        date, train_root, output_path, k, parallelism, expanded_root
    )

    if sample_queries > 0:
        uniq = sorted(base["qid"].unique())
        keep = set(random.Random(42).sample(uniq, min(sample_queries, len(uniq))))
        base = base[base["qid"].isin(keep)].reset_index(drop=True)
        print(
            f"[subsample] {len(keep)} of {len(uniq)} qids (seed=42)",
            file=sys.stderr,
            flush=True,
        )

    spark = get_spark()

    # BM25 baseline over this same candidate set, for reference.
    bm25_results = spark.createDataFrame(
        base[["qid", "docid", "score"]]
    ).withColumn("docid", F.regexp_replace("docid", "^doc", ""))
    bm25_agg = (
        score_search(bm25_results, qrels)
        .agg(F.mean("ndcg_cut_10").alias("m"), F.stddev("ndcg_cut_10").alias("s"))
        .collect()[0]
    )

    table = [("bm25 (baseline)", bm25_agg["m"], bm25_agg["s"])]
    for model_name in model_list:
        try:
            reranked = _rerank(model_name, base, batch_size, device)
        except Exception as e:  # one bad model shouldn't kill the sweep
            print(f"!! {model_name}: {type(e).__name__}: {e}", file=sys.stderr)
            table.append((model_name, None, None))
            continue
        results = spark.createDataFrame(reranked).withColumn(
            "docid", F.regexp_replace("docid", "^doc", "")
        )
        agg = (
            score_search(results, qrels)
            .agg(F.mean("ndcg_cut_10").alias("m"), F.stddev("ndcg_cut_10").alias("s"))
            .collect()[0]
        )
        table.append((model_name, agg["m"], agg["s"]))
        print(f">> done {model_name}: nDCG@10={agg['m']:.4f}", file=sys.stderr, flush=True)

    print("\n==== rerank single-date sweep ====", file=sys.stderr)
    print(f"date    : {date}", file=sys.stderr)
    print(f"expanded: {expanded_root or '(none — original queries)'}", file=sys.stderr)
    print(f"device  : {device}   k={k}   queries={base['qid'].nunique()}", file=sys.stderr)
    print(f"{'model':<55} {'nDCG@10':>9} {'std':>7}", file=sys.stderr)
    for name, m, s in table:
        if m is None:
            print(f"{name:<55} {'ERR':>9} {'':>7}", file=sys.stderr)
        else:
            print(f"{name:<55} {m:>9.4f} {s:>7.4f}", file=sys.stderr)


if __name__ == "__main__":
    app()
