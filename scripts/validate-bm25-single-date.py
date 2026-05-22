"""Single-date BM25 validation against the working-notes paper.

Bypasses the heavy full-corpus parquet ETL: indexes one raw TREC train slice
directly (BM25IndexFromTrecTask), runs the repo's own run_search + score_search,
and prints mean NDCG@10 for comparison with the paper's monthly value.
"""

import sys
from pathlib import Path

import luigi
import typer
from pyspark.sql import functions as F

from longeval.experiment.bm25.workflow import BM25IndexFromTrecTask
from longeval.experiment.bm25.retrieval import run_search
from longeval.experiment.bm25.evaluation import score_search
from longeval.spark import get_spark

app = typer.Typer()


@app.command()
def main(
    date: str = typer.Option("2023-01"),
    train_root: str = typer.Option(
        "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/"
        "release_2025_p1/French/LongEval Train Collection"
    ),
    output_path: str = typer.Option("/mnt/data/tmp/longeval-bm25-validate"),
    parallelism: int = typer.Option(8),
):
    # 1. Build the Lucene index directly from raw TREC for this date.
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

    # 2. Queries straight from the raw TSV for this slice.
    queries = spark.read.csv(
        f"{train_root}/queries/{date}_queries.txt",
        sep="\t",
        schema="qid STRING, query STRING",
    ).where(F.col("query").isNotNull())
    n_q = queries.count()

    # 3. Retrieve top-100 (same call the pipeline makes).
    results = run_search(queries, index_path, k=100)
    # TREC DOCNOs carry a "doc" prefix (doc14290) but qrels_processed.txt
    # uses bare integers (14290) — normalize so the score join matches.
    results = results.withColumn("docid", F.regexp_replace("docid", "^doc", ""))

    # 4. Qrels straight from the raw file.
    qrels = spark.read.csv(
        f"{train_root}/qrels/{date}_fr/qrels_processed.txt",
        sep=" ",
        schema="qid STRING, rank INT, docid STRING, rel INT",
    )

    # 5. Score with the repo's evaluator.
    scored = score_search(results, qrels)
    agg = scored.agg(
        F.count("*").alias("n"),
        F.mean("ndcg_cut_10").alias("ndcg10_mean"),
        F.stddev("ndcg_cut_10").alias("ndcg10_std"),
        F.mean("map").alias("map_mean"),
    ).collect()[0]

    print("\n==== BM25 single-date validation ====", file=sys.stderr)
    print(f"date           : {date}", file=sys.stderr)
    print(f"queries (raw)  : {n_q}", file=sys.stderr)
    print(f"queries scored : {agg['n']}", file=sys.stderr)
    print(f"NDCG@10 mean   : {agg['ndcg10_mean']:.4f}", file=sys.stderr)
    print(f"NDCG@10 std    : {agg['ndcg10_std']:.4f}", file=sys.stderr)
    print(f"MAP mean       : {agg['map_mean']:.4f}", file=sys.stderr)


if __name__ == "__main__":
    app()
