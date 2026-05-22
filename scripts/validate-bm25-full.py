"""Full 15-date BM25 baseline replication against the working-notes paper.

For every train (2022-06..2023-02) and test (2023-03..2023-08) slice:
  1. mirror the slice's TREC files as .trec hardlinks (the test + 2023-02
     slices ship as misnamed *.jsonl.gz that are really TREC XML; pyserini's
     gz reader dies on them, so we relink to a .trec name on the same device),
  2. index with BM25IndexFromTrecTask (pyserini, language=fr),
  3. retrieve top-100 with the repo's run_search, strip the "doc" DOCNO prefix,
  4. score with the repo's score_search against the right qrels per split.

Writes per-query scores to parquet per date, prints a per-date table plus the
pooled mean across all queries (the paper's aggregate bm25 = 0.242).
"""

import glob
import json
import os
from pathlib import Path

import luigi
import typer
from pyspark.sql import functions as F

from longeval.experiment.bm25.workflow import BM25IndexFromTrecTask
from longeval.experiment.bm25.retrieval import run_search
from longeval.experiment.bm25.evaluation import score_search
from longeval.spark import get_spark

app = typer.Typer()

TRAIN_ROOT = (
    "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/"
    "release_2025_p1/French/LongEval Train Collection"
)
TEST_ROOT = (
    "/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
    "LongEval Test Collection"
)
TEST_QRELS = (
    Path("~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels").expanduser()
)

TRAIN_DATES = [
    "2022-06", "2022-07", "2022-08", "2022-09", "2022-10",
    "2022-11", "2022-12", "2023-01", "2023-02",
]
TEST_DATES = ["2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"]


def mirror_trec(date: str, src_root: str, mirror_root: str) -> str:
    """Hardlink a slice's files to <mirror_root>/Trec/<date>_fr/*.trec.

    All-.trec dirs are linked as-is; misnamed files get a .trec suffix. Same
    /mnt/data device => instant hardlinks, no copy. Returns mirror_root so it
    can be passed as BM25IndexFromTrecTask.trec_input_path.
    """
    src_dir = f"{src_root}/Trec/{date}_fr"
    dst_dir = f"{mirror_root}/Trec/{date}_fr"
    os.makedirs(dst_dir, exist_ok=True)
    files = [f for f in glob.glob(f"{src_dir}/**/*", recursive=True) if os.path.isfile(f)]
    for f in files:
        name = os.path.basename(f)
        link = os.path.join(dst_dir, name if name.endswith(".trec") else f"{name}.trec")
        if not os.path.exists(link):
            os.link(f, link)
    return mirror_root


@app.command()
def main(
    output_path: str = typer.Option("/mnt/data/tmp/longeval-bm25-validate"),
    mirror_root: str = typer.Option("/mnt/data/tmp/longeval-trec-mirror-bm25"),
    parallelism: int = typer.Option(8),
):
    spark = get_spark()
    rows = []

    for split, dates, src_root in (
        ("train", TRAIN_DATES, TRAIN_ROOT),
        ("test", TEST_DATES, TEST_ROOT),
    ):
        for date in dates:
            score_out = f"{output_path}/scores/date={date}"
            if Path(f"{score_out}/_SUCCESS").exists():
                agg = _read_agg(spark, score_out)
                rows.append({"date": date, "split": split, **agg, "cached": True})
                print(f"[{date}] cached: NDCG@10={agg['ndcg10_mean']:.4f}", flush=True)
                continue

            mirror_trec(date, src_root, mirror_root)
            ok = luigi.build(
                [
                    BM25IndexFromTrecTask(
                        input_path=mirror_root,
                        output_path=output_path,
                        date=date,
                        parallelism=parallelism,
                        trec_input_path=mirror_root,
                    )
                ],
                workers=1, local_scheduler=True, log_level="INFO",
            )
            if not ok:
                raise RuntimeError(f"indexing failed for {date}")

            index_path = f"{output_path}/index_trec/date={date}"

            q_root = src_root
            queries = spark.read.csv(
                f"{q_root}/queries/{date}_queries.txt",
                sep="\t", schema="qid STRING, query STRING",
            ).where(F.col("query").isNotNull())

            results = run_search(queries, index_path, k=100)
            results = results.withColumn("docid", F.regexp_replace("docid", "^doc", ""))

            if split == "train":
                qrels = spark.read.csv(
                    f"{src_root}/qrels/{date}_fr/qrels_processed.txt",
                    sep=" ", schema="qid STRING, rank INT, docid STRING, rel INT",
                )
            else:
                qrels = spark.read.csv(
                    f"{TEST_QRELS}/{date}/qrels_processed.txt",
                    sep=" ", schema="qid STRING, rank INT, docid STRING, rel INT",
                )

            scored = score_search(results, qrels)
            scored.repartition(1).write.parquet(score_out, mode="overwrite")
            Path(f"{score_out}/_SUCCESS").touch()
            agg = _agg(scored)
            rows.append({"date": date, "split": split, **agg, "cached": False})
            print(f"[{date}] NDCG@10={agg['ndcg10_mean']:.4f} (n={agg['n']})", flush=True)

    # pooled mean across every scored query (the paper's aggregate)
    allscores = spark.read.parquet(f"{output_path}/scores")
    pooled = allscores.agg(
        F.count("*").alias("n"),
        F.mean("ndcg_cut_10").alias("ndcg10_mean"),
        F.stddev("ndcg_cut_10").alias("ndcg10_std"),
    ).collect()[0]

    print("\n==== Full BM25 baseline replication ====")
    print(f"{'date':<10}{'split':<7}{'NDCG@10':>9}{'queries':>9}")
    for r in sorted(rows, key=lambda x: x["date"]):
        print(f"{r['date']:<10}{r['split']:<7}{r['ndcg10_mean']:>9.4f}{r['n']:>9}")
    print("-" * 35)
    print(f"pooled NDCG@10 mean : {pooled['ndcg10_mean']:.4f}  (paper bm25 = 0.242)")
    print(f"pooled queries      : {pooled['n']}")

    Path(f"{output_path}/summary.json").write_text(
        json.dumps({"per_date": rows, "pooled_ndcg10": pooled["ndcg10_mean"],
                    "pooled_n": pooled["n"]}, indent=2)
    )


def _agg(scored):
    a = scored.agg(
        F.count("*").alias("n"),
        F.mean("ndcg_cut_10").alias("ndcg10_mean"),
        F.stddev("ndcg_cut_10").alias("ndcg10_std"),
    ).collect()[0]
    return {"n": a["n"], "ndcg10_mean": a["ndcg10_mean"], "ndcg10_std": a["ndcg10_std"]}


def _read_agg(spark, score_out):
    return _agg(spark.read.parquet(score_out))


if __name__ == "__main__":
    app()
