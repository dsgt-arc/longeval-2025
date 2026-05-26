"""Evaluate retrieval and reranking outputs.

This workflow combines training qrels with held-out qrels, scores retrieval
outputs with pytrec_eval, and writes per-query metrics for comparison.
"""

import typer
import luigi
from longeval.spark import spark_resource
from longeval.collection import ParquetCollection
from .evaluation import score_search
from pathlib import Path
from pyspark.sql import functions as F

app = typer.Typer()


class QrelTask(luigi.Task):
    shared_path = luigi.Parameter()
    scratch_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.shared_path}/evaluation/qrels/_SUCCESS")

    def run(self):
        # we need the collection and the new qrels
        with spark_resource() as spark:
            train_collection = ParquetCollection(
                spark, f"{self.shared_path}/2025/parquet/train"
            )
            test_qrel_root = Path(
                f"{self.scratch_path}/raw/2025/test-qrels/longeval_web_qrels"
            )
            test_qrel = spark.read.csv(
                f"{(test_qrel_root).as_posix()}/*/*",
                sep=" ",
                schema="qid STRING, rank INT, docid STRING, rel INT",
            ).withColumn(
                "date",
                F.udf(lambda x: Path(x).parent.name, "string")(F.input_file_name()),
            )

            (
                train_collection.qrels.select("date", "qid", "docid", "rel")
                .union(test_qrel.select("date", "qid", "docid", "rel"))
                .coalesce(8)
                .write.partitionBy("date")
                .parquet(
                    Path(self.output().path).parent.as_posix(),
                    mode="overwrite",
                )
            )


class EvaluationTask(luigi.Task):
    """Evaluate the BM25 index using the specified queries and qrels."""

    shared_path = luigi.Parameter()
    scratch_path = luigi.Parameter()
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    date = luigi.Parameter()
    filetype = luigi.Parameter(default="parquet")

    def requires(self):
        return QrelTask(
            shared_path=self.shared_path,
            scratch_path=self.scratch_path,
        )

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/date={self.date}/_SUCCESS")

    def _read_csv(self, spark, path):
        return spark.read.csv(path, header=True, inferSchema=True).select(
            "qid", "docid", F.col("rerank_score").alias("score")
        )

    def _read_parquet(self, spark, path):
        return spark.read.parquet(path)

    def run(self):
        with spark_resource() as spark:
            # load the output of the retrieval task
            reader = self._read_csv if self.filetype == "csv" else self._read_parquet
            results = reader(spark, f"{self.input_path}/date={self.date}")
            qrels = spark.read.parquet(
                f"{self.shared_path}/evaluation/qrels/date={self.date}"
            )
            res = score_search(results, qrels)
            res.repartition(1).write.parquet(
                Path(self.output().path).parent.as_posix(), mode="overwrite"
            )


class EvaluationWrapperTask(luigi.WrapperTask):
    """Wrapper task to run the evaluation for all dates."""

    shared_path = luigi.Parameter()
    scratch_path = luigi.Parameter()
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    filetype = luigi.Parameter(default="parquet")

    def requires(self):
        return [
            EvaluationTask(
                shared_path=self.shared_path,
                scratch_path=self.scratch_path,
                input_path=self.input_path,
                output_path=self.output_path,
                filetype=self.filetype,
                date=date,
            )
            for date in [
                "2022-06",
                "2022-07",
                "2022-08",
                "2022-09",
                "2022-10",
                "2022-11",
                "2022-12",
                "2023-01",
                "2023-02",
                "2023-03",
                "2023-04",
                "2023-05",
                "2023-06",
                "2023-07",
                "2023-08",
            ]
        ]


@app.command()
def run_evaluate(workers: int = 8):
    shared_root = Path("~/shared/longeval").expanduser()
    scratch_root = Path("~/scratch/longeval").expanduser()

    # now let's run the evaluation on just the train dates
    res = luigi.build(
        [
            EvaluationWrapperTask(
                shared_path=shared_root.as_posix(),
                scratch_path=scratch_root.as_posix(),
                input_path=input_path,
                output_path=output_path,
                filetype=filetype,
            )
            for input_path, output_path, filetype in [
                (
                    f"{shared_root}/2025/bm25/retrieval",
                    f"{shared_root}/evaluation/scores/experiment=bm25",
                    "parquet",
                ),
                (
                    f"{shared_root}/2025/bm25/retrieval_expanded",
                    f"{shared_root}/evaluation/scores/experiment=bm25-expanded",
                    "parquet",
                ),
                (
                    f"{scratch_root}/2025/bm25/reranked_v2",
                    f"{shared_root}/evaluation/scores/experiment=bm25-reranked",
                    "csv",
                ),
                (
                    f"{scratch_root}/2025/bm25/reranked_expanded",
                    f"{shared_root}/evaluation/scores/experiment=bm25-expanded-reranked",
                    "csv",
                ),
            ]
        ],
        workers=workers,
        local_scheduler=True,
        log_level="INFO",
    )
    if not res:
        raise RuntimeError("BM25 evaluation failed. Check the logs for details.")


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.set_start_method("spawn")
    app()
