"""We use pyserini to run BM25 experiments.

The one downside of opensearch/elasticsearch is that we can't
run the experiments on PACE, which limits a lot of the interesting
experimentation we can do. Anserini uses Lucene under the hood,
which allows us to get some of the same performance as we can get
in an optimized system like OpenSearch.

Here, we write a bit of code in order to get the appropriate indices
and to get some basic functionality working.
"""

import typer
import luigi
from longeval.spark import spark_resource
from longeval.collection import ParquetCollection
from pyspark.sql import functions as F, Window
from longeval.luigi import BashScriptTask
from textwrap import dedent
from .evaluation import run_search, score_search
from pathlib import Path

app = typer.Typer()


class OptionMixin:
    input_path: str = luigi.Parameter(description="Path to the input collection file")
    output_path: str = luigi.Parameter(description="Path to the output directory")
    scratch_path: str = luigi.Parameter(
        description="Path to the scratch directory",
        default=Path("~/scratch").expanduser(),
    )
    date: str = luigi.Parameter(description="Date to use for the collection")
    sample_size: float = luigi.FloatParameter(
        default=0.0,
        description="Sample size to use for the collection. If 0, use the full collection.",
    )
    parallelism: int = luigi.IntParameter(
        default=1,
        description="Number of threads to use for indexing. Default is 1.",
    )


class ExportJSONLTask(luigi.Task, OptionMixin):
    """Generate a JSONL file from the input collection to be indexed.

    https://github.com/castorini/pyserini/blob/master/docs/usage-index.md#building-a-bm25-index-direct-java-implementation
    """

    resources = {"max_workers": 1}

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/jsonl/date={self.date}/_SUCCESS")

    def _deduplicate(self, df):
        """Deduplicate the documents based on docid and date.

        This seems like it might be pretty slow, but thankfully we only have to do this once.
        """
        window = Window.partitionBy("docid").orderBy(F.desc(F.length("contents")))
        return (
            df.where(F.length("contents") > 50)
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") == 1)
            .drop("rank")
        )

    def run(self):
        with spark_resource() as spark:
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            test_collection = ParquetCollection(spark, f"{self.input_path}/test")
            # deduplicate and set a minimum length on content document to 10 words (or 50 characters)
            docs = (train_collection.documents.union(test_collection.documents)).where(
                F.col("date") == self.date
            )
            docs = self._deduplicate(docs)
            if self.sample_size > 0.0:
                docs = docs.sample(self.sample_size)
            (
                docs.select(F.col("docid").alias("id"), "contents").write.json(
                    f"{self.output_path}/jsonl/date={self.date}", mode="overwrite"
                )
            )


class BM25IndexTask(BashScriptTask, OptionMixin):
    """Create a BM25 index from the input JSONL file."""

    resources = {"max_workers": 1}

    def requires(self):
        """Define the dependencies for the BM25 index task."""
        return ExportJSONLTask(
            input_path=self.input_path,
            output_path=self.scratch_path,
            date=self.date,
            parallelism=self.parallelism,
            sample_size=self.sample_size,
        )

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/index/date={self.date}/_SUCCESS")

    def script_text(self) -> str:
        return dedent(
            f"""
            #!/bin/bash
            mkdir -p {self.output_path}/index/date={self.date}
            python -m pyserini.index.lucene \
                --collection JsonCollection \
                --input {self.scratch_path}/jsonl/date={self.date} \
                --index {self.output_path}/index/date={self.date} \
                --generator DefaultLuceneDocumentGenerator \
                --language fr \
                --threads {self.parallelism} \
                --storePositions \
                --storeDocvectors
            touch {self.output().path}
            """
        )


class BM25RetrievalPartialTask(luigi.Task, OptionMixin):
    """Query the BM25 index using the specified queries and qrels."""

    num_sample_ids: int = luigi.IntParameter(
        description="Number of smaller jobs to split the collection into.",
    )
    sample_id: int = luigi.IntParameter(
        description="Sample ID to use for the collection. Default is 0.",
    )
    with_expanded_queries: bool = luigi.BoolParameter(
        default=False,
        description="Whether to use expanded queries for the retrieval.",
    )
    output_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the output directory.",
    )
    expanded_path: str = luigi.Parameter(
        default="~/scratch/longeval/query_expansion/expansion",
        description="Path to the expanded queries JSON file.",
    )

    def requires(self):
        """Define the dependencies for the evaluation task."""
        return BM25IndexTask(
            input_path=self.input_path,
            output_path=self.output_path,
            scratch_path=self.scratch_path,
            date=self.date,
            sample_size=self.sample_size,
            parallelism=self.parallelism,
        )

    def output(self):
        path = f"{self.output_path}/{self.output_prefix}/date={self.date}/sample_id={self.sample_id}"
        return {
            "retrieval": luigi.LocalTarget(f"{path}/_SUCCESS"),
        }

    def run(self):
        with spark_resource(app_name=f"longeval-{self.date}-{self.sample_id}") as spark:
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            test_collection = ParquetCollection(spark, f"{self.input_path}/test")
            queries = (train_collection.queries.union(test_collection.queries)).where(
                F.col("date") == self.date
            )
            queries = queries.where(
                F.crc32(F.col("qid")) % self.num_sample_ids == self.sample_id
            )

            if self.with_expanded_queries:
                # expanded query location
                expanded = spark.read.json(
                    Path(self.expanded_path).expanduser().as_posix(), multiLine=True
                ).cache()
                queries = queries.drop("query").join(
                    expanded,
                    on=["qid"],
                    how="left",
                )

            results = run_search(
                queries,
                f"{self.output_path}/index/date={self.date}",
                k=100,
            )
            path = Path(self.output()["retrieval"].path).parent.as_posix()
            results.coalesce(1).write.parquet(path, mode="overwrite")


class BM25RetrievalTask(luigi.Task, OptionMixin):
    """Query the BM25 index using the specified queries and qrels."""

    num_sample_ids: int = luigi.IntParameter(
        default=20,
        description="Number of smaller jobs to split the collection into.",
    )
    with_expanded_queries: bool = luigi.BoolParameter(
        default=False,
        description="Whether to use expanded queries for the retrieval.",
    )
    output_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the output directory.",
    )

    def output(self):
        return [
            luigi.LocalTarget(
                f"{self.output_path}/{self.output_prefix}/date={self.date}/sample_id={i}/_SUCCESS"
            )
            for i in range(self.num_sample_ids)
        ]

    def run(self):
        tasks = []
        for i in range(self.num_sample_ids):
            tasks.append(
                BM25RetrievalPartialTask(
                    input_path=self.input_path,
                    output_path=self.output_path,
                    scratch_path=self.scratch_path,
                    date=self.date,
                    sample_size=self.sample_size,
                    parallelism=self.parallelism,
                    num_sample_ids=self.num_sample_ids,
                    sample_id=i,
                    with_expanded_queries=self.with_expanded_queries,
                    output_prefix=self.output_prefix,
                )
            )
        yield tasks


class BM25SubmissionTask(OptionMixin, luigi.Task):
    output_prefix: str = luigi.Parameter(
        default="bm25_submission",
        description="Prefix for the output directory.",
    )
    retrieval_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the retrieval output directory.",
    )

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/{self.output_prefix}/{self.date}/run.txt.gz"
        )

    def run(self):
        with spark_resource() as spark:
            # load the output of the retrieval task
            results = spark.read.parquet(
                f"{self.output_path}/{self.retrieval_prefix}/date={self.date}"
            )
            # convert to a submission format
            submission = (
                results.select(
                    "qid",
                    F.lit("Q0").alias("Q0"),
                    F.col("docid").alias("docno"),
                    (
                        F.row_number()
                        .over(Window.partitionBy("qid").orderBy(F.desc("score")))
                        .alias("rank")
                    ),
                    "score",
                    F.lit("dsgt_bm25_submission").alias("tag"),
                )
                .orderBy("qid", "rank")
                .toPandas()
            )
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            submission.to_csv(
                self.output().path,
                sep="\t",
                index=False,
                header=False,
                compression="gzip",
            )


class BM25EvaluationTask(luigi.Task, OptionMixin):
    """Evaluate the BM25 index using the specified queries and qrels."""

    output_prefix: str = luigi.Parameter(
        default="evaluation",
        description="Prefix for the output directory.",
    )
    retrieval_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the retrieval output directory.",
    )

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/{self.output_prefix}/date={self.date}/_SUCCESS"
        )

    def run(self):
        with spark_resource() as spark:
            # load the output of the retrieval task
            results = spark.read.parquet(
                f"{self.output_path}/{self.retrieval_prefix}/date={self.date}"
            )
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            res = score_search(results, train_collection.qrels)
            res.repartition(1).write.parquet(
                Path(self.output().path).parent.as_posix(), mode="overwrite"
            )


@app.command()
def run_bm25(
    input_path: str = typer.Argument(..., help="Path to the input index file"),
    output_path: str = typer.Argument(..., help="Path to the output file"),
    scratch_path: str = typer.Option("~/scratch", help="Path to the scratch directory"),
    should_sample: bool = typer.Option(
        False, help="Whether to sample the collection before indexing."
    ),
    parallelism: int = typer.Option(
        1, help="Number of threads to use for indexing. Default is 1."
    ),
    workers: int = typer.Option(
        1, help="Number of workers to use for the evaluation. Default is 1."
    ),
    num_sample_ids: int = typer.Option(
        -1, help="Number of smaller jobs to split the collection into."
    ),
    sample_id: int = typer.Option(
        -1, help="Sample ID to use for the collection. Default is 0."
    ),
):
    """Run BM25 on the specified index and query file."""
    with spark_resource() as spark:
        # get dates from the documents
        train_dates = sorted(
            [
                row.date
                for row in ParquetCollection(spark, f"{input_path}/train")
                .documents.select("date")
                .distinct()
                .collect()
            ]
        )
        test_dates = sorted(
            [
                row.date
                for row in ParquetCollection(spark, f"{input_path}/test")
                .documents.select("date")
                .distinct()
                .collect()
            ]
        )

    if num_sample_ids > 0 and sample_id >= 0:
        # we batch up the dates so we can run this in parallel
        old_train_dates = train_dates.copy()
        old_test_dates = test_dates.copy()
        train_dates = []
        test_dates = []
        for i, date in enumerate(old_train_dates + old_test_dates):
            if i % num_sample_ids == sample_id:
                if date in old_train_dates:
                    train_dates.append(date)
                else:
                    test_dates.append(date)

    res = luigi.build(
        [
            BM25RetrievalTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                with_expanded_queries=with_expanded_queries,
                output_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            for with_expanded_queries in [False, True]
            for date in train_dates + test_dates
        ],
        workers=workers,
        local_scheduler=True,
        log_level="INFO",
    )
    if not res:
        raise RuntimeError("BM25 retrieval failed. Check the logs for details.")

    # now let's run the evaluation on just the train dates
    res = luigi.build(
        [
            BM25EvaluationTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                output_prefix=(
                    "evaluation" if not with_expanded_queries else "evaluation_expanded"
                ),
                retrieval_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            for with_expanded_queries in [False, True]
            for date in train_dates
        ]
        + [
            BM25SubmissionTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                output_prefix=(
                    "bm25_submission"
                    if not with_expanded_queries
                    else "bm25_expanded_submission"
                ),
                retrieval_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            for with_expanded_queries in [False, True]
            for date in test_dates
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
