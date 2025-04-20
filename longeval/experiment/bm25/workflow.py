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
from pyspark.sql import functions as F
from longeval.luigi import BashScriptTask
from textwrap import dedent
from .evaluation import prepare_queries, run_search, score_search

app = typer.Typer()


class OptionMixin:
    input_path: str = luigi.Parameter(description="Path to the input collection file")
    output_path: str = luigi.Parameter(description="Path to the output directory")
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

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/jsonl/date={self.date}/_SUCCESS")

    def run(self):
        with spark_resource() as spark:
            docs = ParquetCollection(spark, self.input_path).documents

            if self.sample_size > 0.0:
                docs = docs.sample(self.sample_size)
            (
                docs.select("date", F.col("docid").alias("id"), "contents")
                .where(F.col("date") == self.date)
                .drop("date")
                .write.json(
                    f"{self.output_path}/jsonl/date={self.date}", mode="overwrite"
                )
            )


class BM25IndexTask(BashScriptTask, OptionMixin):
    """Create a BM25 index from the input JSONL file."""

    def requires(self):
        """Define the dependencies for the BM25 index task."""
        return ExportJSONLTask(
            input_path=self.input_path,
            output_path=self.output_path,
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
                --input {self.output_path}/jsonl/date={self.date} \
                --index {self.output_path}/index/date={self.date} \
                --generator DefaultLuceneDocumentGenerator \
                --language fr \
                --stemmer none \
                --threads {self.parallelism} \
                --storePositions \
                --storeDocvectors
            touch {self.output().path}
            """
        )


class EvaluateBM25Task(luigi.Task, OptionMixin):
    """Evaluate the BM25 index using the specified queries and qrels."""

    def requires(self):
        """Define the dependencies for the evaluation task."""
        return BM25IndexTask(
            input_path=self.input_path,
            output_path=self.output_path,
            date=self.date,
            sample_size=self.sample_size,
            parallelism=self.parallelism,
        )

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/evaluation/date={self.date}/_SUCCESS"
        )

    def run(self):
        with spark_resource() as spark:
            collection = ParquetCollection(spark, self.input_path)
            queries = prepare_queries(collection).cache()
            index_path = f"{self.output_path}/index/date={self.date}"
            subset = queries.filter(F.col("date") == self.date)
            results = run_search(subset, index_path)
            res = score_search(results)
            res.repartition(1).write.parquet(
                f"{self.output_path}/evaluation/date={self.date}", mode="overwrite"
            )


@app.command()
def run_bm25(
    input_path: str = typer.Argument(..., help="Path to the input index file"),
    output_path: str = typer.Argument(..., help="Path to the output file"),
    should_sample: bool = typer.Option(
        False, help="Whether to sample the collection before indexing."
    ),
    parallelism: int = typer.Option(
        1, help="Number of threads to use for indexing. Default is 1."
    ),
    workers: int = typer.Option(
        1, help="Number of workers to use for the evaluation. Default is 1."
    ),
):
    """Run BM25 on the specified index and query file."""
    with spark_resource() as spark:
        collection = ParquetCollection(spark, input_path)
        dates = [
            row.date for row in collection.qrels.select("date").distinct().collect()
        ]

    luigi.build(
        [
            EvaluateBM25Task(
                input_path=input_path,
                output_path=output_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
            )
            for date in dates
        ],
        workers=workers,
        local_scheduler=True,
        log_level="INFO",
    )


if __name__ == "__main__":
    app()
