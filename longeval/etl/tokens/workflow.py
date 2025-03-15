"""Convert raw data to parquet"""

from pathlib import Path

import luigi
import tiktoken
import typer
from pyspark.sql import functions as F
from typing_extensions import Annotated

from longeval.collection import ParquetCollection
from longeval.etl.parquet.workflow import Workflow as ParquetWorkflow
from longeval.spark import get_spark
from longeval.settings import SCRATCH_PATH
from longeval.luigi import luigi_kwargs


class TokenTask(luigi.Task):
    """Find how many tokens are part of the collection"""

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    def run(self):
        spark = get_spark()
        encoder = tiktoken.encoding_for_model("gpt-4o-mini")
        token_udf = F.udf(lambda s: len(encoder.encode(s)))
        count_udf = lambda s: F.array_size(F.split(s, " "))  # noqa
        collection = ParquetCollection(spark, self.input_path)
        metadata_cols = [F.lit(v).alias(k) for k, v in collection.metadata.items()]
        df = collection.documents.select(
            *metadata_cols,
            F.lit("documents").alias("collection"),
            F.col("docid").alias("id"),
            token_udf("contents").alias("tokens"),
            count_udf("contents").alias("words"),
        ).union(
            collection.queries.select(
                *metadata_cols,
                F.lit("queries").alias("collection"),
                F.col("qid").alias("id"),
                token_udf("query").alias("tokens"),
                count_udf("query").alias("words"),
            )
        )
        df.repartition(4).write.parquet(self.output_path, mode="overwrite")
        df = spark.read.parquet(self.output_path)
        # show some basic statistics
        df.groupBy("collection").agg(
            F.count("id").alias("count"),
            F.sum("tokens").alias("sum_tokens"),
            F.avg("tokens").alias("avg_tokens"),
            F.stddev("tokens").alias("stddev_tokens"),
            F.avg("words").alias("avg_words"),
            F.stddev("words").alias("stddev_words"),
        ).show()


class SummarizeTokenTask(luigi.Task):
    """Find how many tokens are part of the collection"""

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    def run(self):
        spark = get_spark()
        df = spark.read.parquet(f"{self.input_path}/*/*/*").cache()
        # make sure output path exists
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        stats = (
            df.where(F.col("collection") == "documents")
            .groupBy("split", "language", "collection", "date")
            .agg(
                F.sum("tokens").alias("sum_tokens"),
                F.sum("words").alias("sum_words"),
            )
            # divided by 1m
            .withColumn("sum_tokens_pm", F.col("sum_tokens") / 1_000_000)
            .withColumn("sum_words_pm", F.col("sum_words") / 1_000_000)
        )
        stats.show()
        stats.toPandas().to_csv(f"{self.output_path}/documents.csv", index=False)

        stats = (
            df.where(F.col("collection") == "documents")
            .groupBy("split", "language")
            .agg(
                F.sum("tokens").alias("sum_tokens"),
                F.sum("words").alias("sum_words"),
            )
            # divided by 1m
            .withColumn("sum_tokens_pm", F.col("sum_tokens") / 1_000_000)
            .withColumn("sum_words_pm", F.col("sum_words") / 1_000_000)
        )
        stats.show()
        stats.toPandas().to_csv(f"{self.output_path}/documents_split.csv", index=False)

        with open(self.output().path, "w") as f:
            f.write("")


class Workflow(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def require(self):
        return [
            ParquetWorkflow(
                input_path=self.input_path,
                output_path=self.output_path,
            )
        ]

    def _get_collection_roots(self, root):
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def run(self):
        tasks = []
        parquet_root = Path(self.input_path) / "parquet"
        for collection_root in self._get_collection_roots(parquet_root):
            output_path = (
                Path(self.output_path)
                / "tokens"
                / collection_root.relative_to(parquet_root)
            )
            tasks.append(
                TokenTask(
                    input_path=collection_root.as_posix(),
                    output_path=output_path.as_posix(),
                )
            )
        yield tasks
        yield SummarizeTokenTask(
            input_path=(Path(self.output_path) / "tokens").as_posix(),
            output_path=(Path(self.output_path) / "tokens_stats").as_posix(),
        )


def count_tokens(
    input_path: Annotated[str, typer.Argument(help="Input path")] = SCRATCH_PATH,
    output_path: Annotated[str, typer.Option(help="Output root directory")] = None,
    scheduler_host: Annotated[str, typer.Option(help="Scheduler host")] = None,
):
    """Count the number of tokens in the collection"""
    luigi.build(
        [Workflow(input_path=input_path, output_path=output_path or input_path)],
        **luigi_kwargs(scheduler_host),
    )
