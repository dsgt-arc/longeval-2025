"""Convert raw data to parquet"""

from pathlib import Path

import luigi
import tiktoken
import typer
from pyspark.sql import functions as F
from typing_extensions import Annotated

from longeval.collection import ParquetCollection
from longeval.etl.parquet.workflow import Workflow as ParquetWorkflow
from longeval.spark import spark_resource


class TokenTask(luigi.Task):
    """Find how many tokens are part of the collection"""

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    def run(self):
        with spark_resource() as spark:
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


class Workflow(luigi.Task):
    root = luigi.Parameter(default="/mnt/data/longeval")

    def dependencies(self):
        return [ParquetWorkflow(root=self.root)]

    def _get_collection_roots(self, root):
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def run(self):
        tasks = []
        parquet_root = Path(self.root) / "parquet"
        for collection_root in self._get_collection_roots(parquet_root):
            output_path = (
                Path(self.root) / "tokens" / collection_root.relative_to(parquet_root)
            )
            tasks.append(
                TokenTask(
                    input_path=collection_root,
                    output_path=output_path,
                )
            )
        yield tasks


def main(scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None):
    """Count the number of tokens in the collection"""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build([Workflow()], **kwargs)
