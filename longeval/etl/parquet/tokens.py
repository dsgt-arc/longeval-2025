"""Count tokens in a Parquet collection"""

import luigi
import tiktoken
from pyspark.sql import functions as F

from longeval.collection import ParquetCollection
from longeval.spark import get_spark


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
        metadata_cols = ["split", "language", "date"]
        df = collection.documents.select(
            *metadata_cols,
            F.col("docid").alias("id"),
            token_udf("contents").alias("tokens"),
            count_udf("contents").alias("words"),
        )
        df.repartition(4).write.parquet(f"{self.output_path}/parquet", mode="overwrite")
        df = spark.read.parquet(f"{self.output_path}/parquet")
        # show some basic statistics
        df.groupBy("split", "language").agg(
            F.count("id").alias("count"),
            F.sum("tokens").alias("sum_tokens"),
            F.avg("tokens").alias("avg_tokens"),
            F.stddev("tokens").alias("stddev_tokens"),
            F.avg("words").alias("avg_words"),
            F.stddev("words").alias("stddev_words"),
        ).show()

        stats = (
            df.groupBy("split", "language", "date")
            .agg(
                F.sum("tokens").alias("sum_tokens"),
                F.sum("words").alias("sum_words"),
            )
            # divided by 1m
            .withColumn("sum_tokens_pm", F.col("sum_tokens") / 1_000_000)
            .withColumn("sum_words_pm", F.col("sum_words") / 1_000_000)
        )
        stats.show()
        stats.toPandas().to_csv(f"{self.output_path}/summary_by_date.csv", index=False)

        stats = (
            df.groupBy("split", "language")
            .agg(
                F.sum("tokens").alias("sum_tokens"),
                F.sum("words").alias("sum_words"),
            )
            # divided by 1m
            .withColumn("sum_tokens_pm", F.col("sum_tokens") / 1_000_000)
            .withColumn("sum_words_pm", F.col("sum_words") / 1_000_000)
        )
        stats.show()
        stats.toPandas().to_csv(f"{self.output_path}/summary.csv", index=False)

        with open(self.output().path, "w") as f:
            f.write("")
