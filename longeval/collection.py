"""Utilities to load various collections of datasets."""

from pathlib import Path
from pyspark.sql import functions as F


class Collection:
    """A class for reading the collection of datasets"""

    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    def _filename_udf(self, path):
        return F.udf(lambda p: Path(p).name)(path)

    def _extract_attr_udf(self, attr, col="_corrupt_record", use_udf=False):
        return F.when(F.expr(f"{col} is null"), F.col(attr)).otherwise(
            F.udf(
                lambda s: (s or f"<{attr}>")
                .split(f"<{attr}>")[1]
                .split(f"</{attr}>")[0]
            )(col)
            if use_udf
            else F.regexp_extract(F.col(col), f"<{attr}>(.*?)</{attr}>", 1)
        )

    @property
    def documents(self):
        """Read the document collection."""
        df = self.spark.read.json(
            f"{self.path}/Documents/Json/*", multiLine=True
        ).withColumnRenamed("id", "docid")
        return df

    @property
    def qrels(self):
        return self.spark.read.csv(
            f"{self.path}/Qrels/*",
            sep=" ",
            schema="qid STRING, rank INT, docid STRING, rel INT",
        )

    @property
    def queries(self):
        return self.spark.read.csv(
            f"{self.path}/Queries/*.tsv", sep="\t", schema="qid STRING, query STRING"
        )
