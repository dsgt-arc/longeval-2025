"""Utilities to load various collections of datasets."""

from pathlib import Path
from pyspark.sql import functions as F
from unidecode import unidecode


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
        # unfortunately this doesn't work well because we blow up memory
        # df = self.spark.read.json(f"{self.path}/Documents/Json/*")

        # and unfortunately this does work well because special characters cause issues
        df = (
            self.spark.read.format("com.databricks.spark.xml")
            .option("rowTag", "DOC")
            # .option("mode", "FAILFAST")
            .option("inferSchema", "true")
            .load(f"{self.path}/Documents/Trec/*1.txt")
            # special characters cause issues, so we can manually extract other data
            .withColumn(
                "_corrupt_record",
                F.when(
                    F.expr("_corrupt_record is null"),
                    F.lit(None),
                ).otherwise(F.udf(lambda s: unidecode(s or ""))("_corrupt_record")),
            )
            .withColumn("DOCID", self._extract_attr_udf("DOCID"))
            .withColumn("DOCNO", self._extract_attr_udf("DOCNO"))
            .withColumn("TEXT", self._extract_attr_udf("TEXT", use_udf=True))
            # .drop("_corrupt_record")
        )
        return df

    @property
    def qrels(self):
        return self.spark.read.csv(f"{self.path}/Qrels/*", sep=" ")

    @property
    def queries(self):
        return self.spark.read.csv(f"{self.path}/Queries/*.tsv", sep="\t")
