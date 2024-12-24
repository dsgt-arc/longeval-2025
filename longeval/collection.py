"""Utilities to load various collections of datasets."""

from pathlib import Path
from pyspark.sql import functions as F


class RawCollection:
    """A class for reading the collection of datasets"""

    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    def _filename_udf(self, path):
        return F.udf(lambda p: Path(p).name)(path)

    @property
    def metadata(self):
        """Get the parts from the path

        test/2023_04/English -> {"language": "English", "date": "2023_04", "split": "test"}
        """
        parts = list(Path(self.path).parts)
        return {
            "language": parts[-1],
            "date": parts[-2],
            "split": parts[-3],
        }

    @property
    def documents(self):
        """Read the document collection."""
        df = self.spark.read.json(
            f"{self.path}/Documents/Json/*", multiLine=True
        ).withColumnRenamed("id", "docid")
        return df

    @property
    def queries(self):
        return self.spark.read.csv(
            f"{self.path}/Queries/*.tsv", sep="\t", schema="qid STRING, query STRING"
        )

    @property
    def qrels(self):
        if not (Path(self.path) / "Qrels").exists():
            return None
        return self.spark.read.csv(
            f"{self.path}/Qrels/*",
            sep=" ",
            schema="qid STRING, rank INT, docid STRING, rel INT",
        )

    def to_parquet(self, path):
        """Write the collection to parquet format."""
        self.documents.write.parquet(f"{path}/Documents", mode="overwrite")
        self.queries.write.parquet(f"{path}/Queries", mode="overwrite")
        if self.qrels:
            self.qrels.write.parquet(f"{path}/Qrels", mode="overwrite")


class ParquetCollection(RawCollection):
    """A class for reading a collection of parquet files."""

    @property
    def documents(self):
        return self.spark.read.parquet(f"{self.path}/Documents")

    @property
    def queries(self):
        return self.spark.read.parquet(f"{self.path}/Queries")

    @property
    def qrels(self):
        if not (Path(self.path) / "Qrels").exists():
            return None
        return self.spark.read.parquet(f"{self.path}/Qrels")
