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


class Raw2025Collection(RawCollection):
    """A class for reading the 2025 collection of datasets.

    Note that we actually read in the _entirety_ of the release_2025_p1
    and release_2025_p2 datasets to convert this into a single parquet dataset.
    """

    @property
    def metadata(self):
        """
        We have a bunch of weird rules in here because the 2025 dataset is a bit strange.
        We'll throw here and implement later if we need to, since we're changing up the
        semantics of how the collection works. This means that it's a TODO to clean up
        the work in this repo to get rid of all the references to the 2024 collection.
        """
        raise NotImplementedError(
            "The metadata property is not implemented for Raw2025Collection. "
            "This collection does not follow the standard path structure."
        )

    def _filename_date_document_udf(self, path):
        def _parse(p):
            parts = Path(p).parts
            # look for a part that looks like `YYYY-MM_fr` or `YYYY-MM_en` in the path
            # easier just to look at the parts and pop according to the conventon
            parts = [part for part in parts if "collection" not in part]
            return parts[-2].split("_")[0]

        return F.udf(_parse)(path)

    def _filename_date_udf(self, path):
        def _parse(p):
            parts = Path(p).parts
            # look for a part that looks like `YYYY-MM_fr` or `YYYY-MM_en` in the path
            # easier just to look at the parts and pop according to the conventon
            return parts[-1].split("_")[0]

        return F.udf(_parse)(path)

    @property
    def documents(self):
        """
        Read the document collection from the 2025 datasets. We add in the
        metadata directly into the dataset.
        """
        return (
            self.spark.read.json(
                f"{self.path}/release_2025_p2/French/*/Json/*/*", multiLine=True
            )
            .withColumnRenamed("id", "docid")
            .withColumn("language", F.lit("French"))
            # date is the 2nd to last part of the path e.g. 2022-06_fr
            .withColumn(
                "date",
                self._filename_date_document_udf(F.input_file_name()),
            )
            .withColumn("split", F.lit("train"))
        )

    @property
    def queries(self):
        """
        Read the queries from the 2025 datasets. Note that we will read from the
        release_2025_p2 for queries.

        # NOTE: you must download the updated queries from the website and copy them
        # into release_p2 for this to work.
        """
        return (
            self.spark.read.csv(
                f"{self.path}/release_2025_p2/French/*/queries/*",
                sep="\t",
                schema="qid STRING, query STRING",
            )
            .withColumn("date", self._filename_date_udf(F.input_file_name()))
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit("train"))
        )

    @property
    def qrels(self):
        """
        Read the qrels from the 2025 datasets. Note that we will read from the
        release_2025_p1 for qrels.
        """
        return (
            self.spark.read.csv(
                f"{self.path}/release_2025_p2/French/*/qrels/*",
                sep=" ",
                schema="qid STRING, rank INT, docid STRING, rel INT",
            )
            .withColumnRenamed("id", "docid")
            # date is the 2nd to last part of the path e.g. 2022-06_fr
            .withColumn("date", self._filename_date_udf(F.input_file_name()))
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit("train"))
        )

    def to_parquet(self, path):
        """Write the collection to parquet format."""
        # partition by language and date
        self.documents.write.partitionBy("split", "language", "date").parquet(
            f"{path}/Documents", mode="overwrite"
        )
        self.queries.write.partitionBy("split", "language", "date").parquet(
            f"{path}/Queries", mode="overwrite"
        )
        self.qrels.write.partitionBy("split", "language", "date").parquet(
            f"{path}/Qrels", mode="overwrite"
        )


class Raw2025TestCollection(Raw2025Collection):
    """A class for reading the 2025 test collection of datasets."""

    @property
    def documents(self):
        return (
            self.spark.read.json(f"{self.path}/*Test*/Json/*/*", multiLine=True)
            .withColumnRenamed("id", "docid")
            # date is the 2nd to last part of the path e.g. 2022-06_fr
            .withColumn("date", self._filename_data_udf(F.input_file_name()))
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit("test"))
        )

    @property
    def queries(self):
        return (
            self.spark.read.csv(
                f"{self.path}/*Test*/queries/*",
                sep="\t",
                schema="qid STRING, query STRING",
            )
            .withColumn("date", self._filename_date_udf(F.input_file_name()))
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit("test"))
        )

    @property
    def qrels(self):
        raise NotImplementedError(
            "The qrels property is not implemented for Raw2025TestCollection. "
            "This collection does not have qrels."
        )

    def to_parquet(self, path):
        """Write the collection to parquet format."""
        # partition by language and date
        self.documents.write.partitionBy("split", "language", "date").parquet(
            f"{path}/Documents", mode="overwrite"
        )
        self.queries.write.partitionBy("split", "language", "date").parquet(
            f"{path}/Queries", mode="overwrite"
        )


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
