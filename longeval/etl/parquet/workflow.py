"""Convert raw data to parquet"""

import luigi
from pyspark.sql import Window
from pyspark.sql import functions as F
from longeval.collection import (
    Raw2025Collection,
    Raw2025TestCollection,
    TrecCollection,
)
from longeval.spark import get_spark
import typer
from typing_extensions import Annotated
from longeval.luigi import luigi_kwargs

# All nine TREC-XML train slices (2022-06..2023-02). 2023-02's files are
# misnamed *.jsonl.gz but are actually TREC XML (see TrecCollection), so
# they ingest through this same path — this is the full 9-slice French
# train corpus the working-notes paper reports (~19M raw docs). The
# 2023-03..2023-08 *test* slices are a separate (deferred) ingest.
TREC_TRAIN_DATES = (
    "2022-06,2022-07,2022-08,2022-09,2022-10,2022-11,2022-12,2023-01,"
    "2023-02"
)


class ParquetCollectionTask(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return [
            luigi.LocalTarget(f"{self.output_path}/Documents/_SUCCESS"),
            luigi.LocalTarget(f"{self.output_path}/Queries/_SUCCESS"),
        ] + (
            # only exists in train collections
            [luigi.LocalTarget(f"{self.output_path}/Qrels/_SUCCESS")]
            if "train" in str(self.output_path)
            else []
        )

    def run(self):
        spark = get_spark()
        if "train" in str(self.output_path):
            collection = Raw2025Collection(spark, self.input_path)
        else:
            collection = Raw2025TestCollection(spark, self.input_path)
        collection.to_parquet(self.output_path)


class Workflow(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def run(self):
        yield [
            ParquetCollectionTask(
                input_path=self.input_path,
                output_path=f"{self.output_path}/train",
            ),
            ParquetCollectionTask(
                input_path=self.input_path,
                output_path=f"{self.output_path}/test",
            ),
        ]
        # yield [
        #     TokenTask(
        #         input_path=f"{self.output_path}/train",
        #         output_path=(
        #             Path(self.output_path).parent / "tokens" / "train"
        #         ).as_posix(),
        #     ),
        #     TokenTask(
        #         input_path=f"{self.output_path}/test",
        #         output_path=(
        #             Path(self.output_path).parent / "tokens" / "test"
        #         ).as_posix(),
        #     ),
        # ]


def to_parquet(
    input_path: Annotated[str, typer.Argument(help="Input root directory")],
    output_path: Annotated[str, typer.Argument(help="Output root directory")],
    scheduler_host: Annotated[str | None, typer.Option(help="Scheduler host")] = None,
):
    """Convert raw data to parquet"""
    luigi.build(
        [Workflow(input_path=input_path, output_path=output_path)],
        **luigi_kwargs(scheduler_host),
    )


class TrecParquetTask(luigi.Task):
    """Ingest the TREC-XML train slices into one date-partitioned
    ``Documents`` parquet the LDA pipeline reads via ``ParquetCollection``.

    Reuses ``TrecCollection`` (the existing TREC reader — same parser that
    produced the validated 2022-06 parquet); only file discovery is
    recursive now so the ``collection/``-nested slices come in too. Output
    schema matches the existing single-slice input (``docid, contents``
    plus ``split/language/date`` partitions) so the LDA's ``--date``
    filter becomes a partition prune and a pooled cross-slice train can
    read every date at once.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    dates = luigi.Parameter(default=TREC_TRAIN_DATES)

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/Documents/_SUCCESS")

    def run(self):
        spark = get_spark()
        ds = [d.strip() for d in str(self.dates).split(",") if d.strip()]
        collection = TrecCollection(spark, self.input_path, dates=ds)
        docs = collection.documents.withColumn(
            "language", F.lit("French")
        ).withColumn("split", F.lit("train"))
        # The source has ~10% duplicate docids per slice (overlapping
        # collector_* shards). Keep one row per (date, docid), chosen by
        # the smallest sha2(contents) so the corpus is identical across
        # runs regardless of Spark partitioning — the longitudinal signal
        # (same docid in different months) is preserved since docid is
        # deduped *within* date, not globally.
        keep = Window.partitionBy("date", "docid").orderBy(
            F.sha2(F.col("contents"), 256)
        )
        (
            docs.withColumn("_rn", F.row_number().over(keep))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .write.partitionBy("split", "language", "date")
            .parquet(f"{self.output_path}/Documents", mode="overwrite")
        )


def to_trec_parquet(
    input_path: Annotated[
        str,
        typer.Argument(
            help="Train collection root: the dir above Trec/, e.g. "
            "'.../French/LongEval Train Collection'"
        ),
    ],
    output_path: Annotated[
        str,
        typer.Argument(
            help="Output root; writes <out>/Documents partitioned by "
            "split/language/date"
        ),
    ],
    dates: Annotated[
        str,
        typer.Option(help="Comma-separated YYYY-MM TREC-XML train slices"),
    ] = TREC_TRAIN_DATES,
    scheduler_host: Annotated[str | None, typer.Option(help="Scheduler host")] = None,
):
    """Convert the TREC-XML train slices to one date-partitioned parquet."""
    luigi.build(
        [
            TrecParquetTask(
                input_path=input_path,
                output_path=output_path,
                dates=dates,
            )
        ],
        **luigi_kwargs(scheduler_host),
    )
