"""Convert raw data to parquet"""

from pathlib import Path

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

# LongEval-Web 2025 monthly snapshots (the ir_datasets longeval-web download).
WEB2025_TRAIN_DATES = [
    "2022-06", "2022-07", "2022-08", "2022-09", "2022-10",
    "2022-11", "2022-12", "2023-01", "2023-02",
]
WEB2025_TEST_DATES = ["2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"]

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


def _dedup_docs(docs):
    """One row per (date, docid), chosen by smallest sha2(contents) so the
    corpus is identical across runs regardless of Spark partitioning. Same rule
    as TrecParquetTask."""
    keep = Window.partitionBy("date", "docid").orderBy(F.sha2(F.col("contents"), 256))
    return docs.withColumn("_rn", F.row_number().over(keep)).filter(F.col("_rn") == 1).drop("_rn")


def to_web2025_parquet(
    train_root: Annotated[
        str,
        typer.Argument(help="'.../release_2025_p1/release_2025_p1/French/LongEval Train Collection'"),
    ],
    test_root: Annotated[
        str,
        typer.Argument(help="'.../LongEval Test Collection/LongEval Test Collection'"),
    ],
    output_path: Annotated[str, typer.Argument(help="Parquet root; writes {train,test}/{Documents,Queries,Qrels}")],
    test_qrels_root: Annotated[
        str,
        typer.Option(
            help="Extracted longeval_web_test_qrels dir (<date>/qrels_processed.txt); the "
            "test snapshots' qrels are not in the collection itself."
        ),
    ] = "~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels",
    relink_root: Annotated[
        str,
        typer.Option(help="Scratch dir for TrecCollection's .trec hardlink mirror (same FS as source)."),
    ] = "~/scratch/longeval/longeval-trec-relink",
    only_date: Annotated[
        str, typer.Option(help="If set, process just this YYYY-MM snapshot (for SLURM array fan-out).")
    ] = None,
):
    """LongEval-Web 2025 raw collections -> partitioned parquet.

    Reuses ``TrecCollection`` (the validated TREC-XML reader) for documents on
    both splits, plus direct Spark reads for queries and qrels. Train qrels come
    from the collection's own ``qrels/<date>_fr/``; test qrels come from the
    separately-fetched ``longeval_web_test_qrels`` zip (the API does not expose
    them). Output matches ``ParquetCollection`` so the BM25 workflow and
    ``rerank-eval.py`` read it unchanged.

    Writes all dates in one Spark job by default. Pass ``--only-date`` (e.g. from
    a SLURM array) to process a single snapshot; dynamic partition overwrite
    keeps that safe (only the touched ``date=`` partition is rewritten).
    """
    spark = get_spark()
    # Only overwrite the partitions we actually write (array-per-date safe).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # TrecCollection hardlinks misnamed (*.jsonl.gz) test files to *.trec names;
    # the mirror must sit on the same filesystem as the source (scratch), not the
    # class default (/mnt/data, a GCP-box path that doesn't exist on PACE).
    TrecCollection._RELINK_ROOT = str(Path(relink_root).expanduser())

    test_qrels_root = str(Path(test_qrels_root).expanduser())
    splits = [("train", str(Path(train_root).expanduser())), ("test", str(Path(test_root).expanduser()))]

    for split, root in splits:
        all_dates = WEB2025_TRAIN_DATES if split == "train" else WEB2025_TEST_DATES
        dates = [only_date] if only_date else all_dates
        if only_date and only_date not in all_dates:
            continue  # this split doesn't own that date

        # Documents — reuse the TrecCollection parser.
        docs = (
            TrecCollection(spark, root, dates=dates).documents
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit(split))
        )
        _dedup_docs(docs).write.partitionBy("split", "language", "date").parquet(
            f"{output_path}/{split}/Documents", mode="overwrite"
        )

        # Queries — TSV per snapshot, date from the filename.
        queries = (
            spark.read.csv(
                f"{root}/queries/*_queries.txt", sep="\t", schema="qid STRING, query STRING"
            )
            .withColumn("date", F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2})_queries\.txt$", 1))
            .withColumn("language", F.lit("French"))
            .withColumn("split", F.lit(split))
        )
        if only_date:
            queries = queries.where(F.col("date") == only_date)
        queries.write.partitionBy("split", "language", "date").parquet(
            f"{output_path}/{split}/Queries", mode="overwrite"
        )

        # Qrels — train from the collection, test from the fetched zip dir.
        if split == "train":
            qrels = spark.read.csv(
                f"{root}/qrels/*/qrels_processed.txt",
                sep=" ",
                schema="qid STRING, rank INT, docid STRING, rel INT",
            ).withColumn("date", F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2})_fr/", 1))
        else:
            qrels = spark.read.csv(
                f"{test_qrels_root}/*/qrels_processed.txt",
                sep=" ",
                schema="qid STRING, rank INT, docid STRING, rel INT",
            ).withColumn("date", F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2})/", 1))
        qrels = qrels.withColumn("language", F.lit("French")).withColumn("split", F.lit(split))
        if only_date:
            qrels = qrels.where(F.col("date") == only_date)
        qrels.write.partitionBy("split", "language", "date").parquet(
            f"{output_path}/{split}/Qrels", mode="overwrite"
        )
