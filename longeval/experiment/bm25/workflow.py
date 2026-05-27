"""Run BM25 experiments with Pyserini/Anserini.

Anserini uses Lucene under the hood and runs well in file-based PACE/SLURM
workflows. This module builds Lucene indices from parquet collections, runs
retrieval, and writes outputs for evaluation and submissions.
"""

import sys

import typer
import luigi
from longeval.spark import spark_resource
from longeval.collection import ParquetCollection
from pyspark.sql import functions as F, Window
from longeval.luigi import BashScriptTask
from textwrap import dedent
from .retrieval import run_search
from .evaluation import score_search
from pathlib import Path

app = typer.Typer()


class OptionMixin:
    input_path: str = luigi.Parameter(description="Path to the input collection file")
    output_path: str = luigi.Parameter(description="Path to the output directory")
    scratch_path: str = luigi.Parameter(
        description="Path to the scratch directory",
        default=Path("~/scratch").expanduser(),
    )
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

    resources = {"max_workers": 1}

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/jsonl/date={self.date}/_SUCCESS")

    def _deduplicate(self, df):
        """Deduplicate the documents based on docid and date.

        This seems like it might be pretty slow, but thankfully we only have to do this once.
        """
        window = Window.partitionBy("docid").orderBy(F.desc(F.length("contents")))
        return (
            df.where(F.length("contents") > 50)
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") == 1)
            .drop("rank")
        )

    def run(self):
        with spark_resource() as spark:
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            test_collection = ParquetCollection(spark, f"{self.input_path}/test")
            # deduplicate and set a minimum length on content document to 10 words (or 50 characters)
            docs = (train_collection.documents.union(test_collection.documents)).where(
                F.col("date") == self.date
            )
            docs = self._deduplicate(docs)
            if self.sample_size > 0.0:
                docs = docs.sample(self.sample_size)
            (
                docs.select(F.col("docid").alias("id"), "contents").write.json(
                    f"{self.output_path}/jsonl/date={self.date}", mode="overwrite"
                )
            )


class BM25IndexTask(BashScriptTask, OptionMixin):
    """Create a BM25 index from the input JSONL file."""

    resources = {"max_workers": 1}

    def requires(self):
        """Define the dependencies for the BM25 index task."""
        return ExportJSONLTask(
            input_path=self.input_path,
            output_path=self.scratch_path,
            date=self.date,
            parallelism=self.parallelism,
            sample_size=self.sample_size,
        )

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/index/date={self.date}/_SUCCESS")

    def script_text(self) -> str:
        out_dir = f"{self.output_path}/index/date={self.date}"
        return dedent(
            f"""
            #!/bin/bash
            set -e
            mkdir -p {out_dir}
            start=$(date +%s)
            {sys.executable} -m pyserini.index.lucene \
                --collection JsonCollection \
                --input {self.scratch_path}/jsonl/date={self.date} \
                --index {out_dir} \
                --generator DefaultLuceneDocumentGenerator \
                --language fr \
                --threads {self.parallelism} \
                --storePositions \
                --storeDocvectors
            end=$(date +%s)
            # Pyserini's main process can exit 0 even when all index threads
            # error out, leaving an index with segments_1 but no term files.
            # Require at least one .tim (term dictionary) before claiming
            # success.
            ls {out_dir}/_*.tim >/dev/null 2>&1 || {{
              echo "ERROR: no term dictionary written under {out_dir} — index is empty." >&2
              exit 1
            }}
            {{
              echo "source=jsonl"
              echo "date={self.date}"
              echo "threads={self.parallelism}"
              echo "wall_seconds=$((end-start))"
              echo "wall_human=$(date -u -d @$((end-start)) +%H:%M:%S)"
            }} > {out_dir}/time.txt
            touch {self.output().path}
            """
        )


class BM25IndexFromTrecTask(BashScriptTask, OptionMixin):
    """Create a BM25 index directly from raw TREC files (no JSONL step).

    Inputs are read from `{trec_input_path}/Trec/{date}_fr/*.trec` as they ship
    in the LongEval Web release; pyserini's TrecCollection handles the
    `<DOC>...<DOCNO>...</DOCNO>...</DOC>` envelopes natively. Output lives at
    `{output_path}/index_trec/date={date}/` so it coexists with the JSONL
    index for direct A/B comparison.
    """

    trec_input_path: str = luigi.Parameter(
        description="Parent of Trec/<date>_fr (e.g., 'LongEval Train Collection' dir).",
    )

    resources = {"max_workers": 1}

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/index_trec/date={self.date}/_SUCCESS"
        )

    def script_text(self) -> str:
        out_dir = f"{self.output_path}/index_trec/date={self.date}"
        trec_dir = f"{self.trec_input_path}/Trec/{self.date}_fr"
        return dedent(
            f"""
            #!/bin/bash
            set -e
            mkdir -p {out_dir}
            start=$(date +%s)
            {sys.executable} -m pyserini.index.lucene \
                --collection TrecCollection \
                --input "{trec_dir}" \
                --index {out_dir} \
                --generator DefaultLuceneDocumentGenerator \
                --language fr \
                --threads {self.parallelism} \
                --storePositions \
                --storeDocvectors
            end=$(date +%s)
            # Pyserini's main process can exit 0 even when all index threads
            # error out, leaving an index with segments_1 but no term files.
            # Require at least one .tim (term dictionary) before claiming
            # success.
            ls {out_dir}/_*.tim >/dev/null 2>&1 || {{
              echo "ERROR: no term dictionary written under {out_dir} — index is empty." >&2
              exit 1
            }}
            {{
              echo "source=trec"
              echo "date={self.date}"
              echo "threads={self.parallelism}"
              echo "wall_seconds=$((end-start))"
              echo "wall_human=$(date -u -d @$((end-start)) +%H:%M:%S)"
            }} > {out_dir}/time.txt
            touch {self.output().path}
            """
        )


class BM25RetrievalPartialTask(luigi.Task, OptionMixin):
    """Query the BM25 index using the specified queries and qrels."""

    num_sample_ids: int = luigi.IntParameter(
        description="Number of smaller jobs to split the collection into.",
    )
    sample_id: int = luigi.IntParameter(
        description="Sample ID to use for the collection. Default is 0.",
    )
    with_expanded_queries: bool = luigi.BoolParameter(
        default=False,
        description="Whether to use expanded queries for the retrieval.",
    )
    output_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the output directory.",
    )
    expanded_path: str = luigi.Parameter(
        default="~/scratch/longeval/query_expansion/expansion",
        description="Path to the expanded queries JSON file.",
    )

    def requires(self):
        """Define the dependencies for the evaluation task."""
        return BM25IndexTask(
            input_path=self.input_path,
            output_path=self.output_path,
            scratch_path=self.scratch_path,
            date=self.date,
            sample_size=self.sample_size,
            parallelism=self.parallelism,
        )

    def output(self):
        path = f"{self.output_path}/{self.output_prefix}/date={self.date}/sample_id={self.sample_id}"
        return {
            "retrieval": luigi.LocalTarget(f"{path}/_SUCCESS"),
        }

    def run(self):
        with spark_resource(app_name=f"longeval-{self.date}-{self.sample_id}") as spark:
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            test_collection = ParquetCollection(spark, f"{self.input_path}/test")
            queries = (train_collection.queries.union(test_collection.queries)).where(
                F.col("date") == self.date
            )
            queries = queries.where(
                F.crc32(F.col("qid")) % self.num_sample_ids == self.sample_id
            )

            if self.with_expanded_queries:
                # expanded query location
                expanded = spark.read.json(
                    Path(self.expanded_path).expanduser().as_posix(), multiLine=True
                ).cache()
                queries = queries.drop("query").join(
                    expanded,
                    on=["qid"],
                    how="left",
                )

            results = run_search(
                queries,
                f"{self.output_path}/index/date={self.date}",
                k=100,
            )
            path = Path(self.output()["retrieval"].path).parent.as_posix()
            results.coalesce(1).write.parquet(path, mode="overwrite")


class BM25RetrievalTask(luigi.Task, OptionMixin):
    """Query the BM25 index using the specified queries and qrels."""

    num_sample_ids: int = luigi.IntParameter(
        default=20,
        description="Number of smaller jobs to split the collection into.",
    )
    with_expanded_queries: bool = luigi.BoolParameter(
        default=False,
        description="Whether to use expanded queries for the retrieval.",
    )
    output_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the output directory.",
    )

    def output(self):
        return [
            luigi.LocalTarget(
                f"{self.output_path}/{self.output_prefix}/date={self.date}/sample_id={i}/_SUCCESS"
            )
            for i in range(self.num_sample_ids)
        ]

    def run(self):
        tasks = []
        for i in range(self.num_sample_ids):
            tasks.append(
                BM25RetrievalPartialTask(
                    input_path=self.input_path,
                    output_path=self.output_path,
                    scratch_path=self.scratch_path,
                    date=self.date,
                    sample_size=self.sample_size,
                    parallelism=self.parallelism,
                    num_sample_ids=self.num_sample_ids,
                    sample_id=i,
                    with_expanded_queries=self.with_expanded_queries,
                    output_prefix=self.output_prefix,
                )
            )
        yield tasks


class BM25SubmissionTask(OptionMixin, luigi.Task):
    output_prefix: str = luigi.Parameter(
        default="bm25_submission",
        description="Prefix for the output directory.",
    )
    retrieval_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the retrieval output directory.",
    )

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/{self.output_prefix}/{self.date}/run.txt.gz"
        )

    def run(self):
        with spark_resource() as spark:
            # load the output of the retrieval task
            results = spark.read.parquet(
                f"{self.output_path}/{self.retrieval_prefix}/date={self.date}"
            )
            # convert to a submission format
            submission = (
                results.select(
                    "qid",
                    F.lit("Q0").alias("Q0"),
                    F.col("docid").alias("docno"),
                    (
                        F.row_number()
                        .over(Window.partitionBy("qid").orderBy(F.desc("score")))
                        .alias("rank")
                    ),
                    "score",
                    F.lit("dsgt_bm25_submission").alias("tag"),
                )
                .orderBy("qid", "rank")
                .toPandas()
            )
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            submission.to_csv(
                self.output().path,
                sep="\t",
                index=False,
                header=False,
                compression="gzip",
            )


class BM25EvaluationTask(luigi.Task, OptionMixin):
    """Evaluate the BM25 index using the specified queries and qrels."""

    output_prefix: str = luigi.Parameter(
        default="evaluation",
        description="Prefix for the output directory.",
    )
    retrieval_prefix: str = luigi.Parameter(
        default="retrieval",
        description="Prefix for the retrieval output directory.",
    )

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/{self.output_prefix}/date={self.date}/_SUCCESS"
        )

    def run(self):
        with spark_resource() as spark:
            # load the output of the retrieval task
            results = spark.read.parquet(
                f"{self.output_path}/{self.retrieval_prefix}/date={self.date}"
            )
            train_collection = ParquetCollection(spark, f"{self.input_path}/train")
            res = score_search(results, train_collection.qrels)
            res.repartition(1).write.parquet(
                Path(self.output().path).parent.as_posix(), mode="overwrite"
            )


@app.command()
def run_bm25(
    input_path: str = typer.Argument(..., help="Path to the input index file"),
    output_path: str = typer.Argument(..., help="Path to the output file"),
    scratch_path: str = typer.Option("~/scratch", help="Path to the scratch directory"),
    should_sample: bool = typer.Option(
        False, help="Whether to sample the collection before indexing."
    ),
    parallelism: int = typer.Option(
        1, help="Number of threads to use for indexing. Default is 1."
    ),
    workers: int = typer.Option(
        1, help="Number of workers to use for the evaluation. Default is 1."
    ),
    num_sample_ids: int = typer.Option(
        -1, help="Number of smaller jobs to split the collection into."
    ),
    sample_id: int = typer.Option(
        -1, help="Sample ID to use for the collection. Default is 0."
    ),
):
    """Run BM25 on the specified index and query file."""
    with spark_resource() as spark:
        # get dates from the documents
        train_dates = sorted(
            [
                row.date
                for row in ParquetCollection(spark, f"{input_path}/train")
                .documents.select("date")
                .distinct()
                .collect()
            ]
        )
        test_dates = sorted(
            [
                row.date
                for row in ParquetCollection(spark, f"{input_path}/test")
                .documents.select("date")
                .distinct()
                .collect()
            ]
        )

    if num_sample_ids > 0 and sample_id >= 0:
        # we batch up the dates so we can run this in parallel
        old_train_dates = train_dates.copy()
        old_test_dates = test_dates.copy()
        train_dates = []
        test_dates = []
        for i, date in enumerate(old_train_dates + old_test_dates):
            if i % num_sample_ids == sample_id:
                if date in old_train_dates:
                    train_dates.append(date)
                else:
                    test_dates.append(date)

    res = luigi.build(
        [
            BM25RetrievalTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                with_expanded_queries=with_expanded_queries,
                output_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            # issue #37: expanded retrieval needs a query-expansion JSON that is
            # not staged on PACE; pin to original queries only. Restore [False, True]
            # to re-enable the expanded arm.
            for with_expanded_queries in [False]
            for date in train_dates + test_dates
        ],
        workers=workers,
        local_scheduler=True,
        log_level="INFO",
    )
    if not res:
        raise RuntimeError("BM25 retrieval failed. Check the logs for details.")

    # now let's run the evaluation on just the train dates
    res = luigi.build(
        [
            BM25EvaluationTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                output_prefix=(
                    "evaluation" if not with_expanded_queries else "evaluation_expanded"
                ),
                retrieval_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            # issue #37: expanded retrieval needs a query-expansion JSON that is
            # not staged on PACE; pin to original queries only. Restore [False, True]
            # to re-enable the expanded arm.
            for with_expanded_queries in [False]
            for date in train_dates
        ]
        + [
            BM25SubmissionTask(
                input_path=input_path,
                output_path=output_path,
                scratch_path=scratch_path,
                date=date,
                sample_size=0.001 if should_sample else 0.0,
                parallelism=parallelism,
                output_prefix=(
                    "bm25_submission"
                    if not with_expanded_queries
                    else "bm25_expanded_submission"
                ),
                retrieval_prefix=(
                    "retrieval" if not with_expanded_queries else "retrieval_expanded"
                ),
            )
            # issue #37: expanded retrieval needs a query-expansion JSON that is
            # not staged on PACE; pin to original queries only. Restore [False, True]
            # to re-enable the expanded arm.
            for with_expanded_queries in [False]
            for date in test_dates
        ],
        workers=workers,
        local_scheduler=True,
        log_level="INFO",
    )
    if not res:
        raise RuntimeError("BM25 evaluation failed. Check the logs for details.")


def _dates_from_trec_root(trec_root: str) -> list[str]:
    """Enumerate snapshot dates from a `<root>/Trec/<YYYY-MM>_fr/` layout."""
    root = Path(trec_root) / "Trec"
    if not root.is_dir():
        raise FileNotFoundError(f"No Trec/ dir under {trec_root}")
    return sorted(p.name[: -len("_fr")] for p in root.glob("*_fr") if p.is_dir())


@app.command(name="index-trec")
def index_trec(
    output_path: str = typer.Argument(..., help="Where index_trec/date=* will be written."),
    train_trec_root: str = typer.Option(
        ...,
        help="Parent of Trec/<date>_fr for the train collection.",
    ),
    test_trec_root: str = typer.Option(
        None,
        help="Parent of Trec/<date>_fr for the test collection (optional).",
    ),
    parallelism: int = typer.Option(1, help="Threads passed to pyserini."),
    workers: int = typer.Option(1, help="Luigi workers."),
):
    """Build a Lucene index per snapshot directly from raw TREC files."""
    tasks = []
    for date in _dates_from_trec_root(train_trec_root):
        tasks.append(
            BM25IndexFromTrecTask(
                input_path=train_trec_root,
                output_path=output_path,
                date=date,
                parallelism=parallelism,
                trec_input_path=train_trec_root,
            )
        )
    if test_trec_root:
        for date in _dates_from_trec_root(test_trec_root):
            tasks.append(
                BM25IndexFromTrecTask(
                    input_path=test_trec_root,
                    output_path=output_path,
                    date=date,
                    parallelism=parallelism,
                    trec_input_path=test_trec_root,
                )
            )
    res = luigi.build(tasks, workers=workers, local_scheduler=True, log_level="INFO")
    if not res:
        raise RuntimeError("TREC-direct indexing failed. Check the logs for details.")


@app.command(name="compare-index-times")
def compare_index_times(
    output_path: str = typer.Argument(..., help="Root that holds index/ and index_trec/."),
):
    """Print a per-date table of wall-clock for both index paths."""

    def _scan(prefix: str) -> dict[str, int]:
        out = {}
        for time_file in Path(output_path).glob(f"{prefix}/date=*/time.txt"):
            date = time_file.parent.name.split("=", 1)[1]
            for line in time_file.read_text().splitlines():
                if line.startswith("wall_seconds="):
                    out[date] = int(line.split("=", 1)[1])
                    break
        return out

    jsonl = _scan("index")
    trec = _scan("index_trec")
    dates = sorted(set(jsonl) | set(trec))
    if not dates:
        typer.echo("No time.txt files found under index/ or index_trec/.")
        raise typer.Exit(code=1)

    typer.echo(f"{'date':<10} {'jsonl_s':>10} {'trec_s':>10} {'speedup':>10}")
    for date in dates:
        j = jsonl.get(date)
        t = trec.get(date)
        speedup = f"{j / t:.2f}x" if j and t else "-"
        typer.echo(
            f"{date:<10} "
            f"{(str(j) if j is not None else '-'):>10} "
            f"{(str(t) if t is not None else '-'):>10} "
            f"{speedup:>10}"
        )


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.set_start_method("spawn")
    app()
