"""Convert the ir_datasets LongEval-Web cache into the project's parquet layout.

Downstream (`longeval.collection.ParquetCollection`, the BM25 workflow) reads
`{out}/train/{Documents,Queries,Qrels}` and `{out}/test/{Documents,Queries,Qrels}`,
each Spark-partitioned by `split / language / date`. This writer reproduces that
layout straight from the ir_datasets API (`docs_iter / queries_iter /
qrels_iter`), which is layout-independent and — unlike `Raw2025TestCollection` —
yields **test-date qrels** too (issue #37 needs them).

The partition columns (split, language, date) are encoded only in the directory
names; Spark recovers them via partition discovery, so the parquet files
themselves hold just the payload columns. This matches how Spark's own
`partitionBy` writes behave, so `ParquetCollection` reads identical schemas.

ASSUMPTIONS TO VERIFY against `scripts/inspect-irds-layout.py` output before the
full run (the plan's Stage-2a probe gates this):
  * the namespace registers one dataset id per monthly snapshot;
  * each id embeds its date as `YYYY-MM`;
  * train vs test is inferable from the id (substring) or the date range.
`_split_date_for_id` is the single place to adjust if the probe disagrees.
"""

import os
import re
import shutil
from pathlib import Path

import luigi
import typer

NAMESPACE = "longeval-web"
# LongEval-Web monthly split convention (same as the BM25 worklog): the first
# nine snapshots are train (carry qrels historically), the last six are test.
TRAIN_DATES = {
    "2022-06", "2022-07", "2022-08", "2022-09", "2022-10",
    "2022-11", "2022-12", "2023-01", "2023-02",
}
TEST_DATES = {"2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"}

_DATE_RE = re.compile(r"(\d{4}-\d{2})")


def _iter_dataset_ids(ir_datasets, namespace: str):
    """Best-effort enumeration of registered ids under a namespace prefix."""
    ids = set()
    try:
        for name in ir_datasets.registry:
            ids.add(str(name))
    except TypeError:
        pass
    for attr in ("_registered", "_datasets"):
        reg = getattr(ir_datasets.registry, attr, None)
        if reg:
            try:
                ids.update(str(k) for k in reg.keys())
            except AttributeError:
                pass
    return sorted(i for i in ids if i == namespace or i.startswith(f"{namespace}/"))


def _split_date_for_id(dsid: str):
    """Return (split, date) for a snapshot id, or None to skip (e.g. the
    namespace root or an id with no parseable date)."""
    m = _DATE_RE.search(dsid)
    if not m:
        return None
    date = m.group(1)
    low = dsid.lower()
    if "train" in low:
        split = "train"
    elif "test" in low:
        split = "test"
    elif date in TRAIN_DATES:
        split = "train"
    elif date in TEST_DATES:
        split = "test"
    else:
        return None
    return split, date


def _doc_text(doc) -> str:
    fn = getattr(doc, "default_text", None)
    if callable(fn):
        return fn() or ""
    return getattr(doc, "text", "") or ""


def _query_text(q) -> str:
    fn = getattr(q, "default_text", None)
    if callable(fn):
        return fn() or ""
    return getattr(q, "text", "") or ""


def _write_parquet_stream(rows_iter, columns, out_dir: str, batch_size: int):
    """Stream (dict-per-row) into part-*.parquet files under out_dir. Clears the
    dir first so a rerun after a partial failure can't leave duplicate parts."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    buf = {c: [] for c in columns}
    n = part = 0

    def flush():
        nonlocal buf, part
        if not buf[columns[0]]:
            return
        pq.write_table(pa.table(buf), f"{out_dir}/part-{part:05d}.parquet")
        part += 1
        buf = {c: [] for c in columns}

    for row in rows_iter:
        for c in columns:
            buf[c].append(row[c])
        n += 1
        if n % batch_size == 0:
            flush()
    flush()
    return n


def _read_trec_qrels_file(path: str):
    """Yield qrel rows from a space-separated TREC file (`qid rank docid rel`).

    Used for the LongEval-Web TEST snapshots: their qrels ship in the
    `longeval_web_test_qrels.zip` (defined in the package's downloads.json as
    `longeval_2025_web_test_qrels` but never wired into register()), so the
    ir_datasets API reports has_qrels()==False for them. We read the extracted
    file directly. Same `qrels_processed.txt` format as the train qrels."""
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 4:
                continue
            yield {"qid": parts[0], "rank": int(parts[1]), "docid": parts[2], "rel": int(parts[3])}


class IrdsSnapshotTask(luigi.Task):
    """Write one snapshot's Documents/Queries/Qrels into the parquet tree."""

    dsid = luigi.Parameter()
    date = luigi.Parameter()
    split = luigi.Parameter()
    cache = luigi.Parameter()
    output_path = luigi.Parameter()
    test_qrels_root = luigi.Parameter(default="")
    batch_size = luigi.IntParameter(default=25000)

    resources = {"max_workers": 1}

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_path}/{self.split}/_markers/{self.date}.done"
        )

    def run(self):
        import ir_datasets
        import ir_datasets_longeval

        os.environ["IR_DATASETS_HOME"] = str(self.cache)
        ir_datasets_longeval.register(NAMESPACE)
        ds = ir_datasets.load(self.dsid)

        base = f"{self.output_path}/{self.split}"
        part = f"split={self.split}/language=French/date={self.date}"

        n_docs = _write_parquet_stream(
            ({"docid": d.doc_id, "contents": _doc_text(d)} for d in ds.docs_iter()),
            ["docid", "contents"],
            f"{base}/Documents/{part}",
            self.batch_size,
        )
        n_q = _write_parquet_stream(
            ({"qid": q.query_id, "query": _query_text(q)} for q in ds.queries_iter()),
            ["qid", "query"],
            f"{base}/Queries/{part}",
            self.batch_size,
        )
        # Qrels: train snapshots expose them via the API; test snapshots do not
        # (held out by the package), so read the extracted test-qrels file.
        n_qrels = 0
        qrels_dir = f"{base}/Qrels/{part}"
        if ds.has_qrels():
            n_qrels = _write_parquet_stream(
                (
                    {"qid": r.query_id, "rank": 0, "docid": r.doc_id, "rel": int(r.relevance)}
                    for r in ds.qrels_iter()
                ),
                ["qid", "rank", "docid", "rel"],
                qrels_dir,
                self.batch_size,
            )
        elif self.test_qrels_root:
            qrels_file = Path(self.test_qrels_root).expanduser() / self.date / "qrels_processed.txt"
            if qrels_file.is_file():
                n_qrels = _write_parquet_stream(
                    _read_trec_qrels_file(str(qrels_file)),
                    ["qid", "rank", "docid", "rel"],
                    qrels_dir,
                    self.batch_size,
                )
            else:
                print(f"[warn] no qrels for {self.date}: API empty and {qrels_file} missing", flush=True)

        marker = Path(self.output().path)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(
            f"dsid={self.dsid} split={self.split} date={self.date} "
            f"docs={n_docs} queries={n_q} qrels={n_qrels}\n"
        )


def irds_parquet(
    output_path: str = typer.Argument(
        ..., help="Parquet root; writes {train,test}/{Documents,Queries,Qrels}."
    ),
    cache: str = typer.Option(
        "~/scratch/longeval/ir_datasets",
        help="ir_datasets cache root (IR_DATASETS_HOME).",
    ),
    workers: int = typer.Option(1, help="Luigi workers (snapshots processed in parallel)."),
    batch_size: int = typer.Option(25000, help="Rows per parquet part file."),
    test_qrels_root: str = typer.Option(
        "~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels",
        help="Extracted longeval_web_test_qrels dir (<date>/qrels_processed.txt) for the "
        "test snapshots, which the ir_datasets API does not expose.",
    ),
    only_date: str = typer.Option(
        None, help="If set, process just this YYYY-MM snapshot (smoke test)."
    ),
):
    """LongEval-Web ir_datasets cache -> partitioned parquet."""
    import ir_datasets
    import ir_datasets_longeval

    cache_path = str(Path(cache).expanduser())
    os.environ["IR_DATASETS_HOME"] = cache_path
    ir_datasets_longeval.register(NAMESPACE)

    tasks = []
    for dsid in _iter_dataset_ids(ir_datasets, NAMESPACE):
        sd = _split_date_for_id(dsid)
        if sd is None:
            continue
        split, date = sd
        if only_date and date != only_date:
            continue
        tasks.append(
            IrdsSnapshotTask(
                dsid=dsid,
                date=date,
                split=split,
                cache=cache_path,
                output_path=output_path,
                test_qrels_root=test_qrels_root,
                batch_size=batch_size,
            )
        )

    if not tasks:
        raise typer.BadParameter(
            "No snapshot ids resolved to (split, date). Check "
            "scripts/inspect-irds-layout.py output and _split_date_for_id."
        )
    typer.echo(f"resolved {len(tasks)} snapshot task(s)")
    res = luigi.build(tasks, workers=workers, local_scheduler=True, log_level="INFO")
    if not res:
        raise RuntimeError("irds-parquet ETL failed. Check the logs for details.")
