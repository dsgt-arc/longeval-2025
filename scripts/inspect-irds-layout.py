#!/usr/bin/env python
"""Probe the ir_datasets_longeval layout + API before committing the ETL.

Read-only. Run on PACE after the download sbatch so the cache is populated. It
answers the two questions Stage 2 of the issue-37 plan depends on:

  1. What dataset ids does `longeval-web` register, and what is the id -> date
     (YYYY-MM) convention? Which are train vs test?
  2. Does the ir_datasets API expose docs/queries/qrels per id -- and crucially
     do the TEST ids carry qrels via `qrels_iter()`? (If yes, the API-based ETL
     in `longeval/etl/irds/workflow.py` is viable; if not, fall back to the
     symlink + Raw2025TestCollection.qrels path.)

It also walks the extracted tree so we can see the on-disk shape.

Usage:
    scripts/inspect-irds-layout.py                 # default cache ~/scratch/longeval/ir_datasets
    scripts/inspect-irds-layout.py --cache /path   # explicit cache root
"""

import os
from pathlib import Path

import typer

app = typer.Typer()


def _iter_dataset_ids(ir_datasets, namespace: str):
    """Best-effort enumeration of registered ids under a namespace prefix.

    ir_datasets' registry surface has shifted across versions; try the public
    iterator, then a couple of known internal attributes, and dedupe.
    """
    ids = set()
    try:
        for name in ir_datasets.registry:  # iterable in recent versions
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


def _first(it, n=1):
    out = []
    for i, rec in enumerate(it):
        if i >= n:
            break
        out.append(rec)
    return out


def _describe(ir_datasets, dsid: str):
    typer.echo(f"\n--- {dsid} ---")
    try:
        ds = ir_datasets.load(dsid)
    except Exception as e:  # noqa: BLE001
        typer.echo(f"  load failed: {type(e).__name__}: {e}")
        return
    for kind, has, it in (
        ("docs", ds.has_docs(), ds.docs_iter if ds.has_docs() else None),
        ("queries", ds.has_queries(), ds.queries_iter if ds.has_queries() else None),
        ("qrels", ds.has_qrels(), ds.qrels_iter if ds.has_qrels() else None),
    ):
        if not has:
            typer.echo(f"  {kind:8} has=False")
            continue
        try:
            sample = _first(it())
            typer.echo(f"  {kind:8} has=True  first={sample[0] if sample else '(empty)'}")
        except Exception as e:  # noqa: BLE001
            typer.echo(f"  {kind:8} has=True  iter failed: {type(e).__name__}: {e}")


@app.command()
def main(
    cache: Path = typer.Option(
        Path("~/scratch/longeval/ir_datasets").expanduser(),
        "--cache",
        "-c",
        help="ir_datasets cache root (sets IR_DATASETS_HOME).",
    ),
    namespace: str = typer.Option("longeval-web", "--namespace", "-n"),
    walk_depth: int = typer.Option(4, help="Max depth for the on-disk tree walk."),
):
    cache = cache.expanduser().resolve()
    os.environ["IR_DATASETS_HOME"] = str(cache)
    typer.echo(f"IR_DATASETS_HOME={cache}")

    import ir_datasets
    import ir_datasets_longeval

    ir_datasets_longeval.register(namespace)

    ids = _iter_dataset_ids(ir_datasets, namespace)
    typer.echo(f"\n==== {len(ids)} registered ids under '{namespace}' ====")
    for dsid in ids:
        typer.echo(f"  {dsid}")

    # Heuristic split: ids mentioning 'train' vs 'test'; otherwise just sample
    # the first and last so we still exercise both ends of the snapshot range.
    train_like = [i for i in ids if "train" in i.lower()]
    test_like = [i for i in ids if "test" in i.lower()]
    sample_ids = []
    if train_like:
        sample_ids.append(train_like[0])
    if test_like:
        sample_ids.append(test_like[0])
    if not sample_ids and ids:
        sample_ids = [ids[0], ids[-1]]

    typer.echo("\n==== per-id docs/queries/qrels probe ====")
    for dsid in sample_ids:
        _describe(ir_datasets, dsid)

    typer.echo(f"\n==== on-disk tree under {cache / namespace} (depth<={walk_depth}) ====")
    root = cache / namespace
    if root.is_dir():
        base_parts = len(root.parts)
        for dirpath, dirnames, filenames in os.walk(root):
            depth = len(Path(dirpath).parts) - base_parts
            if depth > walk_depth:
                dirnames[:] = []
                continue
            indent = "  " * depth
            typer.echo(f"{indent}{Path(dirpath).name}/")
            for f in sorted(filenames)[:5]:
                typer.echo(f"{indent}  {f}")
            if len(filenames) > 5:
                typer.echo(f"{indent}  ... (+{len(filenames) - 5} files)")
    else:
        typer.echo(f"  (no directory at {root} — run the download sbatch first)")


if __name__ == "__main__":
    app()
