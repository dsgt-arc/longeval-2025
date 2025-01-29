#!/usr/bin/env python
from pathlib import Path
from subprocess import run
import os
import typer
from typing_extensions import Annotated


def main(
    model_name: Annotated[str, typer.Option(help="Model name")] = "all-MiniLM-L6-v2",
    dry_run: Annotated[bool, typer.Option(help="Dry run")] = False,
):
    home = Path(os.getenv("HOME"))
    root = Path(f"{home}/p-dsgt_clef2025-0/longeval/parquet")
    paths = [
        path.relative_to(root)
        for path in Path(root).glob("**/*")
        if path.name == "Documents"
    ]
    for path in paths:
        cmd = [
            "sbatch",
            f"--job-name=longeval-embed-{model_name}-{path}",
            f"{Path(__file__).parent}/slurm-embed.sbatch",
            path.as_posix(),
            model_name,
        ]
        print(" ".join(cmd))
        if not dry_run:
            run(cmd)


if __name__ == "__main__":
    typer.run(main)
