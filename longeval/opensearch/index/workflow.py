"""OpenSearch indexing workflow for Longeval.

This module defines Luigi tasks and workflows for loading document collections
into OpenSearch and configuring appropriate index settings.
"""

from pathlib import Path

import luigi
import typer
from typing_extensions import Annotated

from longeval.etl.parquet.workflow import Workflow as ParquetWorkflow
from longeval.luigi import luigi_kwargs
from .tasks import OpenSearchLoadTask
from longeval.settings import SCRATCH_PATH


class Workflow(luigi.Task):
    """Main Luigi workflow for loading all collections into OpenSearch.

    This workflow identifies all document collections in the system and
    triggers OpenSearchLoadTask for each one.
    """

    root = luigi.Parameter()
    overwrite = luigi.BoolParameter(
        default=False, description="Overwrite existing index"
    )
    opensearch_host = luigi.Parameter(default="localhost:9200")

    def dependencies(self):
        """Define workflow dependencies - requires Parquet workflow to complete first."""
        return [ParquetWorkflow(root=self.root)]

    def _get_collection_roots(self, root):
        """Locate all document collections in the system."""
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def requires(self):
        """Generate OpenSearchLoadTask instances for each collection."""
        tasks = []
        parquet_root = Path(self.root) / "parquet"
        for collection_root in self._get_collection_roots(parquet_root):
            tasks.append(
                OpenSearchLoadTask(
                    input_path=collection_root.as_posix(),
                    overwrite=self.overwrite,
                    opensearch_host=self.opensearch_host,
                )
            )
        yield tasks


def main(
    input_path: Annotated[
        str, typer.Argument(help="Path to the collection root")
    ] = SCRATCH_PATH,
    overwrite: Annotated[bool, typer.Option(help="Overwrite existing index")] = False,
    opensearch_host: Annotated[
        str, typer.Option(help="OpenSearch host address")
    ] = "localhost:9200",
    scheduler_host: Annotated[str, typer.Option(help="Scheduler host")] = None,
):
    """Command-line entry point for the OpenSearch indexing workflow."""
    luigi.build(
        [
            Workflow(
                root=input_path,
                overwrite=overwrite,
                opensearch_host=opensearch_host,
            )
        ],
        **luigi_kwargs(scheduler_host),
    )
