"""
OpenSearch indexing workflow for Longeval.

This module defines Luigi tasks and workflows for loading document collections
into OpenSearch and configuring appropriate index settings.
"""

from pathlib import Path

import luigi
import typer
from opensearchpy import OpenSearch
from typing_extensions import Annotated
from contexttimer import Timer
import json

from longeval.collection import ParquetCollection
from longeval.etl.parquet.workflow import Workflow as ParquetWorkflow
from longeval.spark import get_spark

from .targets import OpenSearchIndexTarget


def update_index_template(opensearch_host, number_of_shards=1):
    """
    Create or update OpenSearch index template.

    This function configures a default index template for longeval collections,
    setting up appropriate mappings for document fields and optimizing
    index settings.
    """
    client = OpenSearch(opensearch_host)
    client.indices.put_index_template(
        name="longeval_default",
        body={
            "index_patterns": ["test-*", "train-*"],
            "priority": 10,
            "template": {
                "settings": {
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": number_of_shards,
                        # https://stackoverflow.com/questions/29040039/how-to-prevent-elasticsearch-from-index-throttling
                        # https://opensearch.org/docs/latest/tuning-your-cluster/performance/
                    }
                },
                "mappings": {
                    "properties": {
                        "contents": {
                            "type": "text",
                        },
                        "docid": {
                            "type": "keyword",
                        },
                    },
                },
            },
        },
    )


class OpenSearchLoadTask(luigi.Task):
    """
    Luigi task to load documents from a Parquet collection into OpenSearch.

    This task handles index name generation, index creation with proper settings,
    and loading documents from Parquet into OpenSearch using Spark.
    """

    input_path = luigi.Parameter()
    opensearch_host = luigi.Parameter()

    def _index_name(self):
        """
        Generate OpenSearch index name from collection metadata.

        Creates an index name using the split, language, and date from the collection
        metadata, following the format: split-language-date.
        """
        collection = ParquetCollection(None, self.input_path)
        metadata = collection.metadata
        split, date, language = (
            metadata["split"],
            metadata["date"],
            metadata["language"],
        )
        # example: train-english-2023_01
        return f"{split}-{language}-{date}".lower()

    def _timing_path(self):
        """
        Generate path for timing information output file.

        Creates a file path to store timing information about the indexing process.
        """
        return (
            Path(
                Path(self.input_path)
                .as_posix()
                .replace("parquet/", "opensearch_load_timings/")
            )
            / "timing.json"
        )

    def output(self):
        """
        Define task outputs: OpenSearch index and timing information file.

        The task produces both an OpenSearch index target and a local file
        containing timing information.
        """
        return [
            OpenSearchIndexTarget(self._index_name(), host=self.opensearch_host),
            luigi.LocalTarget(self._timing_path().as_posix()),
        ]

    def run(self):
        """
        Execute the document loading process.

        This method:
        1. Updates the index template
        2. Deletes the index if it already exists
        3. Loads documents from Parquet into OpenSearch using Spark
        4. Records timing information about the process
        """
        update_index_template(self.opensearch_host)
        # delete the index if it exists
        client = OpenSearch(self.opensearch_host)
        index_name = self._index_name()
        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)
            client.indices.refresh()

        spark = get_spark()
        collection = ParquetCollection(spark, self.input_path)
        with Timer() as timer:
            (
                collection.documents.write.format("org.opensearch.spark.sql")
                .option("opensearch.nodes", self.opensearch_host.split(":")[0])
                .option("opensearch.port", self.opensearch_host.split(":")[1])
                .option("opensearch.nodes.wan.only", "true")
                .mode("overwrite")
                .save(index_name)
            )

        timing_path = self._timing_path()
        timing_path.parent.mkdir(parents=True, exist_ok=True)
        with timing_path.open("w") as fp:
            json.dump(
                {
                    "time": timer.elapsed,
                    "rows": collection.documents.count(),
                    "src": self.input_path,
                    "index": index_name,
                },
                fp,
            )


class Workflow(luigi.Task):
    """
    Main Luigi workflow for loading all collections into OpenSearch.

    This workflow identifies all document collections in the system and
    triggers OpenSearchLoadTask for each one.
    """

    root = luigi.Parameter(default=f"{Path('~').expanduser()}/scratch/longeval")
    opensearch_host = luigi.Parameter(default="localhost:9200")

    def dependencies(self):
        """Define workflow dependencies - requires Parquet workflow to complete first."""
        return [ParquetWorkflow(root=self.root)]

    def _get_collection_roots(self, root):
        """
        Locate all document collections in the system.

        Identifies directories that contain both Documents and Queries subdirectories,
        which represent complete document collections.
        """
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def requires(self):
        """
        Generate OpenSearchLoadTask instances for each collection.

        Scans the parquet directory for collections and creates a load task for each one.
        """
        tasks = []
        parquet_root = Path(self.root) / "parquet"
        for collection_root in self._get_collection_roots(parquet_root):
            tasks.append(
                OpenSearchLoadTask(
                    input_path=collection_root.as_posix(),
                    opensearch_host=self.opensearch_host,
                )
            )
        yield tasks


def main(
    opensearch_host: Annotated[
        str, typer.Argument(help="OpenSearch host")
    ] = "localhost:9200",
    scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    """
    Command-line entry point for the OpenSearch indexing workflow.

    Executes the Luigi workflow to load documents into OpenSearch,
    optionally connecting to a remote scheduler.
    """
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build([Workflow(opensearch_host=opensearch_host)], **kwargs)
