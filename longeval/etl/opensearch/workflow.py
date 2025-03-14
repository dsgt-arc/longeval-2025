"""Load documents into opensearch for indexing."""

from pathlib import Path

import luigi
import typer
from opensearchpy import OpenSearch
from typing_extensions import Annotated
from contexttimer import Timer
import json

from longeval.collection import ParquetCollection
from longeval.etl.parquet.workflow import Workflow as ParquetWorkflow
from longeval.spark import spark_resource

from .targets import OpenSearchIndexTarget


def update_index_template(opensearch_host, number_of_shards=1):
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
    """Load documents into opensearch."""

    input_path = luigi.Parameter()
    opensearch_host = luigi.Parameter()

    def _index_name(self):
        collection = ParquetCollection(None, self.input_path)
        metadata = collection.metadata
        split, date, language = (
            metadata["split"],
            metadata["date"],
            metadata["language"],
        )
        return f"{split}-{language}-{date}".lower()

    def _timing_path(self):
        return (
            Path(
                Path(self.input_path)
                .as_posix()
                .replace("parquet/", "opensearch_load_timings/")
            )
            / "timing.json"
        )

    def output(self):
        return [
            OpenSearchIndexTarget(self._index_name(), host=self.opensearch_host),
            luigi.LocalTarget(self._timing_path().as_posix()),
        ]

    def run(self):
        update_index_template(self.opensearch_host)
        # delete the index if it exists
        client = OpenSearch(self.opensearch_host)
        index_name = self._index_name()
        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)
            client.indices.refresh()

        with spark_resource() as spark:
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
    root = luigi.Parameter(default="/mnt/data/longeval")
    opensearch_host = luigi.Parameter(default="localhost:9200")

    def dependencies(self):
        return [ParquetWorkflow(root=self.root)]

    def _get_collection_roots(self, root):
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def run(self):
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
    """Load documents into opensearch for indexing."""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build([Workflow(opensearch_host=opensearch_host)], **kwargs)
