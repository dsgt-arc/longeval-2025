from pathlib import Path

import luigi
from opensearchpy import OpenSearch
from contexttimer import Timer
import json
from functools import cache

from longeval.collection import ParquetCollection
from longeval.spark import get_spark

from .targets import OpenSearchIndexTarget


def update_index_template(opensearch_host, number_of_shards=1):
    """Create or update OpenSearch index template.

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
    """Luigi task to load documents from a Parquet collection into OpenSearch.

    This task handles index name generation, index creation with proper settings,
    and loading documents from Parquet into OpenSearch using Spark.
    """

    input_path = luigi.Parameter()
    overwrite = luigi.BoolParameter(default=False)
    opensearch_host = luigi.Parameter()

    def _index_name(self):
        """Generate OpenSearch index name from collection metadata."""
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
        """Generate path for timing information output file."""
        return (
            Path(
                Path(self.input_path)
                .as_posix()
                .replace("parquet/", "opensearch_load_timings/")
            )
            / "timing.json"
        )

    def output(self):
        """Define task outputs: OpenSearch index and timing information file."""
        return (
            [
                OpenSearchIndexTarget(
                    self._index_name(),
                    count=self._collection().documents.count(),
                    host=self.opensearch_host,
                )
            ]
            if not self.overwrite
            else [],
        )

    @cache
    def _collection(self):
        """Return the collection associated with this task."""
        spark = get_spark()
        return ParquetCollection(spark, self.input_path)

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

        collection = self._collection()

        with Timer() as timer:
            (
                collection.documents.write.format("org.opensearch.spark.sql")
                .option("opensearch.nodes", self.opensearch_host.split(":")[0])
                .option("opensearch.port", self.opensearch_host.split(":")[1])
                .option("opensearch.nodes.wan.only", "true")
                .option("opensearch.mapping.id", "docid")
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
