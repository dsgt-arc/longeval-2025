"""Convert raw data to parquet"""

import luigi
from longeval.collection import RawCollection
from longeval.spark import spark_resource
from pathlib import Path
from argparse import ArgumentParser


class ParquetCollectionTask(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return [
            luigi.LocalTarget(f"{self.output_path}/Documents/_SUCCESS"),
            luigi.LocalTarget(f"{self.output_path}/Queries/_SUCCESS"),
            luigi.LocalTarget(f"{self.output_path}/Qrels/_SUCCESS"),
        ]

    def run(self):
        with spark_resource() as spark:
            collection = RawCollection(spark, self.input_path)
            collection.to_parquet(self.output_path)


class Workflow(luigi.Task):
    root = luigi.Parameter(default="/mnt/data/longeval")

    def _get_collection_roots(self, root):
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def run(self):
        tasks = []
        for collection_root in self._get_collection_roots(f"{self.root}/raw"):
            # let's rename a few of the parts before we write this to parquet
            #   raw -> parquet
            parts = list(Path(collection_root).relative_to(self.root).parts)
            parts[0] = "parquet"
            parts[1] = "train" if "train" in parts[1].lower() else "test"
            output_path = Path(self.root, *parts)
            tasks.append(
                ParquetCollectionTask(
                    input_path=collection_root,
                    output_path=output_path,
                )
            )
        yield tasks


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scheduler-host",
        type=str,
        default=None,
        help="run luigid, typically localhost:8082",
    )
    args = parser.parse_args()
    kwargs = {}
    if args.scheduler_host:
        kwargs["scheduler_host"] = args.scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build([Workflow()], **kwargs)


if __name__ == "__main__":
    main()
