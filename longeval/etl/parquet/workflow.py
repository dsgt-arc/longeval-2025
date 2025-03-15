"""Convert raw data to parquet"""

import luigi
from longeval.collection import RawCollection
from longeval.spark import get_spark
from pathlib import Path
import typer
from typing_extensions import Annotated
from longeval.luigi import luigi_kwargs
from longeval.settings import SCRATCH_PATH


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
        collection = RawCollection(spark, self.input_path)
        collection.to_parquet(self.output_path)


class Workflow(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def _get_collection_roots(self, root):
        # look for all directories that have Documents and Queries as subdirectories
        return [
            path
            for path in Path(root).glob("**/*")
            if (path / "Documents").exists() and (path / "Queries").exists()
        ]

    def run(self):
        tasks = []
        for collection_root in self._get_collection_roots(f"{self.input_path}"):
            # let's rename a few of the parts before we write this to parquet
            #   raw -> parquet
            parts = list(Path(collection_root).relative_to(self.input_path).parts)
            parts[0] = "parquet"
            parts[1] = "train" if "train" in parts[1].lower() else "test"
            output_path = Path(self.output_path, *parts)
            tasks.append(
                ParquetCollectionTask(
                    input_path=collection_root.as_posix(),
                    output_path=output_path.as_posix(),
                )
            )
        yield tasks


def to_parquet(
    input_path: Annotated[
        str, typer.Argument(help="Input root directory")
    ] = SCRATCH_PATH,
    output_path: Annotated[str, typer.Option(help="Output root directory")] = None,
    scheduler_host: Annotated[str, typer.Option(help="Scheduler host")] = None,
):
    """Convert raw data to parquet"""
    luigi.build(
        [Workflow(input_path=input_path, output_path=output_path or input_path)],
        **luigi_kwargs(scheduler_host),
    )
