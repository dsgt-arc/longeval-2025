"""Convert raw data to parquet"""

import luigi
from longeval.collection import Raw2025Collection, Raw2025TestCollection
from longeval.spark import get_spark
import typer
from typing_extensions import Annotated
from longeval.luigi import luigi_kwargs
from .tokens import TokenTask
from pathlib import Path


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
        if "train" in str(self.output_path):
            collection = Raw2025Collection(spark, self.input_path)
        else:
            collection = Raw2025TestCollection(spark, self.input_path)
        collection.to_parquet(self.output_path)


class Workflow(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def run(self):
        yield [
            ParquetCollectionTask(
                input_path=self.input_path,
                output_path=f"{self.output_path}/train",
            ),
            ParquetCollectionTask(
                input_path=self.input_path,
                output_path=f"{self.output_path}/test",
            ),
        ]
        yield [
            TokenTask(
                input_path=f"{self.output_path}/train",
                output_path=(
                    Path(self.output_path).parent / "tokens" / "train"
                ).as_posix(),
            ),
            TokenTask(
                input_path=f"{self.output_path}/test",
                output_path=(
                    Path(self.output_path).parent / "tokens" / "test"
                ).as_posix(),
            ),
        ]


def to_parquet(
    input_path: Annotated[str, typer.Argument(help="Input root directory")],
    output_path: Annotated[str, typer.Argument(help="Output root directory")],
    scheduler_host: Annotated[str | None, typer.Option(help="Scheduler host")] = None,
):
    """Convert raw data to parquet"""
    luigi.build(
        [Workflow(input_path=input_path, output_path=output_path)],
        **luigi_kwargs(scheduler_host),
    )
