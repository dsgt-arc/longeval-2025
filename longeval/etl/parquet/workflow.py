"""Convert raw data to parquet"""

import luigi
from longeval.collection import Raw2025Collection
from longeval.spark import get_spark
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
        collection = Raw2025Collection(spark, self.input_path)
        collection.to_parquet(self.output_path)


class Workflow(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def run(self):
        yield ParquetCollectionTask(
            input_path=self.input_path,
            output_path=self.output_path,
        )


def to_parquet(
    input_path: Annotated[
        str, typer.Argument(help="Input root directory")
    ] = SCRATCH_PATH + "/2025",
    output_path: Annotated[str, typer.Argument(help="Output root directory")] = None,
    scheduler_host: Annotated[str, typer.Option(help="Scheduler host")] = None,
):
    """Convert raw data to parquet"""
    luigi.build(
        [Workflow(input_path=input_path, output_path=output_path)],
        **luigi_kwargs(scheduler_host),
    )
