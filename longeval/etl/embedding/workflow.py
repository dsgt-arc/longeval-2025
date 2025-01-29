import luigi
import typer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing_extensions import Annotated

from longeval.spark import spark_resource

from .ml import WrappedSentenceTransformer


class SentenceTransformerPipeline(luigi.Task):
    output_path = luigi.Parameter()
    model_name = luigi.Parameter(default="all-MiniLM-L6-v2")
    batch_size = luigi.IntParameter(default=8)

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/metadata/_SUCCESS")

    def pipeline(self) -> Pipeline:
        return Pipeline(
            stages=[
                WrappedSentenceTransformer(
                    input_col="contents",
                    output_col="embedding",
                    model_name=self.model_name,
                    batch_size=self.batch_size,
                )
            ]
        )

    def run(self):
        with spark_resource() as spark:
            model = self.pipeline().fit(spark.createDataFrame([[""]], ["text"]))
            model.write().overwrite().save(f"{self.output_path}")


class ProcessSentenceTransformer(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    # we break the dataset into a number of samples that are processed in parallel
    sample_id = luigi.IntParameter()
    num_sample_ids = luigi.IntParameter(default=20)
    primary_key = luigi.Parameter(default="docid")
    feature_columns = luigi.ListParameter(default=["embedding"])
    # controls the number of partitions written to disk, must be at least the number
    # of tasks that we have in parallel to best take advantage of disk
    num_partitions = luigi.IntParameter(default=8)
    model_name = luigi.Parameter(default="all-MiniLM-L6-v2")
    batch_size = luigi.IntParameter(default=32)
    cpu_count = luigi.IntParameter(default=8)

    def output(self):
        # write a partitioned dataset to disk
        return luigi.LocalTarget(
            f"{self.output_path}/data/sample_id={self.sample_id}/_SUCCESS"
        )

    def requires(self):
        return [
            SentenceTransformerPipeline(
                output_path=f"{self.output_path}/model",
                model_name=self.model_name,
                batch_size=self.batch_size,
            )
        ]

    def transform(self, model, df, features) -> DataFrame:
        transformed = model.transform(df)
        for c in features:
            # check if the feature is a vector and convert it to an array
            if "array" in transformed.schema[c].simpleString():
                continue
            transformed = transformed.withColumn(c, vector_to_array(F.col(c)))
        return transformed

    def run(self):
        kwargs = {
            "cores": self.cpu_count,
            "spark.sql.shuffle.partitions": max(self.num_partitions, 200),
        }
        with spark_resource(**kwargs) as spark:
            # read the data and keep the sample we're currently processing
            df = (
                spark.read.parquet(self.input_path)
                .withColumn(
                    "sample_id", F.crc32(self.primary_key) % self.num_sample_ids
                )
                .where(F.col("sample_id") == self.sample_id)
                .drop("sample_id")
            )
            # transform the dataframe and write it to disk
            df = self.transform(
                PipelineModel.load(f"{self.output_path}/model"),
                df,
                self.feature_columns,
            )
            df.printSchema()
            df.explain()
            (
                df.repartition(self.num_partitions)
                .write.mode("overwrite")
                .parquet(f"{self.output_path}/data/sample_id={self.sample_id}")
            )


class Workflow(luigi.WrapperTask):
    """A dummy workflow with two tasks."""

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    sample_id = luigi.OptionalIntParameter()
    num_sample_ids = luigi.IntParameter(default=20)
    model_name = luigi.Parameter(default="all-MiniLM-L6-v2")
    cpu_count = luigi.IntParameter(default=8)

    def requires(self):
        # either we run a single task or we run all the tasts
        if self.sample_id is not None:
            sample_ids = [self.sample_id]
        else:
            sample_ids = list(range(self.num_tasks))

        tasks = []
        for sample_id in sample_ids:
            task = ProcessSentenceTransformer(
                input_path=self.input_path,
                output_path=self.output_path,
                sample_id=sample_id,
                num_sample_ids=self.num_sample_ids,
                model_name=self.model_name,
                cpu_count=self.cpu_count,
            )
            tasks.append(task)
        yield tasks


def main(
    input_path: Annotated[str, typer.Argument(help="Input root directory")],
    output_path: Annotated[str, typer.Argument(help="Output root directory")],
    sample_id: Annotated[int, typer.Option(help="Sample ID")] = None,
    num_sample_ids: Annotated[int, typer.Option(help="Number of sample IDs")] = 50,
    model_name: Annotated[str, typer.Option(help="Model name")] = "all-MiniLM-L6-v2",
    scheduler_host: Annotated[str, typer.Option(help="Scheduler host")] = None,
    cpu_count: Annotated[int, typer.Option(help="Number of CPUs")] = 8,
):
    """Count the number of tokens in the collection"""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [
            Workflow(
                input_path=input_path,
                output_path=output_path,
                sample_id=sample_id,
                num_sample_ids=num_sample_ids,
                model_name=model_name,
                cpu_count=cpu_count,
            )
        ],
        **kwargs,
    )
