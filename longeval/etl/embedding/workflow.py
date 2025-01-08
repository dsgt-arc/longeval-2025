import luigi
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from longeval.spark import spark_resource
from .ml import WrappedSentenceTransformer


class ProcessSentenceTransformer(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_partitions = luigi.IntParameter(default=200)
    model_name = luigi.Parameter(default="all-MiniLM-L6-v2")
    batch_size = luigi.IntParameter(default=8)

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    @property
    def feature_columns(self) -> list:
        raise NotImplementedError()

    def pipeline(self) -> Pipeline:
        return Pipeline(
            stages=[
                WrappedSentenceTransformer(
                    input_col="text",
                    output_col="transformer",
                    model_name=self.model_name,
                    batch_size=self.batch_size,
                )
            ]
        )

    def transform(self, model, df, features) -> DataFrame:
        transformed = model.transform(df)
        for c in features:
            # check if the feature is a vector and convert it to an array
            if "array" in transformed.schema[c].simpleString():
                continue
            transformed = transformed.withColumn(c, vector_to_array(F.col(c)))
        return transformed

    def run(self):
        with spark_resource(
            **{"spark.sql.shuffle.partitions": max(self.num_partitions, 200)}
        ) as spark:
            df = spark.read.parquet(self.input_path)
            model = self.pipeline().fit(df)
            model.write().overwrite().save(f"{self.output_path}/model")
            transformed = self.transform(model, df, self.feature_columns)
            transformed.repartition(self.num_partitions).write.mode(
                "overwrite"
            ).parquet(f"{self.output_path}/data")

        # now write the success file
        with self.output().open("w") as f:
            f.write("")
