import numpy as np
from pyspark.ml import Transformer
from pyspark.ml.functions import predict_batch_udf
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, FloatType


class WrappedSentenceTransformer(
    Transformer,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """
    Wrapper for SentenceTransformers to add it to the pipeline
    """

    def __init__(
        self,
        input_col: str = "input",
        output_col: str = "output",
        model_name="all-MiniLM-L6-v2",
        batch_size=8,
    ):
        super().__init__()
        self._setDefault(inputCol=input_col, outputCol=output_col)
        self.model_name = model_name
        self.batch_size = batch_size

    def _make_predict_fn(self):
        """Return PredictBatchFunction using a closure over the model"""
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(self.model_name)

        def predict(inputs: np.ndarray) -> np.ndarray:
            return model.encode(inputs)

        return predict

    def _transform(self, df: DataFrame):
        return df.withColumn(
            self.getOutputCol(),
            predict_batch_udf(
                make_predict_fn=self._make_predict_fn,
                return_type=ArrayType(FloatType()),
                batch_size=self.batch_size,
            )(self.getInputCol()),
        )
