import numpy as np
from pyspark.ml import Transformer
from pyspark.ml.functions import predict_batch_udf
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, FloatType


class HasModelName(Param):
    """
    Mixin for param model_name: str
    """

    modelName = Param(
        Params._dummy(),
        "modelName",
        "The name of the SentenceTransformer model to use",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super().__init__(
            default="all-MiniLM-L6-v2",
            doc="The name of the SentenceTransformer model to use",
        )

    def getModelName(self) -> str:
        return self.getOrDefault(self.modelName)


class HasBatchSize(Param):
    """
    Mixin for param batch_size: int
    """

    batchSize = Param(
        Params._dummy(),
        "batchSize",
        "The batch size to use for encoding",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self):
        super().__init__(default=8, doc="The batch size to use for encoding")

    def getBatchSize(self) -> int:
        return self.getOrDefault(self.batchSize)


class WrappedSentenceTransformer(
    Transformer,
    HasInputCol,
    HasOutputCol,
    HasModelName,
    HasBatchSize,
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
        self._setDefault(
            inputCol=input_col,
            outputCol=output_col,
            modelName=model_name,
            batchSize=batch_size,
        )

    def _nvidia_smi(self):
        from subprocess import run

        try:
            run(["nvidia-smi"], check=True)
        except Exception:
            pass

    def _make_predict_fn(self):
        """Return PredictBatchFunction using a closure over the model"""
        from sentence_transformers import SentenceTransformer

        # gpu memory before and after configuring the model
        self._nvidia_smi()
        model = SentenceTransformer(self.getModelName())
        self._nvidia_smi()

        def predict(inputs: np.ndarray) -> np.ndarray:
            return model.encode(inputs)

        return predict

    def _transform(self, df: DataFrame):
        return df.withColumn(
            self.getOutputCol(),
            predict_batch_udf(
                make_predict_fn=self._make_predict_fn,
                return_type=ArrayType(FloatType()),
                batch_size=self.getBatchSize(),
            )(self.getInputCol()),
        )
