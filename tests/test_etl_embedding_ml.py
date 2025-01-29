import pytest
from longeval.etl.embedding.ml import WrappedSentenceTransformer
from pyspark.sql import Row


@pytest.fixture
def df(spark):
    # dataframe with a single text column
    return spark.createDataFrame(
        [
            Row(text="This is a test sentence."),
            Row(text="This is another test sentence."),
        ]
    )


def test_wrapped_sentence_transformer(df):
    model = WrappedSentenceTransformer(
        input_col="text",
        output_col="transformed",
        model_name="all-MiniLM-L6-v2",
        batch_size=8,
    )
    transformed = model.transform(df).cache()
    transformed.printSchema()
    transformed.show()
    assert transformed.count() == 2
    assert transformed.columns == ["text", "transformed"]
    row = transformed.select("transformed").first()
    assert len(row.transformed) == 384
    assert all(isinstance(x, float) for x in row.transformed)
    transformed.show()
