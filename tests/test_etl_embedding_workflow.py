import pytest
from longeval.etl.embedding.workflow import ProcessSentenceTransformer
from pyspark.sql import Row
import luigi


@pytest.fixture
def df(spark):
    # dataframe with a single text column
    return spark.createDataFrame(
        [
            Row(text="This is a test sentence."),
            Row(text="This is another test sentence."),
        ]
    )


@pytest.fixture
def temp_parquet(df, tmp_path):
    path = tmp_path / "data"
    df.write.parquet(path.as_posix())
    return path


def test_process_test_sentence_transformer(spark, temp_parquet, tmp_path):
    task = ProcessSentenceTransformer(
        input_path=temp_parquet.as_posix(),
        output_path=temp_parquet.as_posix(),
        sample_id=0,
        num_sample_ids=1,
        model_name="all-MiniLM-L6-v2",
    )
    luigi.build([task], local_scheduler=True)
    transformed = spark.read.parquet(f"{temp_parquet}/data/sample_0")
    assert transformed.count() == 2
    assert transformed.columns == ["text", "transformed"]
    row = transformed.select("transformed").first()
    assert len(row.transformed) == 384
    assert all(isinstance(x, float) for x in row.transformed)
    transformed.show()
