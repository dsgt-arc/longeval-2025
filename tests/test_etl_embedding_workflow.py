import pytest
from longeval.etl.embedding.workflow import ProcessSentenceTransformer
from pyspark.sql import Row
import luigi
from longeval.spark import get_spark


@pytest.fixture
def df(spark):
    # dataframe with a single text column
    return spark.createDataFrame(
        [
            Row(docid="1", contents="This is a test sentence."),
            Row(docid="2", contents="This is another test sentence."),
        ]
    )


@pytest.fixture
def temp_parquet(df, tmp_path):
    path = tmp_path / "data"
    df.write.parquet(path.as_posix())
    return path


def test_process_test_sentence_transformer(spark, temp_parquet, tmp_path):
    output = tmp_path / "output"
    task = ProcessSentenceTransformer(
        input_path=temp_parquet.as_posix(),
        output_path=output.as_posix(),
        sample_id=0,
        num_sample_ids=1,
        model_name="all-MiniLM-L6-v2",
    )
    luigi.build([task], local_scheduler=True)
    # NOTE this kills our spark instance
    spark = get_spark(app_name="pytest")
    transformed = spark.read.parquet(f"{output}/data")
    assert transformed.count() == 2
    assert transformed.columns == ["docid", "contents", "embedding", "sample_id"]
    row = transformed.select("embedding").first()
    assert len(row.embedding) == 384
    assert all(isinstance(x, float) for x in row.embedding)
    transformed.show()
