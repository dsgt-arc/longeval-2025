from longeval.spark import get_spark
from longeval.collection import ParquetCollection
from pathlib import Path
from pyspark.sql import functions as F, Window


def _deduplicate(df):
    """Deduplicate the documents based on docid and date.

    This seems like it might be pretty slow, but thankfully we only have to do this once.
    """
    window = Window.partitionBy("date", "docid").orderBy(F.desc(F.length("contents")))
    return (
        df.where(F.length("contents") > 50)
        .withColumn("rank", F.row_number().over(window))
        .where(F.col("rank") == 1)
        .drop("rank")
    )


spark = get_spark(cores=8, memory="20g")

# for the submission, we only need the test submission
# but for local testing we will need to get the test set to see
parquet_root = Path("~/shared/longeval/2025/parquet/").expanduser()
train = ParquetCollection(spark, f"{parquet_root}/train")
test = ParquetCollection(spark, f"{parquet_root}/test")
documents = train.documents.union(test.documents)
queries = train.queries.union(test.queries)

retrieval_root = Path("~/shared/longeval/2025/bm25/retrieval_expanded").expanduser()
retrieval = spark.read.parquet(retrieval_root.as_posix())


# zip docids scores and qres into a single column
joined = (
    retrieval.join(
        _deduplicate(documents.select("date", "docid", "contents")),
        on=["docid", "date"],
        how="left",
    )
    .join(
        queries.select("date", "qid", "query"),
        on=["date", "qid"],
        how="left",
    )
    .select("date", "qid", "docid", "query", "contents", "score")
)
joined.printSchema()

joined.write.partitionBy("date").parquet(
    Path("~/scratch/longeval/2025/bm25/retrieval_joined_expanded")
    .expanduser()
    .as_posix(),
    mode="overwrite",
)
