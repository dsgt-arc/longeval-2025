from longeval.spark import get_spark
from longeval.collection import ParquetCollection
from pathlib import Path

spark = get_spark(cores=8, memory="20g")

data_root = Path("~/shared/longeval/2025/parquet/").expanduser()
print(data_root)
train = ParquetCollection(spark, data_root)
train.queries.printSchema()
train.qrels.printSchema()


train.documents.printSchema()
train.documents.groupBy("date").count().orderBy("date").show()

retrieval_root = Path("~/shared/longeval/2025/bm25/retrieval").expanduser()
retrieval = spark.read.parquet(retrieval_root.as_posix())
retrieval.printSchema()
retrieval.show(n=3)
retrieval.count()


from pyspark.sql import functions as F

# zip docids scores and qres into a single column
joined = (
    retrieval.withColumn("zipped", F.arrays_zip("docids", "scores"))
    .select("qid", "date", F.explode("zipped").alias("zipped"))
    .select(
        "qid",
        "date",
        F.col("zipped.docids").alias("docid"),
        F.col("zipped.scores").alias("score"),
    )
    .join(
        train.documents.select("docid", "date", "contents"),
        on=["docid", "date"],
    )
    .join(
        train.queries.select("qid", "query"),
        on=["qid"],
    )
)
joined.printSchema()


joined.write.partitionBy("date").parquet(
    Path("~/scratch/longeval/2025/bm25/retrieval_joined").expanduser().as_posix(),
    mode="overwrite",
)