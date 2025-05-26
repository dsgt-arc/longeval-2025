#!/bin/bash
#SBATCH --job-name=longeval-rerank --account=paceship-dsgt_clef2025 --qos=inferno
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=8 --mem-per-cpu=4G
#SBATCH --time=01:00:00  --output=logs/Report-submission-%A_%a.out

set -xeu

cd ~/scratch/longeval
source .venv/bin/activate

export PYSPARK_DRIVER_CORES=8
export PYSPARK_DRIVER_MEMORY=$((8*4-4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
python3 - <<EOF
from pathlib import Path
from longeval.spark import get_spark
from pyspark.sql import functions as F, Window
shared = Path("~/shared/longeval").expanduser()
scratch = Path("~/scratch/longeval").expanduser()
spark = get_spark()
results = spark.read.csv(f"{scratch}/2025/bm25/reranked", header=True)
results.printSchema()
results.show(5, truncate=False)
# over the set of submission dates only
dates = [
    "2023-03",
    "2023-04",
    "2023-05",
    "2023-06",
    "2023-07",
    "2023-08",
]
for date in dates:
    print(f"Processing date: {date}")
    submission = results.where(F.col("date") == date).select(
        "qid",
        F.lit("Q0").alias("Q0"),
        F.col("docid").alias("docno"),
        (
            F.row_number()
            .over(Window.partitionBy("qid").orderBy(F.desc("rerank_score")))
            .alias("rank")
        ),
        F.col("rerank_score").alias("score"),
        F.lit("dsgt_bm25_rerank_submission").alias("tag"),
    ).orderBy("qid", "rank").toPandas()
    assert submission.shape[0] > 0, "No results found for date: " + date
    output = shared / f"2025/bm25/bm25_rerank_submission/{date}/run.txt.gz"
    output.parent.mkdir(parents=True, exist_ok=True)
    submission.to_csv(
        output,
        sep="\t",
        index=False,
        header=False,
        compression="gzip",
    )
print("Done processing all dates.")
EOF
