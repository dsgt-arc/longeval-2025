#!/bin/bash
#SBATCH --job-name=longeval-check-join --account=paceship-dsgt_clef2025 --qos=inferno
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=8 --mem-per-cpu=4G
#SBATCH --time=00:10:00  --output=logs/Report-check-join-%A_%a.out

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
results = spark.read.parquet(f"{scratch}/2025/bm25/retrieval_joined_expanded")
# by date, qid, docid count
(
    results
    .groupBy("date", "qid")
    .count()
    .describe()
    .show(truncate=False)
)
EOF
