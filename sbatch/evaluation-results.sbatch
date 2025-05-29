#!/bin/bash
#SBATCH --job-name=longeval-evaluation-results --account=paceship-dsgt_clef2025 --qos=inferno
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=8 --mem-per-cpu=4G
#SBATCH --time=00:10:00  --output=Report-evaluation-results-%A_%a.out

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
root = Path("~/shared/longeval/2025/bm25").expanduser()
spark = get_spark()

folders = [
    "evaluation",
    "evaluation_expanded",
]

for folder in folders:
    evaluation = spark.read.parquet((root / folder).as_posix())
    evaluation.describe().show(truncate=False)
EOF
