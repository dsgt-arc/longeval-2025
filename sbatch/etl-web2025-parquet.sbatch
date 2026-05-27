#!/bin/bash
#SBATCH -Jlongeval-web2025-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=8G
#SBATCH -t240 -qinferno -oReport-%A_%a.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

# LongEval-Web 2025 raw collections -> partitioned parquet, reusing the
# validated TrecCollection reader (longeval etl web2025-parquet).
#
# Two ways to run:
#   sbatch sbatch/etl-web2025-parquet.sbatch              # one job, all 15 dates (Spark parallelizes over 24 cores)
#   sbatch --array=0-14 sbatch/etl-web2025-parquet.sbatch # fan out: one date per task (dynamic partition overwrite)
# Both are idempotent; the array form just spreads the TREC parse across nodes.

DATES=( 2022-06 2022-07 2022-08 2022-09 2022-10 2022-11 2022-12 2023-01 2023-02 \
        2023-03 2023-04 2023-05 2023-06 2023-07 2023-08 )
if [ -n "${SLURM_ARRAY_TASK_ID:-}" ]; then
    ONLY_DATE="${DATES[$SLURM_ARRAY_TASK_ID]}"
    echo "array task -> only date: $ONLY_DATE"
else
    ONLY_DATE=""
    echo "single job -> all dates"
fi

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --env ONLY_DATE="$ONLY_DATE" \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
source ~/scratch/longeval/app/.venv/bin/activate

# The repo .env hardcodes a NixOS JAVA_HOME/JVM_PATH (the dev box); longeval.spark
# loads it via python-dotenv with override=False, so exporting the container's
# OpenJDK 21 here wins. Without this PySpark's JVM launch fails with
# JAVA_GATEWAY_EXITED ("/nix/store/.../bin/java: No such file or directory").
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so

scratch_dir=$(realpath ~/scratch/longeval)
web=$scratch_dir/ir_datasets/longeval-web
train_root="$web/release_2025_p1/release_2025_p1/French/LongEval Train Collection"
test_root="$web/LongEval Test Collection/LongEval Test Collection"

export PYSPARK_DRIVER_CORES=24
# Driver heap at ~75% of the 192G cgroup (24 cpus x 8G). The single local[]
# JVM is the whole engine, but -Xmx must leave room for the Python driver,
# Spark off-heap/maxResultSize, JVM metaspace/stacks, and OS page cache —
# setting it to ~184g (cgroup-8) made the JVM abort at launch
# (JAVA_GATEWAY_EXITED). 144g is ample for the TREC parse.
export PYSPARK_DRIVER_MEMORY=$(($PYSPARK_DRIVER_CORES*8*3/4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp

longeval etl web2025-parquet \
    "$train_root" \
    "$test_root" \
    "$scratch_dir/2025/parquet" \
    --test-qrels-root "$scratch_dir/raw/2025/test-qrels/longeval_web_qrels" \
    --relink-root "$scratch_dir/longeval-trec-relink" \
    ${ONLY_DATE:+--only-date "$ONLY_DATE"}
EOF
