#!/bin/bash
#SBATCH -Jlongeval-bm25 --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=6G
#SBATCH -t600 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL,SUCCESS --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-3%4
set -xeu

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --env SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
hostname
source ~/scratch/longeval/app/.venv/bin/activate

# The repo .env hardcodes a NixOS JAVA_HOME/JVM_PATH; override with the
# container's OpenJDK 21 (load_dotenv uses override=False, so env wins).
# JVM_PATH is what pyjnius/pyserini's Lucene JVM honors; JAVA_HOME is for Spark.
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so

python -m pyserini.index.lucene -options

# 2025 project storage is deallocated; everything lives under scratch now.
scratch_dir=$(realpath ~/scratch/longeval)

export PYSPARK_DRIVER_CORES=24
# ~75% of the 144G cgroup (24 cpus x 6G), leaving room for the separate Lucene
# indexing JVM, the Python driver, and OS page cache.
export PYSPARK_DRIVER_MEMORY=$(($PYSPARK_DRIVER_CORES*6*3/4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
# `run-bm25` must be named explicitly: the module's Typer app now exposes
# multiple commands (run-bm25, index-trec, compare-index-times), so a bare
# positional invocation is parsed as a command name and fails.
python -m longeval.experiment.bm25.workflow run-bm25 \
    $scratch_dir/2025/parquet \
    $scratch_dir/2025/bm25 \
    --scratch-path $scratch_dir/bm25 \
    --parallelism $PYSPARK_DRIVER_CORES \
    --workers 12 \
    --num-sample-ids 4 \
    --sample-id $SLURM_ARRAY_TASK_ID
EOF
