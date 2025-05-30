#!/bin/bash
#SBATCH --job-name=longeval-join --account=paceship-dsgt_clef2025 --qos=inferno
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=16 --mem-per-cpu=4G
#SBATCH --time=01:00:00  --output=logs/Report-join-%A_%a.out

set -xeu

cd ~/scratch/longeval
source .venv/bin/activate

export PYSPARK_DRIVER_CORES=16
export PYSPARK_DRIVER_MEMORY=$((16*4-4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
python $HOME/clef/longeval-2025/user/acmiyaguchi/rerank_expanded/join_retrieval.py
