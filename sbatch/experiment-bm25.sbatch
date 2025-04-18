#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=16 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

cd ~/scratch/longeval
source .venv/bin/activate

nproc
free -h

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
scratch_dir=$(realpath ~/scratch/longeval)

export JAVA_HOME=/usr/lib/jvm/jre
export PYSPARK_DRIVER_CORES=8
export PYSPARK_DRIVER_MEMORY=25g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
python -m longeval.experiment.bm25.workflow \
    ~/shared/longeval/2025/parquet \
    $scratch_dir/temp/bm25 \
    --should-sample \
    --parallelism $PYSPARK_DRIVER_CORES
