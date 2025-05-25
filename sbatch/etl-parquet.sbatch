#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

cd ~/scratch/longeval
source .venv/bin/activate

nproc
free -h

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
scratch_dir=$(realpath ~/scratch/longeval)

export PYSPARK_DRIVER_CORES=16
export PYSPARK_DRIVER_MEMORY=90g
export SPARK_LOCAL_DIR=${TMPDIR}/spark-tmp
longeval etl parquet \
    $scratch_dir/raw/2025 \
    $project_dir/2025/parquet

# set to 777 recursively on the output directory to make it readable by all
chmod -R 777 $project_dir/2025/parquet || true
