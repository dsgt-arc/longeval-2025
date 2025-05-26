#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=16 --mem-per-cpu=4G
#SBATCH -t60 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
hostname
source ~/scratch/longeval/app/.venv/bin/activate
python -m pyserini.index.lucene -options

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
scratch_dir=$(realpath ~/scratch/longeval)

export PYSPARK_DRIVER_CORES=16
export PYSPARK_DRIVER_MEMORY=60g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
python -m longeval.experiment.bm25.workflow \
    $project_dir/2025/parquet \
    $scratch_dir/2025/bm25 \
    --should-sample \
    --parallelism $PYSPARK_DRIVER_CORES \
    --workers 1
EOF
