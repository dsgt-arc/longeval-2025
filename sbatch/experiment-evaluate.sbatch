#!/bin/bash
#SBATCH -Jlongeval-evaluate --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=6G
#SBATCH -t600 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL,SUCCESS --mail-user=acmiyaguchi@gatech.edu
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

export PYSPARK_DRIVER_CORES=24
export PYSPARK_DRIVER_MEMORY=$(($PYSPARK_DRIVER_CORES*6-4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
python -m longeval.experiment.evaluate.workflow --workers 12
EOF
