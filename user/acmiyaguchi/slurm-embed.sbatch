#!/bin/bash
#SBATCH --job-name=longeval-embed --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --gres=gpu:V100:1 --cpus-per-task=6 --mem-per-gpu=40G
#SBATCH --qos=inferno -t120     # set qos=ember for free gpu jobs
#SBATCH --output=Report-%j.out
##SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu

# for parallel runs, break into 10 jobs 10 at a time e.g.
#SBATCH --array=0-9%10

set -eu
source ~/clef/longeval-2025/scripts/activate

set -x
nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"
nvidia-smi

# start the nvidia monitoring job in the background using slurm job id
NVIDIA_LOG_FILE=Report-${SLURM_JOB_ID}-nvidia-logs.ndjson
nvidia-logs monitor $NVIDIA_LOG_FILE --interval 15 &
nvidia_logs_pid=$!

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
dataset=train/2023_01/English/Documents
# model_name="all-MiniLM-L6-v2"
# model_name="joe32140/ModernBERT-base-msmarco"
model_name="answerdotai/ModernBERT-base"
export PYSPARK_DRIVER_MEMORY=5g
export PYSPARK_EXECUTOR_MEMORY=5g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp

# NOTE: we always generate a temporary directory for embeddings because
# this is a testing job and we want to make sure to always write new data.
# The slurm job id is used to ensure that we don't overwrite data.
longeval etl embedding \
    $project_dir/parquet/$dataset \
    ~/scratch/longeval/test/embedding/${model_name/\//-}/${SLURM_JOB_ID}/$dataset \
    --model-name $model_name \
    --cpu-count 6 \
    --batch-size 1 \
    --num-sample-ids 50 \
    --sample-id 0

# kill the nvidia monitoring job and then parse the output
kill $nvidia_logs_pid
nvidia-logs parse $NVIDIA_LOG_FILE
