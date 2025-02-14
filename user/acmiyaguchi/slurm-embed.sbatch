#!/bin/bash
#SBATCH --job-name=longeval-embed --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --gres=gpu:V100:1 --cpus-per-task=12 --mem-per-gpu=70G
#SBATCH --qos=inferno -t120     # set qos=embers for free gpu jobs
#SBATCH --output=Report-%j.out
##SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
##SBATCH --array=1,10,50
# for parallel runs, we can do this for some parameter sweeps
# these can be accessed in the script as $SLURM_ARRAY_TASK_ID

set -eu
export NO_REINSTALL=1
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
# model_name="answerdotai/ModernBERT-base"
model_name="nomic-ai/modernbert-embed"
batch_size=$SLURM_ARRAY_TASK_ID
export PYSPARK_DRIVER_MEMORY=10g
export PYSPARK_EXECUTOR_MEMORY=10g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp

# NOTE: we always generate a temporary directory for embeddings because
# this is a testing job and we want to make sure to always write new data.
# The slurm job id is used to ensure that we don't overwrite data.
longeval etl embedding \
    $project_dir/parquet/$dataset \
    ~/scratch/longeval/test/embedding/${model_name/\//-}/${SLURM_JOB_ID}/$dataset \
    --model-name $model_name \
    --cpu-count 6 \
    --batch-size $batch_size \
    --num-sample-ids 50 \
    --sample-id 0

# kill the nvidia monitoring job and then parse the output
kill $nvidia_logs_pid
nvidia-logs parse $NVIDIA_LOG_FILE
