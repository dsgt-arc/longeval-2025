#!/bin/bash
#SBATCH --job-name=longeval-embed --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --gres=gpu:V100:1 git --cpus-per-task=12 --mem-per-gpu=140G
#SBATCH --qos=embers -t120
#SBATCH --output=Report-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-3%5

set -eu
export NO_REINSTALL=1
source ~/clef/longeval-2025/scripts/activate

set -x
nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"

# start the nvidia monitoring job in the background using slurm job id
NVIDIA_LOG_FILE=Report-${SLURM_JOB_ID}-nvidia-logs.ndjson
nvidia-logs monitor $NVIDIA_LOG_FILE --interval 15 &
nvidia_logs_pid=$!

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
dataset=$1
model_name=$2
export PYSPARK_DRIVER_MEMORY=10g
export PYSPARK_EXECUTOR_MEMORY=10g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl embedding \
    $project_dir/parquet/$dataset \
    $project_dir/embedding/${model_name/\//-}/$dataset \
    --model-name $model_name \
    --cpu-count 12 \
    --batch-size 1 \
    --num-sample-ids 50 \
    --sample-id $SLURM_ARRAY_TASK_ID

# kill the nvidia monitoring job and then parse the output
kill $nvidia_logs_pid
nvidia-logs parse $NVIDIA_LOG_FILE
