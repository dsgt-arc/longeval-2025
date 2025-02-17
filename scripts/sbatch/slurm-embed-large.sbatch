#!/bin/bash
#SBATCH --job-name=longeval-embed --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --gres=gpu:V100:1 --cpus-per-task=6 --mem-per-gpu=64G
#SBATCH --qos=embers -t120
#SBATCH --output=Report-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-49%5

set -xeu
cd ~/scratch/longeval
source .venv/bin/activate
export PATH=$PATH:~/clef/longeval-2025/scripts

nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"

# start the nvidia monitoring job in the background using slurm job id
NVIDIA_LOG_FILE=${SLURM_SUBMIT_DIR}/Report-${SLURM_JOB_ID}-nvidia-logs.ndjson
nvidia-logs monitor $NVIDIA_LOG_FILE --interval 15 &
nvidia_logs_pid=$!

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
scratch_dir=$(realpath ~/scratch/longeval)
dataset=$1
model_name=$2
export PYSPARK_DRIVER_MEMORY=8g
export PYSPARK_EXECUTOR_MEMORY=8g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl embedding \
    $scratch_dir/parquet/$dataset \
    $project_dir/embedding/${model_name/\//-}/$dataset \
    --model-name $model_name \
    --cpu-count 6 \
    --batch-size 1 \
    --num-sample-ids 50 \
    --sample-id $SLURM_ARRAY_TASK_ID

# kill the nvidia monitoring job and then parse the output
kill $nvidia_logs_pid
nvidia-logs parse $NVIDIA_LOG_FILE
