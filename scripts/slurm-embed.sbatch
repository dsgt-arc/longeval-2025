#!/bin/bash
#SBATCH -Jlongeval-embed                        # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N1 --gres=gpu:V100:1                   # Number of nodes and cores required
#SBATCH --cpus-per-task=4                       # Number of cores per task
#SBATCH --mem-per-gpu=32G                       # Memory per core
#SBATCH -t120                                   # Duration of the job (Ex: 15 mins)
#SBATCH --array=0-7%8                           # Array range and max jobs
#SBATCH -qembers                                # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
#SBATCH --mail-type=END,FAIL                    # Mail preferences
#SBATCH --mail-user=acmiyaguchi@gatech.edu      # E-mail address for notifications
set -xeu

cd ~/scratch/longeval
source venv/bin/activate

nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"

project_dir=~/p-dsgt_clef2025-0/longeval
dataset=$1
model_name=$2
export PYSPARK_DRIVER_MEMORY=28g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl embedding \
    $project_dir/parquet/$dataset \
    $project_dir/embedding/${model_name/\//-}/$dataset \
    --model-name $model_name \
    --cpu-count 4 \
    --num-sample-ids 8 \
    --sample-id $SLURM_ARRAY_TASK_ID
