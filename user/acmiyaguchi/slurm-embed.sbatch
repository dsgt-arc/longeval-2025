#!/bin/bash
#SBATCH -Jlongeval-embed                        # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N1 --gres=gpu:V100:1                   # Number of nodes and cores required
#SBATCH --cpus-per-task=1                       # Number of cores per task
#SBATCH --mem-per-gpu=32G                       # Memory per core
#SBATCH -t120                                   # Duration of the job (Ex: 15 mins)
##SBATCH --array=0-9%10                          # Array range and max jobs
#SBATCH -qinferno                                # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
##SBATCH --mail-type=END,FAIL                    # Mail preferences
##SBATCH --mail-user=acmiyaguchi@gatech.edu      # E-mail address for notifications
set -xeu

module load python/3.10
export CPATH=$PYTHON_ROOT/include/python3.10:$CPATH

cd ~/scratch/longeval
source venv/bin/activate

nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"
nvidia-smi

project_dir=~/p-dsgt_clef2025-0/longeval
dataset=train/2023_01/English/Documents
# model_name="all-MiniLM-L6-v2"
model_name="joe32140/ModernBERT-base-msmarco"
#model_name="answerdotai/ModernBERT-base"
export PYSPARK_DRIVER_MEMORY=28g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl embedding \
    $project_dir/parquet/$dataset \
    $project_dir/embedding/${model_name/\//-}/$dataset \
    --model-name $model_name \
    --cpu-count 1 \
    --num-sample-ids 8 \
    --sample-id 0
