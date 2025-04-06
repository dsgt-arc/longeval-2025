#!/bin/bash
#SBATCH -Jlongeval-lda                          # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH --cpus-per-task=16                      # Number of cores per task
#SBATCH --mem-per-cpu=8G                        # Memory per core
#SBATCH -t120                                   # Duration of the job (Ex: 15 mins)
#SBATCH -qembers                                # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
#SBATCH --mail-type=END,FAIL                    # Mail preferences
#SBATCH --mail-user=iafrulbasha3@gatech.edu     # E-mail address for notifications
set -xeu

export NO_REINSTALL=1
source ~/p-dsgt_clef2025-0/longeval-2025/scripts/activate

nproc
free -h
python -c "import torch; print(torch.cuda.is_available())"

project_dir=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
scratch_dir=$(realpath ~/scratch/longeval)

export PYSPARK_DRIVER_MEMORY=110g
export PYSPARK_DRIVER_CORES=16
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl lda \
    20 \
    $project_dir/parquet/train/2023_01/English \
    $project_dir/lda
