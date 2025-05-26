#!/bin/bash
#SBATCH -Jlongeval-token                        # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N1 -n1                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=8                       # Number of cores per task
#SBATCH --mem-per-cpu=4G                        # Memory per core
#SBATCH -t120                                   # Duration of the job (Ex: 15 mins)
#SBATCH -qinferno                               # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
#SBATCH --mail-type=BEGIN,END,FAIL              # Mail preferences
#SBATCH --mail-user=acmiyaguchi@gatech.edu      # E-mail address for notifications
set -eux

srun --ntasks=1 hostname
nproc
free -h

# create a virtual environment and install the packages
cd ~/scratch/longeval
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -e ~/clef/longeval-2025

DATADIR=~/p-dsgt_clef2025-0/longeval
export PYSPARK_DRIVER_MEMORY=30g
export SPARK_LOCAL_DIR=$TMPDIR/spark-tmp
longeval etl tokens $DATADIR $DATADIR
