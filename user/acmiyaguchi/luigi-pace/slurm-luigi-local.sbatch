#!/bin/bash
#SBATCH -Jlongeval-luigi-local                  # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N1 -n1                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=2                       # Number of cores per task
#SBATCH --mem-per-cpu=1G                        # Memory per core
#SBATCH -t5                                     # Duration of the job (Ex: 15 mins)
#SBATCH -qinferno                               # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
set -ue

cd ~/scratch/longeval
python3 -m venv venv
source venv/bin/activate
pip install luigi

set +x
cd $SLURM_SUBMIT_DIR
DATADIR=~/scratch/tmp/longeval-$(date +%s)
mkdir -p $DATADIR
python workflow.py \
    --output-path $DATADIR \
    --num-workers 4
