#!/bin/bash
#SBATCH -Jlongeval-test                         # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N1 -n1                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=2                       # Number of cores per task
#SBATCH --mem-per-cpu=2G                        # Memory per core
#SBATCH -t10                                    # Duration of the job (Ex: 15 mins)
#SBATCH -qinferno                               # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
set -ue

cd ~/scratch/longeval
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
# we assume this script is run at the root of the project
pip install -e $SLURM_SUBMIT_DIR

set +x
cd $TMPDIR
cp -r $SLURM_SUBMIT_DIR/tests .
export PYSPARK_DRIVER_MEMORY=1g
pytest -vvv tests/
