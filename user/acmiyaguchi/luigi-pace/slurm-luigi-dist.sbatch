#!/bin/bash
#SBATCH -Jlongeval-luigi-dist                   # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N4 -n4                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=1                       # Number of cores per task
#SBATCH --mem-per-cpu=1G                        # Memory per core
#SBATCH -t5                                     # Duration of the job (Ex: 15 mins)
#SBATCH -qinferno                               # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
set -ue

cd ~/scratch/longeval
python3 -m venv venv
source venv/bin/activate
pip install luigi

echo "Running on $(hostname)"
srun hostname

set +x
cd $SLURM_SUBMIT_DIR
DATADIR=~/scratch/tmp/longeval-$(date +%s)
mkdir -p $DATADIR

luigid &
pid=$!
# wait for luigid to start by curling the scheduler API
while ! curl -s localhost:8082 > /dev/null; do sleep 1; done

# make sure each one startups at slightly different times
srun bash -c "
    source ~/scratch/longeval/venv/bin/activate && \
    set -x && \
    python workflow.py \
        --output-path $DATADIR \
        --scheduler-host $(hostname)
"

kill $pid
