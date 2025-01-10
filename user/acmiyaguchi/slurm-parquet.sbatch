#!/bin/bash
#SBATCH -Jlongeval-parquet                      # Job name
#SBATCH --account=paceship-dsgt_clef2025        # charge account
#SBATCH -N2 -n8                                 # Number of nodes and cores required
#SBATCH --mem-per-cpu=1G                        # Memory per core
#SBATCH -t120                                   # Duration of the job (Ex: 15 mins)
#SBATCH -qinferno                               # QOS Name
#SBATCH -oReport-%j.out                         # Combined output and error messages file
#SBATCH --mail-type=BEGIN,END,FAIL              # Mail preferences
#SBATCH --mail-user=acmiyaguchi@gatech.edu      # E-mail address for notifications
set -xeu PIPEFAIL

# NOTE: we run this on a single node because we are using
# rsync to bring the data to the current node before processing it.
srun --ntasks 1 hostname

# cd $SLURM_SUBMIT_DIR
SCRATCH_DIR=~/scratch/longeval
mkdir -p $SCRATCH_DIR
cd $SCRATCH_DIR

# create a virtual environment and install the packages
function create_venv() {
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -e ~/clef/longeval-2025
}
create_venv

# let's copy and extract the zip data
shared_data=~/p-dsgt_clef2025-0

# run these in parallel, takes about 2 minutes to copy
# 22G Jan  8 22:13 LongEvalTestCollection.zip
# 15G Jan  8 22:15 LongEvalTrainCollection.zip
srun --ntasks=1 rsync -ah --progress $shared_data/longeval/raw/LongEvalTrainCollection.zip raw/ &
srun --ntasks=1 rsync -ah --progress $shared_data/longeval/raw/LongEvalTestCollection.zip raw/ &
time wait

# this takes about 15 minutes to run
srun --ntasks=1 --export UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE \
    unzip -o raw/LongEvalTrainCollection.zip -d $SCRATCH_DIR/raw/ &
srun --ntasks=1 --export UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE \
    unzip -o raw/LongEvalTestCollection.zip -d $SCRATCH_DIR/raw/ &
time wait

luigid &
srun \
    --ntasks=2 \
    --export PYSPARK_LOCAL_DIR=$TMPDIR/spark-tmp \
        longeval etl parquet $SCRATCH_DIR $shared_data/longeval \
            --scheduler-host http://$(hostname):8082

# # rsync the results back to the shared data
# srun --ntasks=1 rsync -ah --progress $TMPDIR/parquet $shared_data/longeval/
