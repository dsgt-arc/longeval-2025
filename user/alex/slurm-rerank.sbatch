#!/bin/bash
#SBATCH --job-name=longeval-embed
#SBATCH --account=paceship-dsgt_clef2025
#SBATCH --nodes=1
#SBATCH --gres=gpu:V100:1
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-cpu=8G
#SBATCH --qos=inferno
#SBATCH --time=00:10:00
#SBATCH --output=logs/Report-%A_%a.out
#SBATCH --array=0-8    # @Anthony, adjust it to the number of dates

# Activate your environment
source scripts/activate
cd ~/github/longeval-2025/
ROOTDIR="/storage/scratch1/9/apramov3/longeval/2025/bm25/retrieval_joined/" # This should match the input files location in the config file

# Load the list of date directories
DATE_LIST=( $(ls -d $ROOTDIR/date=* | sed 's:.*/::') )
TARGET_DATE="${DATE_LIST[$SLURM_ARRAY_TASK_ID]}"

echo "Running reranking on date: $TARGET_DATE"

# Run the reranking script with the selected date
~/scratch/longeval/.venv/bin/python user/alex/run_reranking.py --date "$TARGET_DATE"

