#!/bin/bash
#SBATCH --job-name=longeval-rerank --account=paceship-dsgt_clef2025 --qos=inferno --output=logs/Report-%A_%a.out
#SBATCH --nodes=1 --gres=gpu:V100:1 --cpus-per-task=12 --mem-per-cpu=8G
#SBATCH --time=03:00:00 --array=0-14

# there are 15 dates in total, so 0-14
# Activate your environment
cd ~/scratch/longeval
source .venv/bin/activate

ROOTDIR="$HOME/scratch/longeval/2025/bm25/retrieval_joined"

DATE_LIST=( $(ls -d $ROOTDIR/date=* | sed 's:.*/::') )
TARGET_DATE="${DATE_LIST[$SLURM_ARRAY_TASK_ID]}"

echo "Running reranking on date: $TARGET_DATE"

# Run the reranking script with the selected date
python $HOME/clef/longeval-2025/user/acmiyaguchi/rerank/run_reranking.py \
    --date "$TARGET_DATE"
