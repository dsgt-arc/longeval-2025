#!/bin/bash
#SBATCH -Jlongeval-download --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=4 --mem-per-cpu=8G
#SBATCH -t720 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

# Resumable LongEval-Web download into the ir_datasets cache on scratch.
# ir_datasets' ZipExtractCache skips zips that are already extracted, so a job
# killed at the 12h walltime can simply be resubmitted to continue. ~40 GB.

CACHE=~/scratch/longeval/ir_datasets
mkdir -p "$CACHE"

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
source ~/scratch/longeval/app/.venv/bin/activate
CACHE=~/scratch/longeval/ir_datasets
python ~/arc/longeval-2025/scripts/download-longeval \
    --namespace longeval-web \
    --cache "$CACHE"

echo "=== extracted size ==="
du -sh "$CACHE"/longeval-web || true
echo "=== top-level layout ==="
find "$CACHE"/longeval-web -maxdepth 3 -type d | head -50
EOF
