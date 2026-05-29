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

# The LongEval-Web TEST qrels ship as longeval_web_test_qrels.zip — defined in
# the package's downloads.json (key longeval_2025_web_test_qrels) but NOT wired
# into register(), so the ir_datasets API reports has_qrels()==False for test
# snapshots. Fetch + extract it to the path the ETL reads. Idempotent.
QRELS_DEST=~/scratch/longeval/raw/2025/test-qrels
mkdir -p "$QRELS_DEST"
if [ ! -d "$QRELS_DEST/longeval_web_qrels" ]; then
    URL=$(python - <<'PY'
import json, glob
p = glob.glob("/storage/home/hcoda1/8/amiyaguchi3/scratch/longeval/app/.venv/lib/python*/site-packages/ir_datasets_longeval/etc/downloads.json")[0]
print(json.load(open(p))["longeval-web"]["longeval_2025_web_test_qrels"]["url"])
PY
)
    curl -sL --max-time 600 -o "$QRELS_DEST/longeval_web_test_qrels.zip" "$URL"
    unzip -oq "$QRELS_DEST/longeval_web_test_qrels.zip" -x "__MACOSX/*" "*/.DS_Store" -d "$QRELS_DEST"
    rm -rf "$QRELS_DEST/__MACOSX"
fi
echo "=== test qrels dates ==="
ls "$QRELS_DEST/longeval_web_qrels" || true

echo "=== extracted size ==="
du -sh "$CACHE"/longeval-web || true
echo "=== top-level layout ==="
find "$CACHE"/longeval-web -maxdepth 3 -type d | head -50
EOF
