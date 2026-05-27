#!/bin/bash
#SBATCH -Jlongeval-irds-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=8G
#SBATCH -t720 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

# ir_datasets LongEval-Web cache -> partitioned parquet. Per-snapshot Luigi
# tasks are idempotent (a `_markers/<date>.done` marker per date), so a job that
# hits the walltime can simply be resubmitted to continue. CPU only.
#
# Smoke first with --only-date on one train + one test date, e.g.:
#   ... irds-parquet ~/scratch/longeval/2025/parquet --only-date 2023-01
#   ... irds-parquet ~/scratch/longeval/2025/parquet --only-date 2023-03
# then drop --only-date for the full 15-date run.

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
source ~/scratch/longeval/app/.venv/bin/activate

scratch_dir=$(realpath ~/scratch/longeval)
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp

longeval etl irds-parquet \
    $scratch_dir/2025/parquet \
    --cache $scratch_dir/ir_datasets \
    --workers 4 \
    ${ONLY_DATE:+--only-date $ONLY_DATE}
EOF
