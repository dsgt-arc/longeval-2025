#!/bin/bash
#SBATCH -Jlongeval-rerank --account=paceship-dsgt_clef2025 --qos=embers
#SBATCH --nodes=1 -p gpu-v100 --gres=gpu:1 --cpus-per-task=12 --mem-per-cpu=8G
#SBATCH -t04:00:00 -oReport-%A_%a.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-14
set -xeu

# One array task per date (15 LongEval-Web snapshots). Seeds (42,1,2) and arms
# (bm25, camembert-base, jina-v2) loop inside rerank-eval.py so the per-date
# Spark candidate prep runs once. Per-(date,seed,model) _SUCCESS markers make
# this resumable — resubmit after a timeout to continue.

DATES=( 2022-06 2022-07 2022-08 2022-09 2022-10 2022-11 2022-12 2023-01 2023-02 \
        2023-03 2023-04 2023-05 2023-06 2023-07 2023-08 )
TARGET_DATE="${DATES[$SLURM_ARRAY_TASK_ID]}"
echo "rerank date: $TARGET_DATE"

apptainer exec \
    --nv \
    --writable-tmpfs \
    --cleanenv \
    --env SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID \
    --env TARGET_DATE=$TARGET_DATE \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
source ~/scratch/longeval/app/.venv/bin/activate

# The repo .env hardcodes NixOS paths (load_dotenv override=False, so env wins):
#  - JAVA_HOME/JVM_PATH: the container's OpenJDK 21, for Spark candidate prep.
#  - SSL_CERT_FILE/REQUESTS_CA_BUNDLE point at /etc/ssl/certs/ca-bundle.crt
#    (RHEL/Nix naming) which does not exist on Ubuntu — the HF model fetch would
#    fail with CERTIFICATE_VERIFY_FAILED. Ubuntu's bundle is ca-certificates.crt.
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

scratch_dir=$(realpath ~/scratch/longeval)
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
export HF_HOME=$scratch_dir/hf_cache
# Models are pre-staged in HF_HOME; xet was slow/flaky, so use plain https.
export HF_HUB_DISABLE_XET=1

python ~/arc/longeval-2025/scripts/rerank-eval.py \
    --date "$TARGET_DATE" \
    --root $scratch_dir/2025/parquet \
    --bm25-root $scratch_dir/2025/bm25 \
    --out $scratch_dir/2025/rerank \
    --seeds 42,1,2 \
    --sample-queries 1000 \
    --batch-size 64 \
    --fp16
EOF
