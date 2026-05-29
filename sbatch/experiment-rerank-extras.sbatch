#!/bin/bash
#SBATCH -Jlongeval-rerank-extras --account=paceship-dsgt_clef2025 --qos=embers
#SBATCH --nodes=1 -p gpu-v100 --gres=gpu:1 --cpus-per-task=12 --mem-per-cpu=8G
#SBATCH -t02:30:00 -oReport-%A_%a.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-14
set -xeu

# Extra rerank models — issue #37 phase 2. Runs the 1k 3-seed protocol for
# three more arms (camembert-large, camemberta-L10, bge-v2-m3) into the SAME
# ~/scratch/longeval/2025/rerank dir as the original sweep so the aggregator
# yields one 5-arm table.
#
# Throughput estimates (V100 fp16, batch=32 — half of the base sweep because
# 335M / 568M weights leave less room for activations):
#   camemberta-L10 ~480 pairs/s, camembert-large ~220, bge-v2-m3 ~200
#   per-date 1k 3-seed combined ≈ 55–70 min; 2022-08 (biggest) ≈ 70 min.
# -t02:30:00 gives 2x margin under the 6h embers cap.
#
# 2022-06/07/08 also need --mem-per-cpu=16G — submit those three with
#   sbatch --array=0,1,2 --mem-per-cpu=16G sbatch/experiment-rerank-extras.sbatch

DATES=( 2022-06 2022-07 2022-08 2022-09 2022-10 2022-11 2022-12 2023-01 2023-02 \
        2023-03 2023-04 2023-05 2023-06 2023-07 2023-08 )
TARGET_DATE="${DATES[$SLURM_ARRAY_TASK_ID]}"
echo "rerank-extras date: $TARGET_DATE"

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

# repo .env hardcodes NixOS paths (load_dotenv override=False); override per
# the original rerank sbatch — same comment as experiment-rerank.sbatch.
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

scratch_dir=$(realpath ~/scratch/longeval)
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
export HF_HOME=$scratch_dir/hf_cache
# Models are pre-staged in HF_HOME by the login-node smoke-load; xet was
# slow/flaky, use plain https.
export HF_HUB_DISABLE_XET=1

# batch=32 keeps activations safe for the 335M / 568M models on V100 16GB.
# bm25 is omitted (already done by the base sweep); skipping it here also
# saves the no-op result-write.
python ~/arc/longeval-2025/scripts/rerank-eval.py \
    --date "$TARGET_DATE" \
    --root $scratch_dir/2025/parquet \
    --bm25-root $scratch_dir/2025/bm25 \
    --out $scratch_dir/2025/rerank \
    --models "antoinelouis/crossencoder-camembert-large-mmarcoFR,antoinelouis/crossencoder-camemberta-L10-mmarcoFR,BAAI/bge-reranker-v2-m3" \
    --seeds 42,1,2 \
    --sample-queries 1000 \
    --batch-size 32 \
    --fp16
EOF
