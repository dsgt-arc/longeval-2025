#!/bin/bash
#SBATCH -Jlongeval-rerank-smoke --account=paceship-dsgt_clef2025 --qos=embers
#SBATCH --nodes=1 -p gpu-v100 --gres=gpu:1 --cpus-per-task=12 --mem-per-cpu=8G
#SBATCH -t00:45:00 -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

# GATE 4-smoke: validate the full join+qrels+model+score chain end-to-end on one
# date with only the control cross-encoder + one seed. Pass condition: 2023-01
# control nDCG@10 ~= 0.377 (matches the prior worklog). Writes into the real
# rerank output dir so the cell is reused (idempotent) by the full sweep.

apptainer exec \
    --nv \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
source ~/scratch/longeval/app/.venv/bin/activate

# Same .env-override block as experiment-rerank.sbatch (NixOS paths -> container).
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

scratch_dir=$(realpath ~/scratch/longeval)
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp
export HF_HOME=$scratch_dir/hf_cache
# Models are pre-staged in HF_HOME; xet was slow/flaky, so use plain https.
export HF_HUB_DISABLE_XET=1

nvidia-smi
python -c "import torch; print('cuda_avail', torch.cuda.is_available(), torch.version.cuda)"
python ~/arc/longeval-2025/scripts/rerank-eval.py \
    --date 2023-01 \
    --root $scratch_dir/2025/parquet \
    --bm25-root $scratch_dir/2025/bm25 \
    --out $scratch_dir/2025/rerank \
    --models antoinelouis/crossencoder-camembert-base-mmarcoFR \
    --seeds 42 \
    --sample-queries 1000 \
    --batch-size 64 \
    --fp16
EOF
