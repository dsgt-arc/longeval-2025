#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=4 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

mkdir -p ~/scratch/apptainer/cache ~/scratch/longeval/app ~/scratch/longeval/hf_cache
export APPTAINER_CACHEDIR=$(realpath ~/scratch/apptainer/cache)
apptainer build \
    ~/scratch/longeval/app.sif \
    ~/arc/longeval-2025/app.def

# Create the single project venv at ~/scratch/longeval/app/.venv (the --cwd),
# editable-installed from the read-only code dir. Every other sbatch activates
# this same venv inside apptainer (no second venv at ~/scratch/longeval/.venv).
apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
# Keep uv's download cache OFF the 20 GB home filesystem (torch/pyspark wheels
# blow the home quota); park it on scratch alongside the venv.
export UV_CACHE_DIR=~/scratch/longeval/uv-cache
export XDG_CACHE_HOME=~/scratch/longeval/.cache
mkdir -p "$UV_CACHE_DIR" "$XDG_CACHE_HOME"
uv venv
source .venv/bin/activate
uv pip install -e ~/arc/longeval-2025
python -c "import torch, pyserini, sentence_transformers, ir_datasets_longeval; print('torch', torch.__version__, 'cuda', torch.cuda.is_available())"
EOF
