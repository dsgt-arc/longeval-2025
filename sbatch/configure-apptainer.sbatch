#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=4 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

mkdir -p ~/scratch/apptainer/cache ~/scratch/longeval/app ~/scratch/longeval/hf_cache
export APPTAINER_CACHEDIR=$(realpath ~/scratch/apptainer/cache)
apptainer build --force \
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
# torch is pulled in transitively (no pin in pyproject), and the default index
# ships a CUDA 13 build (cu130). PACE's GPU nodes (V100, driver 575.57.08) only
# support up to CUDA 12.9, so cu130 yields torch.cuda.is_available()==False and
# the reranker silently runs on CPU. Re-pin torch to the cu126 (CUDA 12.6) build,
# which the driver supports. torch 2.12.0 publishes only cu126 and cu130 builds
# (no cu128/cu129), so cu126 is the compatible target. Same version, no dep shift.
uv pip install --reinstall-package torch torch==2.12.0+cu126 \
    --index-url https://download.pytorch.org/whl/cu126
# transformers resolves to 5.x by default, whose major refactor dropped the
# XLM-R helpers (create_position_ids_from_input_ids, ...) that the
# jina-reranker-v2 remote code imports -> jina fails to load. Pin to the latest
# 4.x; sentence-transformers 5.x still accepts it and the camembert control is
# unaffected. (issue #37 needs the jina arm.)
uv pip install --reinstall-package transformers "transformers>=4.46,<5"
python -c "import torch, transformers, sentence_transformers, pyserini, ir_datasets_longeval; print('torch', torch.__version__, torch.version.cuda, 'cuda_avail', torch.cuda.is_available(), 'transformers', transformers.__version__)"
EOF
