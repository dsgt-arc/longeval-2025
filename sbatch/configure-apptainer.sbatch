#!/bin/bash
#SBATCH -Jlongeval-parquet --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=4 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
set -xeu

export APPTAINER_CACHEDIR=$(realpath ~/scratch/apptainer/cache)
mkdir -p ~/scratch/apptainer/cache
apptainer build \
    ~/scratch/longeval/app.sif \
    ~/clef/longeval-2025/app.def

# now install the poetry dependencies
apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
uv venv
source .venv/bin/activate
uv pip install -e ~/clef/longeval-2025
EOF
