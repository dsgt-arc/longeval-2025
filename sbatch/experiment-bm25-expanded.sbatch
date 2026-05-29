#!/bin/bash
#SBATCH -Jlongeval-bm25-exp --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --ntasks=1 --cpus-per-task=24 --mem-per-cpu=6G
#SBATCH -t600 -qinferno -oReport-%j.out
#SBATCH --mail-type=END,FAIL --mail-user=acmiyaguchi@gatech.edu
#SBATCH --array=0-3%4
set -xeu

# BM25 retrieval over french-additive expanded queries.
# Original retrieval/ already exists from the prior issue-#37 run; this only
# runs the --expanded-only arm and writes retrieval_expanded/date=*/sample_id=*.
# Expansion JSONs (additive: `original + original + terms...`) live at
# ~/scratch/longeval/2025/query_expansion/french/expansion/{start}-{end}.json.

apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --env SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash << 'EOF'
set -xeu
hostname
source ~/scratch/longeval/app/.venv/bin/activate

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export JVM_PATH=$JAVA_HOME/lib/server/libjvm.so
export OPENAI_API_KEY=dummy
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

python -m pyserini.index.lucene -options

scratch_dir=$(realpath ~/scratch/longeval)

export PYSPARK_DRIVER_CORES=24
export PYSPARK_DRIVER_MEMORY=$(($PYSPARK_DRIVER_CORES*6*3/4))g
export SPARK_LOCAL_DIR=${TMPDIR:-/tmp}/spark-tmp

python -m longeval.experiment.bm25.workflow run-bm25 \
    $scratch_dir/2025/parquet \
    $scratch_dir/2025/bm25 \
    --scratch-path $scratch_dir/bm25 \
    --parallelism $PYSPARK_DRIVER_CORES \
    --workers 12 \
    --num-sample-ids 4 \
    --sample-id $SLURM_ARRAY_TASK_ID \
    --with-expanded \
    --expanded-only \
    --expanded-path $scratch_dir/2025/query_expansion/french/expansion
EOF
