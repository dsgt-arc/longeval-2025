#!/bin/bash
#SBATCH -J gcloud-rsync
#SBATCH --account=paceship-dsgt_clef2025
#SBATCH -N1 -n1 --cpus-per-task=4 --mem-per-cpu=4G
#SBATCH -t120 -qinferno -oReport-%j.out --mail-type=END,FAIL
#SBATCH --mail-user=acmiyaguchi@gatech.edu
set -xeu

# strip the trailing slashes
suffix=${1%/}
source=/storage/coda1/p-dsgt_clef2025/0/shared/longeval
remote=gs://dsgt-longeval-2025
gcloud storage rsync -r $source/$suffix/ $remote/$suffix/
