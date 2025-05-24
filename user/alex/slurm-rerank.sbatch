#!/bin/bash
#SBATCH --job-name=longeval-embed --account=paceship-dsgt_clef2025
#SBATCH --nodes=1 --gres=gpu:V100:1 --cpus-per-task=12 --mem-per-cpu=8G   
#SBATCH --qos=inferno -t600     # set qos=embers for free gpu jobs
#SBATCH --output=Report-%j.out
##SBATCH --mail-type=END,FAIL --mail-user=apramov3@gatech.edu


#source ~/scratch/longeval/.venv/bin/activate
source scripts/activate

cd ~/github/longeval-2025/
~/scratch/longeval/.venv/bin/python ~/github/longeval-2025/user/alex/run_reranking.py
 