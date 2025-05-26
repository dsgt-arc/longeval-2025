```bash
$ ./scripts/slurm-embed.py
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-train/2023_01/French/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch train/2023_01/French/Documents all-MiniLM-L6-v2
Submitted batch job 2636533
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-train/2023_01/English/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch train/2023_01/English/Documents all-MiniLM-L6-v2
Submitted batch job 2636534
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-test/2023_06/French/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch test/2023_06/French/Documents all-MiniLM-L6-v2
Submitted batch job 2636535
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-test/2023_06/English/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch test/2023_06/English/Documents all-MiniLM-L6-v2
Submitted batch job 2636536
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-test/2023_08/French/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch test/2023_08/French/Documents all-MiniLM-L6-v2
Submitted batch job 2636538
sbatch --job-name=longeval-embed-all-MiniLM-L6-v2-test/2023_08/English/Documents /storage/home/hcoda1/8/amiyaguchi3/clef/longeval-2025/scripts/slurm-embed.sbatch test/2023_08/English/Documents all-MiniLM-L6-v2
sbatch: error: QOSMaxSubmitJobPerUserLimit
sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)
```
