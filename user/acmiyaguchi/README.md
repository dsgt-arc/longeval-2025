# acmiyaguchi

## pace experiments

For running on PACE.

```bash
salloc \
    -A paceship-dsgt_clef2025 \
    -qinferno --gres=gpu:V100:1 -N1 -n1 --cpus-per-task=4 --mem-per-cpu=4G \
    -t1:00:00
```

```
salloc \
    -A paceship-dsgt_clef2025 \
    -qinferno -N1 -n1 --cpus-per-task=4 --mem-per-cpu=4G \
    -t1:00:00
```

To run a specific batch script:

```bash
$ sbatch slurm-parquet.sbatch
Submitted batch job 2458483
$ sacct -j 2458483
JobID           JobName  Partition    Account  AllocCPUS      State ExitCode
------------ ---------- ---------- ---------- ---------- ---------- --------
2458483      longeval-+  cpu-small paceship-+          8     FAILED      2:0
2458483.bat+      batch            paceship-+          8     FAILED      2:0
2458483.ext+     extern            paceship-+          8  COMPLETED      0:0
2458483.0      hostname            paceship-+          8  COMPLETED      0:0
2458483.1         rsync            paceship-+          8  COMPLETED      0:0
2458483.2         rsync            paceship-+          8  COMPLETED      0:0
2458483.3         unzip            paceship-+          8     FAILED      2:0
2458483.4         unzip            paceship-+          8     FAILED      2:0
2458483.5      longeval            paceship-+          8     FAILED      2:0
```

Can also just run `sacct -u amiyaguchi3` to see jobs that have been run recently.

Figuring out slurm has been kind of tricky with the options for parallelism, as well as trying to get luigi to run on this thing.
The simplest way is to specify a single node with a single task, and to task for multiple cpus.

```bash
#SBATCH -N1 -n1             # Number of nodes and tasks
#SBATCH --cpus-per-task=8   # Number of cores per task
#SBATCH --mem-per-cpu=4G    # Memory per core
```

## copying files over to gcp

```bash
gcloud storage cp -r /storage/home/hcoda1/8/amiyaguchi3/shared/longeval/parquet/ gs://dsgt-longeval-2025/
gcloud storage cp -r /storage/home/hcoda1/8/amiyaguchi3/shared/longeval/embedding/all-MiniLM-L6-v2/ gs://dsgt-longeval-2025/embedding/
```

Do it the smarter way and let slurm deal with it since there are a lot of files. Make sure to omit the trailing slash.

```bash
sbatch gcloud/rsync.sbatch parquet
sbatch gcloud/rsync.sbatch embedding/all-MiniLM-L6-v2
```
