# luigi-pace

This is just to figure out how to run luigi on pace while taking advantage of the slurm scheduler.
I want to be able to run a piece of code in parallel, and to be able to use a central scheduler of some kind.

This will run jobs in one of two ways.
The first way is to use the array method: `#SBATCH --array=x-y%4` where x and y are the start and end of the array, and 4 is the number of jobs to run at once.
The second will be to run the jobs in a single job, but to use a central scheduler between the nodes.
This way will let us take advantage of the luigi scheduler instead.

## experiments

### local scheduler

First we run the luigi local scheduler on a vertically scaled node.
We are able to then run 4 workers on a single node that has multiple cpus.

```bash
#SBATCH -N1 -n1                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=2                       # Number of cores per task

sbatch slurm-luigi-local.sbatch
```

The results are found in [logs-luigi-local.txt](logs-luigi-local.txt), most importantly being able to run this job in 37 seconds.

```bash
---------------------------------------
Begin Slurm Epilog: Jan-13-2025 01:53:49
Job ID:        2481218
Array Job ID:  _4294967294
User ID:       amiyaguchi3
Account:       paceship-dsgt_clef2025
Job name:      longeval-luigi-local
Resources:     cpu=2,mem=2G,node=1
Rsrc Used:     cput=00:01:14,vmem=0,walltime=00:00:37,mem=39468K,energy_used=0
Partition:     cpu-small
QOS:           inferno
Nodes:         atl1-1-02-003-16-1
---------------------------------------
```

### distributed execution

We now run luigi across two nodes, but this time having the nodes communicate with each other using the hostname of the entry node for the slurm job.
We can then take advantage of a luigi scheduler to keep track of our job.
It would be preferable to be able to proxy the luigi scheduler to an external address that we can access over the internet, but it would require a bit of setup.

```bash
#SBATCH -N4 -n4                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=1                       # Number of cores per task
```

We run luigi in a background task e.g. `luigid &` and then kick off our workflow:

```bash
srun bash -c "
    source ~/scratch/longeval/venv/bin/activate && \
    set -x && \
    python workflow.py \
        --output-path $DATADIR \
        --scheduler-host $(hostname)
"
```

Results are found in [logs-luigi-dist.txt](logs-luigi-dist.txt), with a total time of 40 seconds.

### slurm array

The final way we run the job is by using a slurm array.

```bash
#SBATCH --array=0-9%4                          # Array range and max jobs
#SBATCH -N1 -n1                                 # Number of nodes and cores required
#SBATCH --cpus-per-task=1                       # Number of cores per task
#SBATCH --mem-per-cpu=1G                        # Memory per core
```

The result here is effectively the same as luigi with the local scheduler, except that we have kicked off 10 jobs in parallel instead of just one.

```bash
$ sacct -j 2495059
JobID           JobName  Partition    Account  AllocCPUS      State ExitCode
------------ ---------- ---------- ---------- ---------- ---------- --------
2495059_0    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_0.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_0.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_0.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_1    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_1.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_1.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_1.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_2    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_2.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_2.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_2.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_3    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_3.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_3.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_3.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_4    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_4.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_4.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_4.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_5    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_5.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_5.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_5.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_6    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_6.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_6.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_6.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_7    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_7.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_7.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_7.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_8    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_8.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_8.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_8.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_9    longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_9.b+      batch            paceship-+          1  COMPLETED      0:0
2495059_9.e+     extern            paceship-+          1  COMPLETED      0:0
2495059_9.0    hostname            paceship-+          1  COMPLETED      0:0
2495059_10   longeval-+  cpu-small paceship-+          1  COMPLETED      0:0
2495059_10.+      batch            paceship-+          1  COMPLETED      0:0
2495059_10.+     extern            paceship-+          1  COMPLETED      0:0
2495059_10.0   hostname            paceship-+          1  COMPLETED      0:0
```

## discussion

These are the three main ways that we can take advantage of parallelism on PACE with slurm and luigi.
The first way is the simplest, but not when the job is preemptible.
We might use this for basic computation that we know can be done on a single node.
A centralized scheduler might have its use cases, but the scripting logic is a bit convoluted.
The use of slurm array creates a job per subtask, and this is likely the best way to handle a preemptible job. Since our jobs are going to be broken up into chunks anyways, we can simply have a job per chunk and let the slurm scheduler batch things up in a way that we can walk away.

One of the things that I would like more information about is whether pre-empted jobs are retried or not, but luigi handles some degree of idemopotency for us.
