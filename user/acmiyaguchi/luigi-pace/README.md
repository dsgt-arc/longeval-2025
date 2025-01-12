# luigi-pace

This is just to figure out how to run luigi on pace while taking advantage of the slurm scheduler.
I want to be able to run a piece of code in parallel, and to be able to use a central scheduler of some kind.

This will run jobs in one of two ways.
The first way is to use the array method: `#SBATCH --array=x-y%4` where x and y are the start and end of the array, and 4 is the number of jobs to run at once.

The second will be to run the jobs in a single job, but to use a central scheduler between the nodes.
This way will let us take advantage of the luigi scheduler instead.
