#!/bin/bash
# A frontend to sbatch to submit a job and tail the log file
set -eu

out=$(sbatch scripts/slurm-test.sbatch)
echo $out
# parse out the job "Submitted batch job 2618424"
jobid=$(echo $out | awk '{print $4}')
echo "$jobid"
# implement backoff
round=0
while ! tail -f Report-$jobid.out; do
    backoff=$((2**$round))
    echo "sleeping $backoff seconds"
    sleep $backoff
    round=$((round+1))
done
