#!/bin/bash
echo "Starting Apptainer container, run the following command to enter the container:"
echo "source ~/scratch/longeval/app/.venv/bin/activate"
apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash --noprofile --norc
