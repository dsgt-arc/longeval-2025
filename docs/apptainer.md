# apptainer

Turns out that PACE doesn't have java 21 installed, which makes it hard to run anserini (which uses java 21). We have to use apptainer, which is yet another new tool to learn.

I've created a `app.def` with java dependencies.

```
export APPTAINER_CACHEDIR=$(realpath ~/scratch/apptainer/cache)
mkdir -p ~/scratch/apptainer/cache

apptainer build ~/scratch/longeval/app.sif ~/clef/longeval-2025/app.def
apptainer exec ~/scratch/longeval/app.sif bash
apptainer exec \
    --writable-tmpfs \
    --cleanenv \
    --cwd ~/scratch/longeval/app \
    ~/scratch/longeval/app.sif \
    bash
```

After getting the image to run, we build a venv and can get most of the anserini things working.
This actually works quite well, it's almost as-if I were working inside a ubuntu host (albeit, without sudo or root).
But given that my whole home directory is mounted, the experience between apptainer and the host is seamless.
I do probably have to think about some of the environment variables that are setup though, since packages are likely still cached into my home directory and not into scratch as I hope.
