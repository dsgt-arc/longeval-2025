# overview

This document describes the main pieces needed to develop in this repository: data preparation, BM25 retrieval with Pyserini/Anserini, evaluation, and dense embedding experiments.

## system overview

### parquet data preparation

The raw LongEval collections are normalized into parquet with the Spark-based ETL workflow:

```bash
longeval etl parquet <raw-input-path> <parquet-output-path>
```

The parquet output is the common input for retrieval, evaluation, and embedding jobs.

### BM25 retrieval with Pyserini/Anserini

BM25 experiments are run through Pyserini, which wraps Anserini/Lucene. This keeps the retrieval pipeline file-based and suitable for PACE/SLURM jobs without a long-running search service.

The main workflow is implemented in:

- `longeval/experiment/bm25/workflow.py`
- `longeval/experiment/bm25/retrieval.py`
- `longeval/experiment/bm25/evaluation.py`

A PACE batch example is available at:

```bash
sbatch/experiment-bm25.sbatch
```

The workflow exports collection documents to JSONL, builds Lucene indices with `python -m pyserini.index.lucene`, runs retrieval, and writes parquet outputs for downstream scoring/reranking.

### evaluation

We evaluate retrieval outputs against qrels with `pytrec_eval`. The evaluation code consumes parquet/csv retrieval outputs, converts them into TREC-style mappings, and writes per-query scores.

Relevant files:

- `longeval/experiment/bm25/evaluation.py`
- `longeval/experiment/evaluate/workflow.py`

### dense embeddings

Dense embedding experiments use Spark plus Hugging Face / sentence-transformers models to map queries and documents into vector representations.

Relevant files:

- `longeval/etl/embedding/workflow.py`
- `longeval/etl/embedding/ml.py`

We use the PACE cluster for large embedding jobs, then copy generated parquet outputs to shared storage or GCS for later analysis.
