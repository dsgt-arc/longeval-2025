# overview

This document will describe many of the overall pieces you will need to get started developing on this repository.
This will include general organization of code, data, and systems where these things need to be run.
We also cover the general approach to solving the problem and point to prior art where necessary.

## system overview

### opensearch

We use opensearch (a fork of elasticsearch) to store and query the datasets.
The opensearch instance is run inside of a docker container which is bind-mounted to the host machine.
We bind-mount since this allows us to easily persist the data between runs and to inspect the data in the container, as well as providing a snapshotting mechanism that is out of band from opensearch itself.

We load data into opensearch using Spark and the `opensearch-hadoop` connector.
We install the jar into the pyspark environment which becomes available to the spark cluster.
We use the opensearch library to setup an index template which allows us to control the number of replicas and shards, as well as the actual mappings of the fields to the types in opensearch.
If we do not have a template, we get a useless default template that is not optimized for our use case.

The same opensearch library is used to query the data.
We can parallelize operations via Spark directly through the spark-sql interface.

We run all of this infrastructure on Google Cloud Platform (GCP) using a shared VM instance.
There are some modifications to the workflow that we must make in order to run this efficiently.
First we note the prices of the various resources we are using.

| resource                | price gb/mo |
| ----------------------- | ----------- |
| regional object storage | $0.02       |
| standard disk           | $0.04       |
| balanced disk           | $0.10       |
| ssd disk                | $0.17       |

First, we synchronize our state from the host machine to object storage (GCS) to save the state of the instance for cost-savings.
We prefer object storage over persistent disk due to monthly cost, as well as free bandwidth between GCS and GCE.
We configure this via snapshot backups and restores to simplify index management.
Thus between the beginning (and generally end) of each session, we need to do the following:

```bash
# bring up the opensearch instance
docker compose up

# start the snapshop restore process
longeval opensearch restore

# do some work
# ...

# start the snapshot backup process (if necessary)
longeval opensearch backup
```

### evaluation

We evaluate the datasets against the relevancy scores in the test set.
We have a set of true-positive relevant (and irrelevant) documents for every query in the `qrels` test dataset.
We query the system and compute a NDCG score over every query.

The first part of our experiments is to benchmark the datasets using the default BM25 algorithm.
This means that we calculate the differential NDCG score between time periods to obtain a shift in the scoring dataset.

### dense embeddings

The second part of our experiments is to obtain a dense embedding of the documents.
We use an embedding model derived from BERT to map queries and documents into a low dimensional latent space that preserves semantic information.
BERT-styled models are autoregressive and are encoder-only models, meaning that these models are designed expressly for efficient mapping of written language to a dense numerical representation.

We use the PACE cluster to embed our datasets and then transfer the data out into GCP to query the data using opensearch.
This allows us the best of both world where we have access to how our opensearch service is run on a VM, and where we can take advantage of raw compute via timesharing facilitated by SLURM.
The code to run the embedding process is handled by a Spark job run on a single node, where resources are allocated via a supercomputer hypervisor layer.
We write a MLLib wrapper around huggingface transformers which accepts a text column and outputs an array of floats.
These datasets will be particularly large given the number of documents per collection.

| collection | model | dataset size (gb) | wallclock (mm:ss) |
| ---------- | ----- | ----------------- | ----------------- |
| TODO       | TODO  | TODO              | TODO              |

The dataset is transferred out of PACE into the GCS bucket, and then ingested into our indices.