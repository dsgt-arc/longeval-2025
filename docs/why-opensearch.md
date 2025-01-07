# why opensearch

Opensearch is a fork of Elasticsearch which occured due to changes in product licensing.
It provides an API over lucene, and provides the foundation for a production-quality system.
We choose Opensearch to obtain operational experience with end-to-end search pipelines particularly focused on evaluation.
Opensearch (and Elasticsearch) also provides a toolset that is particularly accomodating to vector search problems e.g. retrieval-augmented generation (RAG) applications that use the latent-space of large language models (LLMs) to encode knowledge and to find similar documents.

There are alternatives to Opensearch that we could use that implement both term-based search (e.g. BM25) and vector search (e.g. approximate k nearest neighbors).
These libraries provide similar semantics to Opensearch, in that they must construct indices, provide persistence mechanisms, and be performant for large datasets.
However, many of these libraries require some engineering overhead for building maintaining experimental pipelines.
