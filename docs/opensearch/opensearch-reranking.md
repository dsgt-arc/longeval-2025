# OpenSearch Reranking

Reference Links
- https://opensearch.org/docs/latest/search-plugins/search-relevance/rerank-cross-encoder/
- https://opensearch.org/docs/latest/ml-commons-plugin/pretrained-models/#cross-encoder-models
---

## Part 1: Configure a cross-encoder model

Ensure you have OpenSearch docker containers running locally before starting this guide.

### Configure Settings

Note: On models with ML nodes, specify below for improved performance
```
"only_run_on_ml_node": "true"
```
For local setup, can just run the following, 
```curl
PUT _cluster/settings
{
  "persistent": {
    "plugins.ml_commons.only_run_on_ml_node": "false",
    "plugins.ml_commons.model_access_control_enabled": "true",
    "plugins.ml_commons.native_memory_threshold": "99"
  }
}
```
### Register a model group

Run the below command and grab the `model_group_id` from the response.
```
POST /_plugins/_ml/model_groups/_register
{
  "name": "longeval_local_model_group",
  "description": "A model group used for reranking"
}
```
Response should look something like this 
```json
{
  "model_group_id": "yC_IoZUBuE3j1kPsF1_F",
  "status": "CREATED"
}
```
### Register an OpenSearch provided model

Run the below command with the appropriate parameter values.
```
POST /_plugins/_ml/models/_register
{
  "name": "huggingface/cross-encoders/ms-marco-MiniLM-L-6-v2",
  "version": "1.0.2",
  "model_group_id": "yC_IoZUBuE3j1kPsF1_F",
  "model_format": "TORCH_SCRIPT"
}
```
Take note of the response which should look something like this
```json
{
  "task_id": "yi_LoZUBuE3j1kPsLV9D",
  "status": "CREATED"
}
```

Run this command intermittently to check on the progress of the task with its id
```
GET /_plugins/_ml/tasks/yi_LoZUBuE3j1kPsLV9D
```
You should expect to see something like this as a response with state COMPLETED.
```json
{
  "model_id": "zC_LoZUBuE3j1kPsMV-8",
  "task_type": "REGISTER_MODEL",
  "function_name": "TEXT_SIMILARITY",
  "state": "COMPLETED",
  "worker_node": [
    "k3YZn0bwSYGreYOxEfPcnQ"
  ],
  "create_time": 1742176201800,
  "last_update_time": 1742176214146,
  "is_async": true
}
```
### Deploy the model

The deploy operation reads the model’s chunks from the model index and then creates an instance of the model to load into memory.

```curl
POST /_plugins/_ml/models/zC_LoZUBuE3j1kPsMV-8/_deploy
```
After running this command, note the task_id in the response
```json
{
  "task_id": "zS_ToZUBuE3j1kPsxV8u",
  "task_type": "DEPLOY_MODEL",
  "status": "CREATED"
}
```
You can check the status of the task similarly as before
```
GET /_plugins/_ml/tasks/zS_ToZUBuE3j1kPsxV8u
```
Response should look something like this
```json
{
  "model_id": "zC_LoZUBuE3j1kPsMV-8",
  "task_type": "DEPLOY_MODEL",
  "function_name": "TEXT_SIMILARITY",
  "state": "COMPLETED",
  "worker_node": [
    "k3YZn0bwSYGreYOxEfPcnQ"
  ],
  "create_time": 1742176765221,
  "last_update_time": 1742176779009,
  "is_async": true
}
```

To test the cross-encoder model, run teh following curl with the model id and ensure the model calculates the similarity score of query_text and each document in text_docs and returns a list of scores for each document in the order they were provided in text_docs:

```commandline
POST _plugins/_ml/models/zC_LoZUBuE3j1kPsMV-8/_predict
{
    "query_text": "today is sunny",
    "text_docs": [
        "how are you",
        "today is sunny",
        "today is july fifth",
        "it is winter"
    ]
}
```
---

## Part 2: Setup Reranking

Ensure you have an index where your documents are stored to test the reranking functionality before proceeding.

### Configure a reranking pipeline

Use the model_id from the previous request in this curl command.

```
PUT /_search/pipeline/longeval_rerank_pipeline
{
  "description": "Pipeline for reranking with a cross-encoder",
  "response_processors": [
    {
      "rerank": {
        "ml_opensearch": {
          "model_id": "zC_LoZUBuE3j1kPsMV-8"
        },
        "context": {
          "document_fields": [
            "contents"
          ]
        }
      }
    }
  ]
}
```

### Update an existing index for ingestion

```
PUT /long_eval_dev_docs/_settings
{
  "index.search.default_pipeline": "longeval_rerank_pipeline"
}
```

### Search using re-ranking

The below query is modified for longeval usage and caps results to 10.

```
POST /long_eval_dev_docs/_search
{
  "query": {
    "match": {
      "contents": "The orange house is dangerous"
    }
  },
  "ext": {
    "rerank": {
      "query_context": {
         "query_text": "the orange house is dangerous"
      },
      "size": 10  
    }
  }
}
```
