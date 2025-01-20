# LongEval 2025 Quickstart Guide

Code for the DS@GT CLEF 2025 LongEval team.
Refer to [docs](docs/) for more information.

## Prerequisites

If you haven't already, install mvn.
This might take a while depending on the strength of your internet connection.
```bash
brew install maven
```

Install the package into your environment:
```bash
pip install -e .

# install pyspark connectors
./scripts/spark-jars.sh
```

This will add a command line tool `longeval` to your environment.

Populate your local .env file before proceeding. 
For the password variable, you can check the strength of your password [at this website](https://lowe.github.io/tryzxcvbn/).

Run `docker-compose up` and ensure you can view the OpenSearch Dashboard properly by navigating here: http://localhost:5601/app/home#/


## Healthcheck OpenSearch 

To validate OpenSearch is working properly, you can instantiate an OpenSearch object with the port 9200.

```python
client = OpenSearch(
    hosts="http://localhost:9200",
)
```

Then you may establish a template insertion pattern using the `put_template` function.
```python
client.indices.put_template(
    name="default",
    body={
        "index_patterns": ["*"],
        "settings": {
            "number_of_replicas": 0,
            "number_of_shards": 4,
        },
        "mappings": {
            "properties": {
                "contents": {
                    "type": "text",
                },
                "docid": {
                    "type": "keyword",
                },
            },
        },
    },
)
```

You can insert documents using the `index` function

```python
index_name = "animal_index"  

doc_1 = {
    "contents": "I am a dog. I like the color green.",
    "docid": "111"
}

response = client.index(
    index=index_name,
    body=doc_1,
    id="1"  
)

print(response)
```

If you navigte to the OpenSearch Dashboard on your localhost and click the left menu bar -> Dev Tools, you can enter console queries such as 
```
GET /animal_index/_doc/1
```
to view the document you just inserted and ones like below

```
GET /animal_index/_search
{
  "query": {
    "match": {
      "contents": "like"
    }
  }
}
```
to perform basic search when your index contains multiple documents.
You can observe the best BM25 score by finding the `max_score` value in the response.

