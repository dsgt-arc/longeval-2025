import pytest
from opensearchpy import OpenSearch

OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
INDEX_NAME = "test-index"


@pytest.fixture(scope="module")
def opensearch_client():
    """Provides an OpenSearch client instance."""
    client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
    )
    yield client


# To see the logs, run `pytest tests/opensearch_tests/test_opensearch_healthcheck.py -s`
def test_opensearch_indexing(opensearch_client):
    """Tests indexing, querying BM25, and deletion."""
    settings = {"settings": {"index": {"similarity": {"default": {"type": "BM25"}}}}}
    opensearch_client.indices.create(index=INDEX_NAME, body=settings, ignore=400)

    docs = [
        {"id": 1, "title": "The quick brown fox"},
        {"id": 2, "title": "Jumped over the lazy dog"},
        {"id": 3, "title": "Fast cheetahs are clever"},
    ]
    for doc in docs:
        opensearch_client.index(index=INDEX_NAME, body=doc, id=doc["id"], refresh=True)

    count_resp = opensearch_client.count(index=INDEX_NAME)
    assert count_resp["count"] == 3

    query = {"query": {"match": {"title": "fox"}}}
    search_resp = opensearch_client.search(index=INDEX_NAME, body=query)
    assert len(search_resp["hits"]["hits"]) == 1
    bm25_scores = []
    for hit in search_resp["hits"]["hits"]:
        bm25_scores.append(hit["_score"])
        print(f"Doc ID: {hit['_id']}, BM25 Score: {hit['_score']}")

    opensearch_client.delete_by_query(
        index=INDEX_NAME, body={"query": {"match_all": {}}}
    )
    opensearch_client.indices.refresh(index=INDEX_NAME)

    count_resp_after_delete = opensearch_client.count(index=INDEX_NAME)
    assert count_resp_after_delete["count"] == 0

    opensearch_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
