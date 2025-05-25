from functools import cache

import pytrec_eval
from pyserini.search.lucene import LuceneSearcher
from pyspark.sql import functions as F

from longeval.spark import get_spark


@cache
def get_searcher(index_path: str) -> LuceneSearcher:
    """Gets or creates a cached LuceneSearcher instance for the index path."""
    searcher = LuceneSearcher(index_path)
    searcher.set_language("fr")
    return searcher


def _search_worker(qid: str, query: str, index_path: str, k: int) -> dict:
    """Worker function for parallel search. Uses a cached searcher."""
    searcher = get_searcher(index_path)
    hits = searcher.search(query, k)
    total_hits = len(hits)
    max_score = max(hit.score for hit in hits) if total_hits > 0 else 0.0
    return {
        "qid": qid,
        "hits": {
            "total": {"value": total_hits, "relation": "eq"},
            "max_score": max_score,
            "hits": [
                {
                    "_index": index_path,
                    "_id": hit.docid,
                    "_score": hit.score,
                }
                for hit in hits
            ],
        },
    }


def run_search(queries, index_path: str, k=100):
    pdf = queries.toPandas()

    tasks = [(row.qid, row.query, index_path, k) for row in pdf.itertuples()]

    results_list = []
    for task in tasks:
        qid, query, index_path, k = task
        result = _search_worker(qid, query, index_path, k)
        results_list.append(result)

    # Schema remains the same as it now expects the real index_path
    schema = """
        qid: string,
        hits: struct<
            total: struct<value: long, relation: string>,
            max_score: double,
            hits: array<struct<_index: string, _id: string, _score: double>>
        >
    """
    resp = (
        get_spark()
        .createDataFrame(results_list, schema=schema)
        .select("qid", "hits.*")
        .withColumn("total", F.col("total.value"))
    )

    # note that we drop a lot of the information we're not using here
    return resp.select("qid", F.posexplode("hits").alias("rank", "hits")).select(
        "qid",
        "rank",
        F.col("hits._id").alias("docid"),
        F.col("hits._score").alias("score"),
    )


def score_search(
    retrieval,
    qrels,
    scores={"ndcg", "ndcg_rel", "ndcg_cut_10", "map"},
):
    # retrieval should have qid, docid, score (higher score is better)
    run_df = (
        retrieval.join(qrels.select("qid", "docid", "rel"), on="qid", how="left")
        .fillna(0)
        .orderBy("qid", "docid", "rel")
    )

    # now convert to the format required by trec_eval
    qrel = {}
    run = {}
    for row in run_df.collect():
        if row.qid not in qrel:
            qrel[row.qid] = {}
        if row.qid not in run:
            run[row.qid] = {}
        qrel[row.qid][row.docid] = row.rel
        run[row.qid][row.docid] = row.score

    evaluator = pytrec_eval.RelevanceEvaluator(qrel, scores)
    evals = evaluator.evaluate(run)

    return get_spark().createDataFrame([{"qid": k, **v} for k, v in evals.items()])
