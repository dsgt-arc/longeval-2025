from longeval.spark import get_spark
from pyspark.sql import functions as F, Window
import pytrec_eval
from pyserini.search.lucene import LuceneSearcher
from functools import cache  # Import cache


@cache
def get_searcher(index_path: str) -> LuceneSearcher:
    """Gets or creates a cached LuceneSearcher instance for the index path."""
    searcher = LuceneSearcher(index_path)
    searcher.set_language("fr")
    return searcher


def prepare_queries(collection):
    return collection.queries.join(
        collection.qrels.groupBy("qid", "date").agg(
            F.collect_set(F.struct("docid", "rel")).alias("qrel")
        ),
        on="qid",
    ).select("date", "qid", "query", "qrel")


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


def run_search(queries, index_path: str, k=100, parallelism=4) -> list[dict]:
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

    window = Window.partitionBy("qid").orderBy("pos")
    return (
        resp.select(
            "qid", "total", "max_score", F.posexplode("hits").alias("pos", "hits")
        )
        .withColumn("docids", F.collect_list(F.col("hits._id")).over(window))
        .withColumn("scores", F.collect_list(F.col("hits._score")).over(window))
        .groupBy("qid")
        .agg(
            F.any_value("total").alias("total"),
            F.any_value("max_score").alias("max_score"),
            F.max("docids").alias("docids"),
            F.max("scores").alias("scores"),
        )
        .join(queries.select("qid", "qrel"), on="qid")
    )


def score_search(
    search_df,
    scores={"ndcg", "ndcg_rel", "ndcg_cut_10", "map"},
):
    @F.udf("map<string, float>")
    def run_udf(docids: list[str], scores: list[float]) -> dict[str, float]:
        return {k: v for k, v in zip(docids, scores)}

    @F.udf("map<string, int>")
    def qrel_udf(qrel: list[dict]) -> dict[str, int]:
        return {v["docid"]: v["rel"] for v in qrel}

    # for each qid, we need a mapping of docid to relevance
    run_df = search_df.select(
        "qid",
        run_udf("docids", "scores").alias("run"),
        qrel_udf("qrel").alias("qrel"),
    )

    # now convert to the format required by trec_eval
    qrel = {}
    run = {}
    for row in run_df.collect():
        qrel[row.qid] = row.qrel
        run[row.qid] = row.run

    evaluator = pytrec_eval.RelevanceEvaluator(qrel, scores)
    evals = evaluator.evaluate(run)

    return get_spark().createDataFrame([{"qid": k, **v} for k, v in evals.items()])
