from longeval.spark import get_spark
from pyspark.sql import functions as F, Window
from opensearchpy import OpenSearch
import pytrec_eval


def _generate_bulk_query(df, index_name: str, k: int = None) -> list[dict]:
    data = []
    for row in df.itertuples():
        data += [
            {
                "index": index_name,
            },
            {
                "query": {
                    "match": {
                        "contents": {
                            "query": row.query,
                        }
                    }
                },
                "_source": False,
                **({"size": k} if k else {}),
            },
        ]
    return data


def prepare_queries(collection):
    return collection.queries.join(
        collection.qrels.groupBy("qid").agg(
            F.collect_set(F.struct("docid", "rel")).alias("qrel")
        ),
        on="qid",
    ).select("qid", "query", "qrel")


def run_search(queries, index_name: str, k=100, host="localhost:9200") -> list[dict]:
    client = OpenSearch(host, timeout=120)

    pdf = queries.toPandas()
    results = client.msearch(_generate_bulk_query(pdf, index_name, k))
    for row, obj in zip(pdf.itertuples(), results["responses"]):
        obj["qid"] = row.qid

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
        .createDataFrame(results["responses"], schema=schema)
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
    def run_udf(docids: list[str], scores: list[float]) -> dict[str, int]:
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
