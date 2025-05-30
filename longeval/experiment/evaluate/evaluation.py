import pytrec_eval
import tqdm
from longeval.spark import get_spark
from pyspark.sql import functions as F


def _run_df(retrieval, qrels):
    # retrieval should have qid, docid, score (higher score is better)
    run_df = (
        # retrieval has doc{id} whereas qrel just has id
        retrieval.withColumn(
            "docid", F.replace(F.col("docid"), F.lit("doc"), F.lit("")).astype("int")
        )
        .join(
            qrels.select("qid", "docid", "rel"),
            on=["qid", "docid"],
            how="left",
        )
        .fillna(0)
        .orderBy("qid", "docid", "rel")
    )
    return run_df


def _prep_run(run_df):
    qrel = {}
    run = {}
    for row in tqdm.tqdm(run_df.collect()):
        qid = str(row.qid)
        docid = str(row.docid)
        if qid not in qrel:
            qrel[qid] = {}
        if qid not in run:
            run[qid] = {}
        qrel[qid][docid] = row.rel
        run[qid][docid] = row.score
    return qrel, run


def score_search(
    retrieval,
    qrels,
    scores={"ndcg", "ndcg_rel", "ndcg_cut_10", "map"},
):
    # retrieval should have qid, docid, score (higher score is better)
    run_df = _run_df(retrieval, qrels)
    # now convert to the format required by trec_eval
    qrel, run = _prep_run(run_df)
    evaluator = pytrec_eval.RelevanceEvaluator(qrel, scores)
    evals = evaluator.evaluate(run)

    return get_spark().createDataFrame([{"qid": k, **v} for k, v in evals.items()])
