import pytrec_eval
from longeval.spark import get_spark


def score_search(
    retrieval,
    qrels,
    scores={"ndcg", "ndcg_rel", "ndcg_cut_10", "map"},
):
    # retrieval should have qid, docid, score (higher score is better)
    run_df = (
        retrieval.join(
            qrels.select("qid", "docid", "rel"),
            on=["qid", "docid"],
            how="left",
        )
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
