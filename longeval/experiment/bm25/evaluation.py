import pytrec_eval
from pyspark.sql import functions as F

from longeval.spark import get_spark


def score_search(
    retrieval,
    qrels,
    scores={"ndcg", "ndcg_rel", "ndcg_cut_10", "map"},
):
    # retrieval should have qid, docid, score (higher score is better)
    #
    # LongEval document DOCNOs carry a `doc` prefix (e.g. `doc14290`) so the
    # collection, BM25 index, and retrieval results all use prefixed ids — but
    # the qrels reference the bare numeric id (`14290`). Without normalizing the
    # qid+docid join below never lands, every `rel` becomes 0, and nDCG
    # collapses to 0. Strip a leading `doc` (only when followed by a digit, so
    # genuinely non-numeric ids are untouched) on both sides before joining.
    strip = lambda df: df.withColumn(
        "docid", F.regexp_replace(F.col("docid"), r"^doc(\d)", r"$1")
    )
    retrieval = strip(retrieval)
    qrels = strip(qrels)
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
