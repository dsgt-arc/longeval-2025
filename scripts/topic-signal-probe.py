"""Topic-signal probe: do query<->doc LDA topic similarities discriminate
relevant from non-relevant documents within the qrels-judged set?

Gate experiment before committing compute to a high-K (e.g. K=200) LDA: if the
existing K=4 / K=20 topic cosine cannot separate relevant from non-relevant
judged docs, a finer K (more, narrower topics of the same bag-of-words signal)
won't rescue it. If it shows separation — and especially if K=20 > K=4 — then
finer K is worth training.

For each judged (qid, docid, rel) in the qrels we compute cos(theta_query,
theta_doc) and report: mean cosine by relevance grade, pooled AUC (rel>0 vs
rel=0), and macro per-query AUC. Run for both K=4 and K=20.
"""

import glob
import os

import numpy as np
import pandas as pd
import typer
from pyspark.sql import functions as F
from sklearn.metrics import roc_auc_score

from longeval.spark import spark_resource

app = typer.Typer()

TRAIN_QRELS = ("/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
               "French/LongEval Train Collection/qrels")
TEST_QRELS = os.path.expanduser(
    "~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels")
DOCTD = {
    4:  "/mnt/data/tmp/lda-k4-converged/k4/docTopicDistribution_lda.parquet",
    20: "/mnt/data/tmp/lda-k20-converged/k20/docTopicDistribution_lda.parquet",
}
QTD = {
    4:  "/mnt/data/tmp/lda-queries/k4_query_topics.parquet",
    20: "/mnt/data/tmp/lda-queries/k20_query_topics.parquet",
}


def _qrels(spark, date):
    if date <= "2023-02":
        path = f"{TRAIN_QRELS}/{date}_fr/qrels_processed.txt"
    else:
        path = f"{TEST_QRELS}/{date}/qrels_processed.txt"
    return (
        spark.read.csv(path, sep=" ", schema="qid STRING, rank INT, docid STRING, rel INT")
        .select("qid", "docid", "rel")
    )


@app.command()
def main(dates: str = typer.Option("2023-01,2023-08")):
    date_list = [d.strip() for d in dates.split(",")]
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        qrels = None
        for d in date_list:
            q = _qrels(spark, d).withColumn("date", F.lit(d))
            qrels = q if qrels is None else qrels.union(q)
        qrels = qrels.cache()

        for k in (4, 20):
            tcols = [f"topic_{i}" for i in range(k)]
            # doc theta: strip "doc" prefix, dedup by docid (corpus is
            # deduped within-date not globally; same text -> ~same theta).
            doc = (
                spark.read.parquet(DOCTD[k])
                .withColumn("docid", F.regexp_replace("docid", "^doc", ""))
                .dropDuplicates(["docid"])
                .select("docid", F.array(*tcols).alias("dtd"))
            )
            # query theta (non-empty only)
            qtd = (
                spark.read.parquet(QTD[k])
                .where(F.col("vocab_hits") > 0)
                .select("qid", F.col("td").alias("qtd"))
            )
            joined = (
                qrels.join(qtd, "qid", "inner")
                .join(doc, "docid", "inner")
                .select("qid", "rel", "qtd", "dtd")
            )
            pdf = joined.toPandas()
            Q = np.vstack(pdf["qtd"].values).astype(float)
            D = np.vstack(pdf["dtd"].values).astype(float)
            # cosine
            num = (Q * D).sum(1)
            den = np.linalg.norm(Q, axis=1) * np.linalg.norm(D, axis=1)
            cos = np.where(den > 0, num / den, 0.0)
            pdf["cos"] = cos
            pdf["pos"] = (pdf["rel"] > 0).astype(int)

            print(f"\n==== K={k}  (judged pairs after join: {len(pdf):,}) ====")
            print("mean topic-cosine by relevance grade:")
            for g, sub in sorted(pdf.groupby("rel")):
                print(f"  rel={g}: n={len(sub):>7,}  mean_cos={sub['cos'].mean():.4f}")
            # pooled AUC
            if pdf["pos"].nunique() == 2:
                pooled = roc_auc_score(pdf["pos"], pdf["cos"])
            else:
                pooled = float("nan")
            # macro per-query AUC (queries with both classes)
            aucs = []
            for qid, sub in pdf.groupby("qid"):
                if sub["pos"].nunique() == 2:
                    aucs.append(roc_auc_score(sub["pos"], sub["cos"]))
            macro = np.mean(aucs) if aucs else float("nan")
            print(f"pooled AUC (rel>0 vs rel=0)         : {pooled:.4f}")
            print(f"macro per-query AUC (n_q={len(aucs)}) : {macro:.4f}")
            print("  (0.5 = no signal; >0.5 = topic cosine ranks relevant higher)")

        qrels.unpersist()


if __name__ == "__main__":
    app()
