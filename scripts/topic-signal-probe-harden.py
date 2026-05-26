"""Hardening checks for the topic-signal probe, per pi-review:

(A) dedup-divergence: is dropDuplicates(["docid"]) on doc theta sound? Quantify
    how often a docid recurs and how much theta diverges across its copies. If
    copies are near-identical, the date-blind join is harmless.
(B) Hellinger similarity (the natural simplex geometry) re-score, alongside
    cosine, so the ~0.5 null isn't an artifact of the wrong metric.
"""

import os

import numpy as np
import typer
from pyspark.sql import functions as F
from sklearn.metrics import roc_auc_score

from longeval.spark import spark_resource

app = typer.Typer()

TRAIN_QRELS = ("/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
               "French/LongEval Train Collection/qrels")
TEST_QRELS = os.path.expanduser("~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels")
DOCTD = {4: "/mnt/data/tmp/lda-k4-converged/k4/docTopicDistribution_lda.parquet",
         20: "/mnt/data/tmp/lda-k20-converged/k20/docTopicDistribution_lda.parquet"}
QTD = {4: "/mnt/data/tmp/lda-queries/k4_query_topics.parquet",
       20: "/mnt/data/tmp/lda-queries/k20_query_topics.parquet"}


def _qrels(spark, date):
    path = (f"{TRAIN_QRELS}/{date}_fr/qrels_processed.txt" if date <= "2023-02"
            else f"{TEST_QRELS}/{date}/qrels_processed.txt")
    return spark.read.csv(path, sep=" ",
                          schema="qid STRING, rank INT, docid STRING, rel INT").select("qid", "docid", "rel")


@app.command()
def main(dates: str = typer.Option("2023-01,2023-08")):
    date_list = [d.strip() for d in dates.split(",")]
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        qrels = None
        for d in date_list:
            q = _qrels(spark, d)
            qrels = q if qrels is None else qrels.union(q)
        qrels = qrels.dropDuplicates(["qid", "docid"]).cache()

        for k in (4, 20):
            tcols = [f"topic_{i}" for i in range(k)]
            raw = spark.read.parquet(DOCTD[k]).withColumn(
                "docid", F.regexp_replace("docid", "^doc", "")
            )
            # (A) dedup-divergence over docids that actually appear in our qrels
            jd = qrels.select("docid").distinct().join(raw, "docid", "inner")
            # per-docid: number of copies, and spread of topic_0 as a proxy
            div = jd.groupBy("docid").agg(
                F.count("*").alias("copies"),
                *[F.stddev(c).alias(f"sd_{c}") for c in tcols],
            )
            sd_cols = [f"sd_{c}" for c in tcols]
            div = div.withColumn("max_sd", F.greatest(*[F.coalesce(F.col(c), F.lit(0.0)) for c in sd_cols]))
            stats = div.agg(
                F.count("*").alias("n_docids"),
                F.sum(F.when(F.col("copies") > 1, 1).otherwise(0)).alias("n_dup"),
                F.max("copies").alias("max_copies"),
                F.mean(F.when(F.col("copies") > 1, F.col("max_sd"))).alias("mean_max_sd_dups"),
                F.max("max_sd").alias("worst_max_sd"),
            ).collect()[0]
            print(f"\n==== K={k} dedup-divergence (judged docids) ====")
            print(f"  judged docids matched : {stats['n_docids']:,}")
            print(f"  with >1 copy          : {stats['n_dup']:,} "
                  f"({(stats['n_dup'] or 0)/max(stats['n_docids'],1):.1%}), max copies={stats['max_copies']}")
            print(f"  mean max-θ-stddev among dup docids : {stats['mean_max_sd_dups']}")
            print(f"  worst max-θ-stddev (any docid)     : {stats['worst_max_sd']}")
            print("  (near-0 stddev => copies share ~same θ => date-blind dedup harmless)")

            # (B) re-score with cosine AND Hellinger on the deduped join
            doc = raw.dropDuplicates(["docid"]).select("docid", F.array(*tcols).alias("dtd"))
            qtd = spark.read.parquet(QTD[k]).where(F.col("vocab_hits") > 0).select(
                "qid", F.col("td").alias("qtd"))
            pdf = (qrels.join(qtd, "qid", "inner").join(doc, "docid", "inner")
                   .select("qid", "rel", "qtd", "dtd").toPandas())
            Q = np.vstack(pdf["qtd"].values).astype(float)
            D = np.vstack(pdf["dtd"].values).astype(float)
            cos = (Q * D).sum(1) / (np.linalg.norm(Q, axis=1) * np.linalg.norm(D, axis=1) + 1e-12)
            # Hellinger similarity = 1 - H, H = (1/sqrt2)||sqrt p - sqrt q||_2
            # normalize rows to sum 1 first (theta should already, but be safe)
            Qn = Q / (Q.sum(1, keepdims=True) + 1e-12)
            Dn = D / (D.sum(1, keepdims=True) + 1e-12)
            H = np.linalg.norm(np.sqrt(Qn) - np.sqrt(Dn), axis=1) / np.sqrt(2)
            hell_sim = 1.0 - H
            pos = (pdf["rel"] > 0).astype(int).values
            def aucs(score):
                pooled = roc_auc_score(pos, score) if len(set(pos)) == 2 else float("nan")
                per = []
                for _, idx in pdf.groupby("qid").groups.items():
                    ii = [pdf.index.get_loc(x) for x in idx]
                    yp = pos[ii]
                    if len(set(yp)) == 2:
                        per.append(roc_auc_score(yp, score[ii]))
                return pooled, (np.mean(per) if per else float("nan")), len(per)
            cp, cm, nq = aucs(cos)
            hp, hm, _ = aucs(hell_sim)
            print(f"  cosine    : pooled AUC={cp:.4f}  macro per-query AUC={cm:.4f} (n_q={nq})")
            print(f"  Hellinger : pooled AUC={hp:.4f}  macro per-query AUC={hm:.4f}")
        qrels.unpersist()


if __name__ == "__main__":
    app()
