"""Distribution of the BM25 top-k retrieved documents.

(1) Topic mix of retrieved docs vs the corpus mix and the query mix (K=4/K=20):
    does BM25 surface query-like content, or drag in the un-queried
    English-media/spam bloat that dominates the corpus?
(2) BM25 score/rank distribution: per-rank score decay, percentiles,
    hits-per-query.
(3) Positive control: AUC of raw BM25 score for rel>0 vs rel=0, contrasted with
    the topic-cosine AUC (~0.5) — shows separable signal exists; topics just
    don't carry it.
"""

import os

import numpy as np
import pandas as pd
import typer
from pyspark.sql import functions as F
from sklearn.metrics import roc_auc_score

from longeval.spark import spark_resource
from longeval.experiment.bm25.retrieval import run_search

app = typer.Typer()

INDEX = "/mnt/data/tmp/longeval-bm25-test/index_trec"
TRAIN = ("/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
         "French/LongEval Train Collection")
TEST = ("/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
        "LongEval Test Collection")
TEST_QRELS = os.path.expanduser("~/scratch/longeval/raw/2025/test-qrels/longeval_web_qrels")
DOCTD = {4: "/mnt/data/tmp/lda-k4-converged/k4/docTopicDistribution_lda.parquet",
         20: "/mnt/data/tmp/lda-k20-converged/k20/docTopicDistribution_lda.parquet"}
QTD = {4: "/mnt/data/tmp/lda-queries/k4_query_topics.parquet",
       20: "/mnt/data/tmp/lda-queries/k20_query_topics.parquet"}


@app.command()
def main(dates: str = typer.Option("2023-01,2023-08"), topk: int = typer.Option(100)):
    date_list = [d.strip() for d in dates.split(",")]
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        parts = []
        for d in date_list:
            qroot = TRAIN if d <= "2023-02" else TEST
            queries = spark.read.csv(f"{qroot}/queries/{d}_queries.txt", sep="\t",
                                     schema="qid STRING, query STRING").where(F.col("query").isNotNull())
            res = run_search(queries, f"{INDEX}/date={d}", k=topk)
            res = res.withColumn("docid", F.regexp_replace("docid", "^doc", "")).withColumn("date", F.lit(d))
            parts.append(res)
        ret = parts[0]
        for p in parts[1:]:
            ret = ret.union(p)
        ret = ret.cache()
        n_ret = ret.count()
        n_q = ret.select("qid").distinct().count()

        # ---- (2) score / rank distribution ----
        rp = ret.select("qid", "rank", "score").toPandas()
        print(f"\n==== BM25 score/rank distribution (top-{topk}, {n_q} queries, {n_ret:,} rows) ====")
        print("hits per query: mean={:.1f} median={:.0f} min={} max={}".format(
            rp.groupby("qid").size().mean(), rp.groupby("qid").size().median(),
            rp.groupby("qid").size().min(), rp.groupby("qid").size().max()))
        print("score percentiles (all rows):", {p: round(np.percentile(rp["score"], p), 2)
                                                 for p in (1, 25, 50, 75, 99)})
        print("mean BM25 score by rank position:")
        for r in (0, 1, 4, 9, 24, 49, 99):
            sub = rp[rp["rank"] == r]["score"]
            if len(sub):
                print(f"  rank {r+1:>3}: mean={sub.mean():.2f}  median={sub.median():.2f}  n={len(sub)}")

        # ---- qrels ----
        qr = None
        for d in date_list:
            path = (f"{TRAIN}/qrels/{d}_fr/qrels_processed.txt" if d <= "2023-02"
                    else f"{TEST_QRELS}/{d}/qrels_processed.txt")
            q = spark.read.csv(path, sep=" ", schema="qid STRING, rank INT, docid STRING, rel INT").select(
                "qid", "docid", "rel")
            qr = q if qr is None else qr.union(q)

        for k in (4, 20):
            tcols = [f"topic_{i}" for i in range(k)]
            raw = spark.read.parquet(DOCTD[k]).withColumn("docid", F.regexp_replace("docid", "^doc", ""))
            doc = raw.dropDuplicates(["docid"]).select("docid", *tcols)
            # corpus mix = mean of topic cols over all docs
            corpus = np.array(raw.agg(*[F.mean(c).alias(c) for c in tcols]).collect()[0])
            # retrieved mix = mean topic over retrieved (qid,docid) pairs
            rdoc = ret.select("qid", "docid").join(doc, "docid", "inner")
            retr = np.array(rdoc.agg(*[F.mean(c).alias(c) for c in tcols]).collect()[0])
            # query mix (non-empty)
            qpdf = spark.read.parquet(QTD[k]).where(F.col("vocab_hits") > 0).select("td").toPandas()
            qmix = np.vstack(qpdf["td"].values).mean(0); qmix = qmix / qmix.sum()
            retr = retr / retr.sum(); corpus = corpus / corpus.sum()
            print(f"\n==== K={k} topic mix: corpus vs query vs BM25-retrieved ====")
            print(f"{'topic':<7}{'corpus':>9}{'query':>9}{'retrieved':>11}")
            for i in range(k):
                print(f"{i:<7}{corpus[i]:>9.3f}{qmix[i]:>9.3f}{retr[i]:>11.3f}")

            # ---- (3) positive control: BM25-score AUC vs topic-cosine AUC ----
            qtd = spark.read.parquet(QTD[k]).where(F.col("vocab_hits") > 0).select(
                "qid", F.col("td").alias("qtd"))
            jj = (ret.select("qid", "docid", "score").join(qr, ["qid", "docid"], "inner")
                  .join(qtd, "qid", "inner").join(doc.select("docid", *tcols), "docid", "inner")).toPandas()
            if len(jj):
                Q = np.vstack(jj["qtd"].values).astype(float)
                D = jj[tcols].values.astype(float)
                cos = (Q * D).sum(1) / (np.linalg.norm(Q, axis=1) * np.linalg.norm(D, axis=1) + 1e-12)
                pos = (jj["rel"] > 0).astype(int).values
                if len(set(pos)) == 2:
                    print(f"  [retrieved∩judged n={len(jj):,}]  "
                          f"AUC(BM25 score)={roc_auc_score(pos, jj['score'].values):.3f}  "
                          f"AUC(topic cos)={roc_auc_score(pos, cos):.3f}")
        ret.unpersist()


if __name__ == "__main__":
    app()
