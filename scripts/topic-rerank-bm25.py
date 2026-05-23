"""Does LDA topic similarity improve the ordering of the BM25 top-25?

The principled version of the topic-signal probe (pi-review point #2): operate
on the *actual* BM25 candidate set, not the full qrels pool. For each query we
take BM25's top-25, then build three orderings and score NDCG@10 against the
qrels:
  - bm25      : original BM25 score order (baseline)
  - topic     : reorder purely by cos(theta_query, theta_doc)
  - fusion    : min-max-normalize each, score = (1-w)*bm25 + w*topic
We also report Kendall tau between the bm25 and topic orderings (how much
topics even reshuffle the list). Run for K=4 and K=20.
"""

import os

import numpy as np
import pandas as pd
import pytrec_eval
import typer
from pyspark.sql import functions as F
from scipy.stats import kendalltau

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


def _ndcg10(run, qrel):
    ev = pytrec_eval.RelevanceEvaluator(qrel, {"ndcg_cut_10"})
    res = ev.evaluate(run)
    return np.mean([v["ndcg_cut_10"] for v in res.values()]) if res else float("nan")


@app.command()
def main(dates: str = typer.Option("2023-01,2023-08"), topk: int = typer.Option(25)):
    date_list = [d.strip() for d in dates.split(",")]
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        # ---- BM25 retrieval (top-k) for each date, collected to one pandas df ----
        parts = []
        for d in date_list:
            qroot, split = (TRAIN, "train") if d <= "2023-02" else (TEST, "test")
            queries = spark.read.csv(f"{qroot}/queries/{d}_queries.txt", sep="\t",
                                     schema="qid STRING, query STRING").where(F.col("query").isNotNull())
            res = run_search(queries, f"{INDEX}/date={d}", k=topk)  # qid, rank, docid, score
            res = res.withColumn("docid", F.regexp_replace("docid", "^doc", "")).withColumn("date", F.lit(d))
            parts.append(res)
        ret = parts[0]
        for p in parts[1:]:
            ret = ret.union(p)
        ret = ret.cache()

        # ---- qrels (bare-int docid) ----
        qr = None
        for d in date_list:
            path = (f"{TRAIN}/qrels/{d}_fr/qrels_processed.txt" if d <= "2023-02"
                    else f"{TEST_QRELS}/{d}/qrels_processed.txt")
            q = spark.read.csv(path, sep=" ", schema="qid STRING, rank INT, docid STRING, rel INT").select(
                "qid", "docid", "rel")
            qr = q if qr is None else qr.union(q)
        qrels_pdf = qr.toPandas()
        # full per-query qrels for IDCG
        qrel_dict = {}
        for qid, docid, rel in qrels_pdf.itertuples(index=False):
            qrel_dict.setdefault(qid, {})[docid] = int(rel)

        retr_pdf = ret.select("qid", "docid", F.col("score").alias("bm25")).toPandas()

        for k in (4, 20):
            tcols = [f"topic_{i}" for i in range(k)]
            doc = (spark.read.parquet(DOCTD[k]).withColumn("docid", F.regexp_replace("docid", "^doc", ""))
                   .dropDuplicates(["docid"]).select("docid", F.array(*tcols).alias("dtd")))
            qtd = spark.read.parquet(QTD[k]).where(F.col("vocab_hits") > 0).select(
                "qid", F.col("td").alias("qtd"))
            # join topic vectors onto the retrieved top-k
            j = (ret.select("qid", "docid", "score")
                 .join(qtd, "qid", "inner").join(doc, "docid", "inner")).toPandas()
            Q = np.vstack(j["qtd"].values).astype(float)
            D = np.vstack(j["dtd"].values).astype(float)
            j["topic"] = (Q * D).sum(1) / (np.linalg.norm(Q, axis=1) * np.linalg.norm(D, axis=1) + 1e-12)
            j = j.rename(columns={"score": "bm25"})

            # build runs
            run_bm25, run_topic = {}, {}
            run_fuse = {w: {} for w in (0.1, 0.3, 0.5)}
            taus = []
            for qid, g in j.groupby("qid"):
                g = g.copy()
                run_bm25[qid] = dict(zip(g["docid"], g["bm25"].astype(float)))
                run_topic[qid] = dict(zip(g["docid"], g["topic"].astype(float)))
                if len(g) > 2 and g["bm25"].nunique() > 1 and g["topic"].nunique() > 1:
                    taus.append(kendalltau(g["bm25"].rank(), g["topic"].rank()).correlation)
                # min-max norm within candidate set for fusion
                def mm(s):
                    s = s.values.astype(float)
                    return (s - s.min()) / (s.max() - s.min() + 1e-12)
                bn, tn = mm(g["bm25"]), mm(g["topic"])
                for w in run_fuse:
                    run_fuse[w][qid] = dict(zip(g["docid"], (1 - w) * bn + w * tn))

            # restrict qrels to queries we actually scored
            qd = {q: qrel_dict[q] for q in run_bm25 if q in qrel_dict}
            print(f"\n==== K={k}  (queries scored: {len(qd)}, top-{topk}) ====")
            print(f"  NDCG@10  bm25 (baseline) : {_ndcg10({q:run_bm25[q] for q in qd}, qd):.4f}")
            print(f"  NDCG@10  topic-only      : {_ndcg10({q:run_topic[q] for q in qd}, qd):.4f}")
            for w in sorted(run_fuse):
                print(f"  NDCG@10  fusion w={w}    : {_ndcg10({q:run_fuse[w][q] for q in qd}, qd):.4f}")
            print(f"  Kendall tau (bm25 vs topic order): mean={np.nanmean(taus):.3f} "
                  f"(0=no relation, 1=identical order; n_q={len(taus)})")
        ret.unpersist()


if __name__ == "__main__":
    app()
