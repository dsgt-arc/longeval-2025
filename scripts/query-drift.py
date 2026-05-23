"""Per-month query-set overlap + query topic drift (K=4).

Each qid's text (hence its LDA topic) is fixed across months, so any monthly
query-topic drift is driven purely by *which* queries appear each month. This
script (1) quantifies how much the per-month query sets overlap, and (2) joins
each month's query instances to the frozen per-query K=4 topic distribution and
reports the monthly topic mix, to compare against the document Dec-2022 step.
"""

import glob
import os

import numpy as np
import typer
from pyspark.sql import functions as F

from longeval.spark import spark_resource

app = typer.Typer()

TRAIN_Q = ("/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
           "French/LongEval Train Collection/queries")
TEST_Q = ("/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
          "LongEval Test Collection/queries")
K4_TD = "/mnt/data/tmp/lda-queries/k4_query_topics.parquet"


@app.command()
def main():
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        # per-date query instances: (date, qid)
        rows = []
        for f in sorted(glob.glob(f"{TRAIN_Q}/*_queries.txt")) + sorted(
            glob.glob(f"{TEST_Q}/*_queries.txt")
        ):
            date = os.path.basename(f).split("_")[0]
            d = (
                spark.read.csv(f, sep="\t", schema="qid STRING, query STRING")
                .where(F.col("query").isNotNull())
                .select(F.lit(date).alias("date"), "qid")
                .distinct()
            )
            rows.append(d)
        inst = rows[0]
        for d in rows[1:]:
            inst = inst.union(d)
        inst.cache()

        dates = [r["date"] for r in inst.select("date").distinct().orderBy("date").collect()]
        # qid -> set of months it appears in
        permonth = {r["date"]: set(x["qid"] for x in inst.where(F.col("date") == r["date"]).select("qid").collect())
                    for r in inst.select("date").distinct().collect()}

        # 1. how many distinct months does each qid appear in?
        appear = inst.groupBy("qid").agg(F.countDistinct("date").alias("nmonths"))
        hist = {r["nmonths"]: r["count"] for r in appear.groupBy("nmonths").count().orderBy("nmonths").collect()}
        nuniq = appear.count()
        print(f"unique qids: {nuniq}")
        print("months-appeared histogram (nmonths: #qids):", hist)
        print(f"  appear in exactly 1 month : {hist.get(1,0)} ({hist.get(1,0)/nuniq:.1%})")
        print(f"  appear in all {len(dates)} months : {hist.get(len(dates),0)} ({hist.get(len(dates),0)/nuniq:.1%})")

        # 2. consecutive-month Jaccard overlap of query sets
        print("\nconsecutive-month query-set overlap (Jaccard | new this month):")
        for a, b in zip(dates, dates[1:]):
            sa, sb = permonth[a], permonth[b]
            jac = len(sa & sb) / len(sa | sb)
            newb = len(sb - sa) / len(sb)
            print(f"  {a} -> {b}: Jaccard={jac:.2f}  new={newb:.1%}  (|{a}|={len(sa)}, |{b}|={len(sb)})")

        # 3. per-month K=4 topic mix (non-empty queries only). td is already
        # an array<double> column in the parquet.
        td = spark.read.parquet(K4_TD).select("qid", "vocab_hits", "td")
        td = td.where(F.col("vocab_hits") > 0)
        j = inst.join(td, on="qid", how="inner")
        print("\nper-month K=4 query topic mix (non-empty, renormalized):")
        print(f"{'date':<9}{'n_q':>8}  t0     t1     t2     t3")
        for date in dates:
            sub = j.where(F.col("date") == date).select("td").rdd.map(lambda r: r.td).collect()
            a = np.array(sub); m = a.mean(0); m = m / m.sum()
            print(f"{date:<9}{len(sub):>8}  " + "  ".join(f"{x:.3f}" for x in m))
        inst.unpersist()


if __name__ == "__main__":
    app()
