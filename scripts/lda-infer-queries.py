"""Apply the converged K=4 and K=20 LDA models to the LongEval query set.

Replicates the exact document preprocessing chain (Lucene FR analyzer ->
NLTK stop stems -> length/numeric filter -> NPMI phrases -> CountVectorizer
vocab) on queries, then transforms with each frozen LDA model to get a
per-query topic distribution. Queries are ~3 words, so many reduce to an
empty in-vocab bag; we report that explicitly. Compares the query topic mix
to the document topic mix (archived topic_proportions.parquet).
"""

import glob
import os
from pathlib import Path

import typer
from pyspark.sql import functions as F
from pyspark.ml.feature import CountVectorizerModel, StopWordsRemover
from pyspark.ml.clustering import LocalLDAModel
from pyspark.ml.functions import vector_to_array

from longeval.spark import spark_resource
from longeval.etl.lda.workflow import (
    _analyzer_jars, _register_fr_analyzer, _build_stop_stems, _ANALYZER_UDF_NAME,
)
from longeval.etl.lda.phrases import apply_phrases

app = typer.Typer()

ARC = "/mnt/data/research/arc"
K4 = f"{ARC}/longeval-lda-k4-converged-archive"
K20 = f"{ARC}/longeval-lda-k20-converged-archive"
TRAIN_Q = (
    "/mnt/data/scratch/longeval/longeval-web/release_2025_p1/release_2025_p1/"
    "French/LongEval Train Collection/queries"
)
TEST_Q = (
    "/mnt/data/scratch/longeval/longeval-web/LongEval Test Collection/"
    "LongEval Test Collection/queries"
)


@app.command()
def main(out_path: str = typer.Option("/mnt/data/tmp/lda-queries")):
    os.makedirs(out_path, exist_ok=True)
    with spark_resource(**{"spark.jars": _analyzer_jars()}) as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _register_fr_analyzer(spark)

        # --- unique queries (qid text is identical across dates) ---
        files = sorted(glob.glob(f"{TRAIN_Q}/*_queries.txt")) + sorted(
            glob.glob(f"{TEST_Q}/*_queries.txt")
        )
        raw = (
            spark.read.csv(files, sep="\t", schema="qid STRING, query STRING")
            .where(F.col("query").isNotNull())
            .groupBy("qid")
            .agg(F.first("query").alias("query"))
        )
        n_q = raw.count()

        # --- replicate the document preprocessing chain ---
        remover = StopWordsRemover(
            inputCol="tokens_raw", outputCol="tokens_no_stop",
            stopWords=_build_stop_stems(),
        )
        analyzed = (
            remover.transform(
                raw.select(
                    "qid", "query",
                    F.expr(f"{_ANALYZER_UDF_NAME}(query)").alias("tokens_raw"),
                )
            )
            .withColumn(
                "tokens",
                F.expr("filter(tokens_no_stop, t -> "
                       "length(t) >= 3 AND NOT (t rlike '^[0-9]+$'))"),
            )
            .select("qid", "query", "tokens")
        )

        phrases_df = spark.read.parquet(f"{K4}/preprocess/phrases.parquet")
        phrased = apply_phrases(analyzed, phrases_df, id_col="qid")  # adds tokens_phrased

        vector_model = CountVectorizerModel.load(f"{K4}/preprocess/vector_model")
        feats = vector_model.transform(phrased).select(
            "qid", "query", "tokens_phrased", "features"
        )
        # an empty CountVectorizer vector => no in-vocab tokens
        n_vec = vector_to_array("features")
        feats = feats.withColumn("vocab_hits", F.aggregate(n_vec, F.lit(0.0), lambda a, x: a + x))
        feats.cache()
        n_empty = feats.where(F.col("vocab_hits") == 0).count()

        # --- transform with each model, write dominant topic ---
        for tag, root, k in (("k4", K4, 4), ("k20", K20, 20)):
            lda = LocalLDAModel.load(f"{root}/{('k4' if tag=='k4' else 'k20')}/lda_model")
            td = lda.transform(feats.select("qid", "query", "vocab_hits", "features"))
            td = td.select(
                "qid", "query", "vocab_hits",
                vector_to_array("topicDistribution").alias("td"),
            ).withColumn("dominant", F.expr("array_position(td, array_max(td)) - 1"))
            td.write.mode("overwrite").parquet(f"{out_path}/{tag}_query_topics.parquet")

            print(f"\n==== {tag.upper()} query topic mix (n={n_q}, empty-bag={n_empty}) ====")
            # mean topic proportion across all queries (incl. empty -> prior)
            arr = td.select("td").rdd.map(lambda r: r.td).collect()
            import numpy as np
            arr = np.array(arr)
            qmix = arr.mean(axis=0)
            # dominant-topic histogram over non-empty queries
            dom = (
                td.where(F.col("vocab_hits") > 0)
                .groupBy("dominant").count().orderBy(F.desc("count"))
                .collect()
            )
            print("mean topic proportion (all queries):",
                  ", ".join(f"t{i}={v:.3f}" for i, v in enumerate(qmix)))
            print("dominant-topic histogram (non-empty queries):")
            tot = sum(r["count"] for r in dom)
            for r in dom[:k]:
                print(f"  topic {r['dominant']:>2}: {r['count']:>6} ({r['count']/tot:.1%})")

        feats.unpersist()
        print(f"\nwrote per-query topic parquets under {out_path}")


if __name__ == "__main__":
    app()
