"""Held-out longitudinal extension: score the *fixed* 9-slice train model
on the 6 LongEval test slices (2023-03..08) and recompute the per-slice
topic-proportion + drift summary over all 15 slices.

Why a separate module, not the stamped sweep DAG: the train sweep
(``TrainLDASweep`` -> ``InferLDASweep`` -> ``AggregateTopicProportions``)
fits the model, the NPMI phrase set and the CountVectorizer vocab on the
9 train slices and pools them via the ``date="all"`` sentinel. The
scientifically correct way to ask "does the Nov->Dec 2022 regime hold
into 2023?" is *held-out inference of that already-trained model* on the
test slices, NOT a 15-slice retrain (which would change the topics and
destroy the comparison). That is a fundamentally different operation
than the DAG models, so it lives here as a focused, reviewable step.

The featurisation path is byte-identical to ``MinePhrases`` +
``BuildFeatures`` (same native FR analyzer UDF, same StopWordsRemover
stems, same length/numeric filter, same ``apply_phrases``, the same
saved ``vector_model``) so the test documents land in exactly the train
vocabulary. Test n-grams absent from the train vocab are dropped by
``CountVectorizerModel`` — the correct held-out behaviour, not a bug.

The per-slice aggregation and the drift window are copied verbatim from
``TopicProportions`` / ``AggregateTopicProportions`` so the 6 test rows
are computed by the identical method as the 9 validated train rows; the
two long-format frames are then unioned and drift is recomputed over the
15-date order.
"""

import functools
import os

import typer
from pyspark.ml.clustering import LocalLDAModel
from pyspark.ml.feature import CountVectorizerModel, StopWordsRemover
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window, functions as F
from typing_extensions import Annotated

from longeval.collection import ParquetCollection
from longeval.etl.lda.phrases import apply_phrases
from longeval.etl.lda.workflow import (
    _ANALYZER_UDF_NAME,
    _analyzer_jars,
    _build_stop_stems,
    _register_fr_analyzer,
)
from longeval.spark import spark_resource

# The 6 deferred LongEval test slices. Same misnamed-`*.jsonl.gz`-is-TREC
# release pattern as 2023-02; ingested to their own parquet via
# `longeval etl trec-parquet` (the train path's mode="overwrite" static
# partition would otherwise wipe the 9 train slices).
TEST_DATES = ["2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"]


def _analyze(docs):
    """The exact MinePhrases token pipeline: native FR analyzer UDF ->
    StopWordsRemover (NLTK FR+EN stems) -> length/numeric filter.

    Kept character-identical to ``MinePhrases.run`` so a held-out doc is
    tokenised the same way the vocabulary was built; any drift here would
    silently shift which test terms are in-vocab.
    """
    remover = StopWordsRemover(
        inputCol="tokens_raw",
        outputCol="tokens_no_stop",
        stopWords=_build_stop_stems(),
    )
    return (
        remover.transform(
            docs.select(
                "docid",
                F.expr(f"{_ANALYZER_UDF_NAME}(contents)").alias("tokens_raw"),
            )
        )
        .withColumn(
            "tokens",
            F.expr(
                "filter(tokens_no_stop, t -> "
                "length(t) >= 3 AND NOT (t rlike '^[0-9]+$'))"
            ),
        )
        .select("docid", "tokens")
    )


def infer_heldout(spark, test_parquet, sweep_root, k_values):
    """Held-out per-K ``docTopicDistribution`` for the test slices.

    Reuses the train ``phrases.parquet`` + ``vector_model`` + per-K
    ``lda_model`` from ``sweep_root``; writes
    ``<sweep_root>/heldout/k{K}/docTopicDistribution_lda.parquet`` in the
    same wide layout ``RunLDAInference`` writes. Idempotent per K
    (``_SUCCESS`` guard) so a re-run resumes instead of recomputing the
    expensive analyze pass.
    """
    heldout_root = os.path.join(sweep_root, "heldout")
    needed = [
        k
        for k in k_values
        if not os.path.exists(
            os.path.join(
                heldout_root, f"k{int(k)}",
                "docTopicDistribution_lda.parquet", "_SUCCESS",
            )
        )
    ]
    if not needed:
        print("infer_heldout: all K already present, skipping.")
        return

    features_path = os.path.join(heldout_root, "features.parquet")
    if not os.path.exists(os.path.join(features_path, "_SUCCESS")):
        # The analyze UDF over ~11M test docs is the dominant cost and
        # is not per-K; persist features to disk (mirrors BuildFeatures)
        # so a crash mid-K resumes without re-running the analyze pass.
        docs = ParquetCollection(spark, test_parquet).documents.select(
            "docid", "contents", "date"
        )
        analyzed = _analyze(docs)
        phrases_df = spark.read.parquet(
            os.path.join(sweep_root, "preprocess", "phrases.parquet")
        )
        phrased = apply_phrases(analyzed, phrases_df).select(
            "docid", "tokens_phrased"
        )
        vector_model = CountVectorizerModel.load(
            os.path.join(sweep_root, "preprocess", "vector_model")
        )
        (
            vector_model.transform(phrased)
            .select("docid", "features")
            .write.mode("overwrite")
            .parquet(features_path)
        )
        print("infer_heldout: held-out features.parquet written.")
    features = spark.read.parquet(features_path).cache()
    try:
        features.count()
        for k in needed:
            k = int(k)
            k_dir = os.path.join(heldout_root, f"k{k}")
            os.makedirs(k_dir, exist_ok=True)
            lda = LocalLDAModel.load(
                os.path.join(sweep_root, f"k{k}", "lda_model")
            )
            scored = (
                lda.transform(features)
                .select(
                    "docid",
                    vector_to_array("topicDistribution").alias("td"),
                )
                .withColumn(
                    "highest_topic",
                    (
                        F.array_position(
                            F.col("td"), F.array_max(F.col("td"))
                        )
                        - 1
                    ).cast("int"),
                )
            )
            projected = scored.select(
                F.col("docid"),
                F.col("highest_topic"),
                *[F.col("td")[i].alias(f"topic_{i}") for i in range(k)],
            )
            projected.write.mode("overwrite").parquet(
                os.path.join(k_dir, "docTopicDistribution_lda.parquet")
            )
            print(f"infer_heldout: wrote held-out docTopicDistribution k={k}.")
    finally:
        features.unpersist()


def _slice_proportions(spark, doc_topic_path, corpus, k):
    """The verbatim ``TopicProportions`` aggregation for one K.

    docid-mean theta (absorbs longitudinal recurrence, 1:1 corpus join)
    -> inner-join (docid,date) -> per-(date) doc count + mean theta ->
    wide-to-long ``(k, date, topic, mean_theta, n_docs)``. Identical to
    ``TopicProportions.run`` so train and test rows are comparable.
    """
    topic_cols = [f"topic_{i}" for i in range(k)]
    doc_topic = spark.read.parquet(doc_topic_path).select(
        "docid", *topic_cols
    )
    dmean = doc_topic.groupBy("docid").agg(
        *[F.avg(c).alias(c) for c in topic_cols]
    )
    joined = corpus.join(dmean, "docid", "inner")
    per_slice = joined.groupBy("date").agg(
        F.count(F.lit(1)).alias("n_docs"),
        *[F.avg(c).alias(c) for c in topic_cols],
    )
    return (
        per_slice.select(
            F.lit(k).cast("int").alias("k"),
            "date",
            "n_docs",
            F.posexplode(
                F.array(*[F.col(c) for c in topic_cols])
            ).alias("topic", "mean_theta"),
        )
        .select("k", "date", "topic", "mean_theta", "n_docs")
        .orderBy("date", "topic")
    )


def combine_and_drift(spark, sweep_root, test_parquet, k_values):
    """Union the validated 9-slice train proportions with the freshly
    computed 6-slice held-out proportions and recompute drift over 15
    slices. The train side is read straight off the existing
    ``topic_proportions.parquet`` (no train recompute, exact validated
    numbers); the test side is computed by ``_slice_proportions`` (the
    same method) and schema-aligned before union.
    """
    train_prop = spark.read.parquet(
        os.path.join(sweep_root, "topic_proportions.parquet")
    )
    test_corpus = ParquetCollection(
        spark, test_parquet
    ).documents.select("docid", "date")
    test_frames = [
        _slice_proportions(
            spark,
            os.path.join(
                sweep_root, "heldout", f"k{int(k)}",
                "docTopicDistribution_lda.parquet",
            ),
            test_corpus,
            int(k),
        )
        for k in k_values
    ]
    test_prop = functools.reduce(
        lambda a, b: a.unionByName(b), test_frames
    )
    # Align the test frame to the on-disk train schema field-by-field so
    # unionByName never silently up/down-casts a column.
    test_prop = test_prop.select(
        *[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in train_prop.schema.fields
        ]
    )
    long_df = train_prop.unionByName(test_prop).orderBy(
        "k", "date", "topic"
    )
    heldout_root = os.path.join(sweep_root, "heldout")
    os.makedirs(heldout_root, exist_ok=True)
    long_df.write.mode("overwrite").parquet(
        os.path.join(heldout_root, "topic_proportions_15slice.parquet")
    )

    span = Window.partitionBy("k", "topic").orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    drift = (
        long_df.withColumn("first_theta", F.first("mean_theta").over(span))
        .withColumn("last_theta", F.last("mean_theta").over(span))
        .groupBy("k", "topic")
        .agg(
            F.min("mean_theta").alias("min_theta"),
            F.max("mean_theta").alias("max_theta"),
            F.avg("mean_theta").alias("mean_theta"),
            F.stddev_pop("mean_theta").alias("std_theta"),
            F.first("first_theta").alias("first_theta"),
            F.first("last_theta").alias("last_theta"),
            F.countDistinct("date").alias("n_slices"),
        )
        .withColumn("range_theta", F.col("max_theta") - F.col("min_theta"))
        .withColumn(
            "delta_first_last", F.col("last_theta") - F.col("first_theta")
        )
        .orderBy("k", "topic")
    )
    drift.write.mode("overwrite").parquet(
        os.path.join(heldout_root, "topic_drift_15slice.parquet")
    )
    print("combine_and_drift: 15-slice proportions + drift written.")


def heldout_drift_main(
    test_parquet: Annotated[
        str,
        typer.Argument(help="Test-slice parquet (separate from train)"),
    ],
    sweep_root: Annotated[
        str,
        typer.Argument(
            help="Finished train sweep root (has preprocess/, k*/, "
            "topic_proportions.parquet) — e.g. "
            "/mnt/data/tmp/lda-all9-clean-sweep"
        ),
    ],
    k_values: Annotated[
        str, typer.Option(help="Comma-separated K (must match the sweep)")
    ] = "5,10,20",
):
    """Held-out test-slice inference + 15-slice topic-proportion drift.

    Reads the fixed train model/vocab/phrases from ``sweep_root``, scores
    the test slices, and writes ``heldout/topic_proportions_15slice.
    parquet`` + ``heldout/topic_drift_15slice.parquet``.
    """
    ks = [int(x) for x in k_values.split(",") if x.strip()]
    with spark_resource(**{"spark.jars": _analyzer_jars()}) as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _register_fr_analyzer(spark)
        infer_heldout(spark, test_parquet, sweep_root, ks)
        combine_and_drift(spark, sweep_root, test_parquet, ks)
