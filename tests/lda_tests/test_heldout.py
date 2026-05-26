"""Held-out longitudinal extension (longeval.etl.lda.heldout).

Covers the genuinely new logic — not the analyzer/inference path
(``infer_heldout``'s tokenisation is byte-identical reused MinePhrases
code, and the native UDF has its own test): the per-slice aggregation is
the verbatim ``TopicProportions`` method, and ``combine_and_drift``
reuses the *existing validated* train ``topic_proportions.parquet``,
schema-aligns the freshly computed test rows, unions them, and recomputes
drift over the full date order. This asserts that union + drift are
exact and that the train numbers pass through untouched.
"""

import os

from longeval.etl.lda.heldout import combine_and_drift
from longeval.spark import spark_resource


def _write_train_proportions(spark, sweep_root):
    """The validated 9-slice artifact's shape: long
    (k, date, topic, mean_theta, n_docs) — here 2 'train' slices, K=2."""
    rows = [
        (2, "2022-06", 0, 0.6, 10),
        (2, "2022-06", 1, 0.4, 10),
        (2, "2022-07", 0, 0.5, 10),
        (2, "2022-07", 1, 0.5, 10),
    ]
    schema = (
        "k int, date string, topic int, mean_theta double, n_docs bigint"
    )
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(
        os.path.join(sweep_root, "topic_proportions.parquet")
    )


def _write_heldout_doc_topic(spark, sweep_root, k):
    """Staged held-out docTopicDistribution (docid keyed, no date — what
    infer_heldout writes), distinct test docids."""
    k_dir = os.path.join(sweep_root, "heldout", f"k{k}")
    os.makedirs(k_dir, exist_ok=True)
    rows = [
        ("t0", 0.8, 0.2, 0),
        ("t1", 0.1, 0.9, 1),
        ("t2", 0.4, 0.6, 1),
        ("t3", 0.7, 0.3, 0),
    ]
    schema = "docid string, topic_0 double, topic_1 double, highest_topic int"
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(
        os.path.join(k_dir, "docTopicDistribution_lda.parquet")
    )


def _write_test_corpus(spark, corpus_root):
    """Test-slice Documents parquet: 3 docs in 2023-03, 1 in 2023-04."""
    rows = [
        ("t0", "2023-03"), ("t1", "2023-03"), ("t2", "2023-03"),
        ("t3", "2023-04"),
    ]
    spark.createDataFrame(rows, "docid string, date string").write.mode(
        "overwrite"
    ).partitionBy("date").parquet(os.path.join(corpus_root, "Documents"))


def test_combine_and_drift_unions_train_and_heldout_over_full_span(tmp_path):
    sweep_root = str(tmp_path / "sweep")
    test_parquet = str(tmp_path / "testcorpus")
    os.makedirs(sweep_root, exist_ok=True)

    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _write_train_proportions(spark, sweep_root)
        _write_heldout_doc_topic(spark, sweep_root, 2)
        _write_test_corpus(spark, test_parquet)

    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        combine_and_drift(spark, sweep_root, test_parquet, [2])

        heldout = os.path.join(sweep_root, "heldout")
        prop = spark.read.parquet(
            os.path.join(heldout, "topic_proportions_15slice.parquet")
        )
        assert set(prop.columns) == {
            "k", "date", "topic", "mean_theta", "n_docs"
        }
        # 2 train slices + 2 test slices, K=2 -> 4 dates * 2 topics = 8.
        assert prop.count() == 8
        recs = {(r["date"], r["topic"]): r for r in prop.collect()}
        # Train rows pass through byte-untouched.
        assert recs[("2022-06", 0)]["mean_theta"] == 0.6
        assert recs[("2022-06", 0)]["n_docs"] == 10
        # Test rows computed by the identical TopicProportions method:
        # 2023-03 has t0,t1,t2 -> topic_0 = (0.8+0.1+0.4)/3, n=3.
        assert recs[("2023-03", 0)]["n_docs"] == 3
        assert abs(recs[("2023-03", 0)]["mean_theta"] - 1.3 / 3) < 1e-9
        # each slice's theta sums to ~1.
        for d in ("2022-06", "2022-07", "2023-03", "2023-04"):
            s = sum(recs[(d, t)]["mean_theta"] for t in (0, 1))
            assert abs(s - 1.0) < 1e-9
        assert recs[("2023-04", 0)]["n_docs"] == 1
        assert abs(recs[("2023-04", 0)]["mean_theta"] - 0.7) < 1e-9

        drift = {
            r["topic"]: r
            for r in spark.read.parquet(
                os.path.join(heldout, "topic_drift_15slice.parquet")
            ).collect()
        }
        assert set(drift) == {0, 1}
        t0 = drift[0]
        # 4 slices ordered 2022-06,2022-07,2023-03,2023-04;
        # topic_0 = [0.6, 0.5, 1.3/3, 0.7].
        assert t0["n_slices"] == 4
        assert abs(t0["first_theta"] - 0.6) < 1e-9
        assert abs(t0["last_theta"] - 0.7) < 1e-9
        assert abs(t0["delta_first_last"] - (0.7 - 0.6)) < 1e-9
        assert abs(t0["range_theta"] - (0.7 - 1.3 / 3)) < 1e-9
