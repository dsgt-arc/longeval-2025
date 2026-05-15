"""Tests for the online-LDA + fitMultiple sweep refactor.

Two layers:
  - Pure (no Spark): requires() routing and the TrainLDASweep ↔
    TrainLDAModel stamp-identity invariant — these are what make the
    sweep dedupe onto one training node without per-K retraining.
  - Spark end-to-end: _build_lda yields an online LocalLDAModel, and
    TrainLDASweep fits every K in one session with correct per-K layout,
    stamps, completion, and topic counts.
"""

import json
import os

from pyspark.ml.clustering import LocalLDAModel
from pyspark.ml.linalg import Vectors

from longeval.etl.lda.workflow import (
    AggregateLDASweep,
    EvaluateLDAModel,
    PlotResults,
    RunLDAInference,
    TrainLDAModel,
    TrainLDASweep,
    _build_lda,
)
from longeval.spark import spark_resource

# ---------------------------------------------------------------------------
# Pure-Python: routing + stamp identity
# ---------------------------------------------------------------------------


def _eval(**over):
    base = dict(
        input_path="/in",
        output_path="/out/k3",
        num_topics=3,
        preprocess_path="/out/preprocess",
    )
    base.update(over)
    return EvaluateLDAModel(**base)


def test_requires_routes_to_single_k_when_no_k_values():
    req = _eval(k_values=None).requires()
    assert isinstance(req, TrainLDAModel)
    assert not isinstance(req, TrainLDASweep)
    assert req.num_topics == 3


def test_requires_routes_to_sweep_when_k_values_set():
    req = _eval(k_values=[2, 3, 5]).requires()
    assert isinstance(req, TrainLDASweep)
    # Sweep root is the parent of the shared preprocess dir, not the
    # per-K output_path — so every per-K Eval derives the SAME node.
    assert req.output_path == "/out"
    assert list(req.k_values) == [2, 3, 5]


def test_all_per_k_evals_dedupe_onto_one_sweep_node():
    agg = AggregateLDASweep(input_path="/in", output_path="/out", k_values=[2, 3])
    sweep_reqs = {
        e.requires() for e in agg.requires()
    }  # set => Luigi task identity
    assert len(sweep_reqs) == 1
    (only,) = sweep_reqs
    assert isinstance(only, TrainLDASweep)


def test_plotresults_requires_inference_and_passes_k_values():
    req = PlotResults(
        input_path="/in",
        output_path="/out/k3",
        num_topics=3,
        preprocess_path="/out/preprocess",
        k_values=[2, 3],
    ).requires()
    assert isinstance(req, RunLDAInference)
    assert list(req.k_values) == [2, 3]


def test_sweep_stamp_is_byte_identical_to_per_k_train():
    # The load-bearing invariant: a downstream Eval's _stamp_matches
    # against k{K}/_train_config.json must pass without a per-K Train
    # ever running. No files exist => both preprocess_hash are None and
    # the dicts must still be equal.
    common = dict(
        input_path="/in",
        preprocess_path="/nope/preprocess",
        seed=7,
        max_iter=4,
        learning_offset=512.0,
        learning_decay=0.7,
        subsampling_rate=0.1,
        optimize_doc_concentration=False,
    )
    sweep = TrainLDASweep(output_path="/out", k_values=[2, 3], **common)
    per_k = TrainLDAModel(output_path="/out/k3", num_topics=3, **common)
    assert sweep._train_config_dict(3) == per_k._config_dict()


# ---------------------------------------------------------------------------
# Spark end-to-end
# ---------------------------------------------------------------------------


def _write_features(spark, preprocess_dir):
    """Stage a tiny features.parquet + preprocess stamp TrainLDASweep
    can read (mirrors what FitLDAPreprocessing would have written)."""
    os.makedirs(preprocess_dir, exist_ok=True)
    rows = [
        (f"d{i}", Vectors.dense([float((i + j) % 5) for j in range(6)]))
        for i in range(24)
    ]
    spark.createDataFrame(rows, ["docid", "features"]).write.mode(
        "overwrite"
    ).parquet(os.path.join(preprocess_dir, "features.parquet"))
    with open(os.path.join(preprocess_dir, "_config.json"), "w") as f:
        json.dump({"fixture": True}, f)


def test_build_lda_is_online_and_fits_local_model(tmp_path):
    pp = str(tmp_path / "preprocess")
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _write_features(spark, pp)
        lda = _build_lda(
            k=3,
            max_iter=4,
            seed=1,
            learning_offset=512.0,
            learning_decay=0.7,
            subsampling_rate=0.5,
            optimize_doc_concentration=False,
        )
        assert lda.getOptimizer() == "online"
        assert lda.getLearningOffset() == 512.0
        assert lda.getLearningDecay() == 0.7
        assert lda.getSubsamplingRate() == 0.5
        assert lda.getOptimizeDocConcentration() is False
        feats = spark.read.parquet(os.path.join(pp, "features.parquet"))
        model = lda.fit(feats)
        assert isinstance(model, LocalLDAModel)


def test_trainldasweep_fits_all_k_in_one_session(tmp_path):
    root = str(tmp_path / "sweep")
    pp = os.path.join(root, "preprocess")
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _write_features(spark, pp)

    sweep = TrainLDASweep(
        input_path="/unused",
        output_path=root,
        preprocess_path=pp,
        k_values=[2, 3],
        max_iter=4,
        subsampling_rate=0.5,
    )
    assert sweep.complete() is False
    sweep.run()
    assert sweep.complete() is True

    for k in (2, 3):
        k_dir = os.path.join(root, f"k{k}")
        assert os.path.isdir(os.path.join(k_dir, "lda_model"))
        stamp = os.path.join(k_dir, "_train_config.json")
        assert os.path.exists(stamp)
        with open(stamp) as f:
            assert json.load(f)["num_topics"] == k

    # fitMultiple index↔k: each saved model must have its k topics.
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        for k in (2, 3):
            m = LocalLDAModel.load(os.path.join(root, f"k{k}", "lda_model"))
            assert m.describeTopics().count() == k

    # Mutating a stamp must drop completion (the per-K invalidation path).
    bad = os.path.join(root, "k2", "_train_config.json")
    with open(bad, "w") as f:
        json.dump({"num_topics": 999}, f)
    assert sweep.complete() is False
