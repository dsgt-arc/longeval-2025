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
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.linalg import Vectors

from longeval.etl.lda.workflow import (
    AggregateLDASweep,
    BuildFeatures,
    EvaluateLDAModel,
    InferLDASweep,
    MinePhrases,
    PlotResults,
    RunLDAInference,
    TrainLDAModel,
    TrainLDASweep,
    _build_lda,
    _phrases_hash,
    _preprocess_hash,
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


def _plot(**over):
    base = dict(
        input_path="/in",
        output_path="/out/k3",
        num_topics=3,
        preprocess_path="/out/preprocess",
    )
    base.update(over)
    return PlotResults(**base)


def test_plotresults_routes_to_infer_sweep_when_k_values_set():
    req = _plot(k_values=[2, 3]).requires()
    assert isinstance(req, InferLDASweep)
    assert req.output_path == "/out"  # parent of the shared preprocess dir
    assert list(req.k_values) == [2, 3]


def test_plotresults_routes_to_single_k_inference_when_no_k_values():
    req = _plot(k_values=None).requires()
    assert isinstance(req, RunLDAInference)
    assert not isinstance(req, InferLDASweep)
    assert req.num_topics == 3


def test_aggregate_pulls_one_infer_sweep_deduped_with_train():
    agg = AggregateLDASweep(input_path="/in", output_path="/out", k_values=[2, 3])
    reqs = agg.requires()
    infers = [r for r in reqs if isinstance(r, InferLDASweep)]
    assert len(infers) == 1
    # InferLDASweep and every Eval route to the SAME TrainLDASweep node.
    train_nodes = {
        r.requires() for r in reqs
    }  # set => Luigi task identity
    assert len(train_nodes) == 1
    (only,) = train_nodes
    assert isinstance(only, TrainLDASweep)


def test_infersweep_stamp_byte_identical_to_run_inference():
    # Same load-bearing invariant as train: a per-K RunLDAInference's
    # _stamp_matches against k{K}/_inference_config.json must pass without
    # it ever running. Identical requested params => identical
    # preprocess_hash => dicts equal (path-independent by design).
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
    sweep = InferLDASweep(output_path="/out", k_values=[2, 3], **common)
    per_k = RunLDAInference(output_path="/out/k3", num_topics=3, **common)
    assert sweep._infer_config_dict(3) == per_k._config_dict()


def test_buildfeatures_requires_minephrases_and_cascades():
    bf = BuildFeatures(input_path="/in", output_path="/pp")
    assert isinstance(bf.requires(), MinePhrases)
    h1 = bf._config_dict()["phrases_hash"]
    # phrases_hash is derived from BuildFeatures' OWN params (not read
    # from disk), so a phrase-param change flips it deterministically.
    bf2 = BuildFeatures(
        input_path="/in", output_path="/pp", phrases_threshold=0.9
    )
    assert bf2._config_dict()["phrases_hash"] != h1
    # ...but a pure vocab change must NOT touch phrases_hash (only the
    # expensive analyze pass is keyed by phrase params).
    bf3 = BuildFeatures(input_path="/in", output_path="/pp", min_df=100.0)
    assert bf3._config_dict()["phrases_hash"] == h1


def test_requested_params_hash_is_path_independent_and_param_sensitive():
    # Same requested config from different tasks => same hash (this is
    # what preserves the TrainLDASweep<->TrainLDAModel byte-identity and
    # the per-K dedup); a min_df change => different hash.
    a = TrainLDAModel(
        input_path="/in", output_path="/a/k3", num_topics=3, min_df=50.0
    )
    b = TrainLDASweep(
        input_path="/in", output_path="/b", k_values=[3], min_df=50.0
    )
    assert _preprocess_hash(a) == _preprocess_hash(b)
    c = TrainLDAModel(
        input_path="/in", output_path="/a/k3", num_topics=3, min_df=100.0
    )
    assert _preprocess_hash(c) != _preprocess_hash(a)


def test_preprocess_param_change_flips_consumer_complete_keys(tmp_path):
    # Regression for the stale-hash bug: a min_df change MUST change the
    # preprocess_hash embedded in every consumer + root stamp, so their
    # complete() flips and Luigi descends to re-run BuildFeatures —
    # instead of short-circuiting on a materialized hash.
    base = dict(
        input_path="/in",
        date="2022-06",
        sample_fraction=0.05,
        preprocess_path="/out/preprocess",
        k_values=[5, 10],
    )
    ev50 = EvaluateLDAModel(output_path="/out/k5", num_topics=5, min_df=50.0, **base)
    ev100 = EvaluateLDAModel(output_path="/out/k5", num_topics=5, min_df=100.0, **base)
    assert (
        ev50._config_dict()["preprocess_hash"]
        != ev100._config_dict()["preprocess_hash"]
    )
    agg50 = AggregateLDASweep(
        input_path="/in", output_path="/out", k_values=[5, 10],
        date="2022-06", sample_fraction=0.05, min_df=50.0,
    )
    agg100 = AggregateLDASweep(
        input_path="/in", output_path="/out", k_values=[5, 10],
        date="2022-06", sample_fraction=0.05, min_df=100.0,
    )
    assert (
        agg50._config_dict()["preprocess_hash"]
        != agg100._config_dict()["preprocess_hash"]
    )


def test_sweep_stamp_is_byte_identical_to_per_k_train():
    # The load-bearing invariant: a downstream Eval's _stamp_matches
    # against k{K}/_train_config.json must pass without a per-K Train
    # ever running. Identical requested params => identical
    # preprocess_hash => dicts equal (path-independent by design).
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
    can read (mirrors what BuildFeatures would have written)."""
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


def _write_preprocess(spark, preprocess_dir):
    """Stage a real vector_model + features.parquet + stamp (InferLDASweep
    loads the CountVectorizerModel, so the dense-vector fixture is not
    enough here)."""
    os.makedirs(preprocess_dir, exist_ok=True)
    vocab = ["alpha", "beta", "gamma", "delta", "eps", "zeta"]
    rows = [(f"d{i}", [vocab[(i + j) % len(vocab)] for j in range(4)]) for i in range(30)]
    df = spark.createDataFrame(rows, ["docid", "tokens_phrased"])
    cv = CountVectorizer(
        inputCol="tokens_phrased", outputCol="features", minDF=1.0
    ).fit(df)
    cv.write().overwrite().save(os.path.join(preprocess_dir, "vector_model"))
    cv.transform(df).select("docid", "features").write.mode(
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


def test_infersweep_scores_all_k_and_satisfies_per_k_inference(tmp_path):
    root = str(tmp_path / "sweep")
    pp = os.path.join(root, "preprocess")
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _write_preprocess(spark, pp)

    kw = dict(
        input_path="/unused",
        output_path=root,
        preprocess_path=pp,
        k_values=[2, 3],
        max_iter=4,
        subsampling_rate=0.5,
    )
    TrainLDASweep(**kw).run()
    infer = InferLDASweep(**kw)
    assert infer.complete() is False
    infer.run()
    assert infer.complete() is True

    for k in (2, 3):
        k_dir = os.path.join(root, f"k{k}")
        dt = os.path.join(k_dir, "docTopicDistribution_lda.parquet")
        assert os.path.isdir(dt)
        assert os.path.exists(os.path.join(k_dir, "topicWords_lda.txt"))
        # Byte-identical stamp => a per-K RunLDAInference is satisfied
        # by the sweep without ever running.
        per_k = RunLDAInference(
            input_path="/unused",
            output_path=k_dir,
            num_topics=k,
            preprocess_path=pp,
            max_iter=4,
            subsampling_rate=0.5,
        )
        assert infer._infer_config_dict(k) == per_k._config_dict()
        assert per_k.complete() is True

    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        cols = spark.read.parquet(
            os.path.join(root, "k3", "docTopicDistribution_lda.parquet")
        ).columns
        assert "docid" in cols and "highest_topic" in cols
        assert {f"topic_{i}" for i in range(3)}.issubset(cols)

    bad = os.path.join(root, "k2", "_inference_config.json")
    with open(bad, "w") as f:
        json.dump({"task": "nope"}, f)
    assert infer.complete() is False
