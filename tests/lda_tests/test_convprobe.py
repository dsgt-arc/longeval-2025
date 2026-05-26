"""Convergence-probe smoke test (longeval.etl.lda.convprobe).

Exercises the grid fit + train/holdout split + held-out perplexity
collection on a tiny synthetic features.parquet — enough to lock the
output schema and that every grid cell yields a finite held-out
perplexity, without the cost of a real corpus.
"""

import math
import os

import numpy as np
from pyspark.ml.linalg import Vectors

from longeval.etl.lda.convprobe import convergence_probe
from longeval.spark import spark_resource


def _write_features(spark, preprocess_dir):
    os.makedirs(preprocess_dir, exist_ok=True)
    rng = np.random.default_rng(0)
    rows = [
        (f"d{i}", Vectors.dense([float(x) for x in rng.integers(0, 4, 6)]))
        for i in range(60)
    ]
    spark.createDataFrame(rows, ["docid", "features"]).write.mode(
        "overwrite"
    ).parquet(os.path.join(preprocess_dir, "features.parquet"))


def test_convergence_probe_grid_schema_and_finite_perplexity(tmp_path):
    pp = str(tmp_path / "preprocess")
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        _write_features(spark, pp)
        df = convergence_probe(
            spark,
            preprocess_path=pp,
            k=2,
            iters=[2, 4],
            learning_offsets=[16.0, 64.0],
            learning_decay=0.51,
            subsampling_rate=0.5,
            optimize_doc_concentration=False,
            sample_fraction=1.0,
            holdout_fraction=0.25,
            seed=42,
        )

    assert set(df.columns) == {
        "k", "max_iter", "learning_offset", "learning_decay",
        "subsampling_rate", "train_n", "holdout_n",
        "log_perplexity", "log_likelihood",
    }
    # One row per grid cell (2 iters x 2 offsets).
    assert len(df) == 4
    assert set(df["max_iter"]) == {2, 4}
    assert set(df["learning_offset"]) == {16.0, 64.0}
    assert (df["k"] == 2).all()
    # Held-out perplexity must be finite and positive at every cell;
    # split is non-empty.
    assert df["log_perplexity"].map(math.isfinite).all()
    assert (df["log_perplexity"] > 0).all()
    assert df["log_likelihood"].map(math.isfinite).all()
    assert (df["train_n"] > 0).all() and (df["holdout_n"] > 0).all()
