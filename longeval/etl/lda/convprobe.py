"""Online-LDA convergence probe.

The sweep trains at ``maxIter=10`` / ``learningOffset=1024`` /
``subsamplingRate=0.05``. With N=16.26M that is ~0.5 epochs, and the
step-size schedule rho_t = (learningOffset + t)^(-learningDecay) is
*flat* at ~0.03 for t in 0..10 because the 1024 offset dwarfs t — the
global topic matrix barely moves from its random init. So the models
are under-trained at every K (most acutely at large K, which must
populate more topics from the same few updates). Before committing the
expensive converged retrain we need to *measure* where training
actually plateaus rather than guess a number.

This fits one K on a subsample of the existing ``features.parquet`` over
a grid of ``(max_iter x learning_offset)``, holding out a fraction of
docs, and reports the held-out ``logPerplexity`` (lower = better, the
generalisation bound) and ``logLikelihood``. Two things fall out at
once: the ``max_iter`` at which held-out perplexity stops improving
(the converged iteration count), and whether dropping
``learning_offset`` so the SVI schedule actually decays reaches a
better optimum in the same budget. A subsample preserves the
convergence *shape* (the plateau location) at a fraction of the cost —
the probe locates a setting, it does not produce a final model.
"""

import os

import pandas as pd
import typer
from typing_extensions import Annotated

from longeval.etl.lda.workflow import _build_lda
from longeval.spark import spark_resource


def convergence_probe(
    spark,
    preprocess_path,
    k,
    iters,
    learning_offsets,
    learning_decay,
    subsampling_rate,
    optimize_doc_concentration,
    sample_fraction,
    holdout_fraction,
    seed,
):
    """Fit ``k`` over the ``iters x learning_offsets`` grid on a
    train/holdout split of a subsample of ``features.parquet``; return a
    pandas frame of held-out perplexity/likelihood per cell.
    """
    features = spark.read.parquet(
        os.path.join(preprocess_path, "features.parquet")
    )
    if sample_fraction < 1.0:
        features = features.sample(
            fraction=float(sample_fraction), seed=int(seed)
        )
    train, holdout = features.randomSplit(
        [1.0 - float(holdout_fraction), float(holdout_fraction)],
        seed=int(seed),
    )
    train = train.cache()
    holdout = holdout.cache()
    try:
        train_n, holdout_n = train.count(), holdout.count()
        rows = []
        for lo in learning_offsets:
            for mi in iters:
                lda = _build_lda(
                    k=int(k),
                    max_iter=int(mi),
                    seed=int(seed),
                    learning_offset=float(lo),
                    learning_decay=float(learning_decay),
                    subsampling_rate=float(subsampling_rate),
                    optimize_doc_concentration=optimize_doc_concentration,
                )
                model = lda.fit(train)
                # logPerplexity is a per-token upper bound on the
                # negative held-out log-likelihood — lower is better and
                # it is comparable across K (logLikelihood is not).
                lp = model.logPerplexity(holdout)
                ll = model.logLikelihood(holdout)
                rows.append(
                    {
                        "k": int(k),
                        "max_iter": int(mi),
                        "learning_offset": float(lo),
                        "learning_decay": float(learning_decay),
                        "subsampling_rate": float(subsampling_rate),
                        "train_n": int(train_n),
                        "holdout_n": int(holdout_n),
                        "log_perplexity": float(lp),
                        "log_likelihood": float(ll),
                    }
                )
                print(
                    f"probe k={k} max_iter={mi} learning_offset={lo} "
                    f"-> logPerplexity={lp:.5f} logLikelihood={ll:.1f}"
                )
        return pd.DataFrame(rows)
    finally:
        train.unpersist()
        holdout.unpersist()


def conv_probe_main(
    preprocess_path: Annotated[
        str,
        typer.Argument(
            help="Preprocess dir with features.parquet, e.g. "
            "/mnt/data/tmp/lda-all9-clean-sweep/preprocess"
        ),
    ],
    output_path: Annotated[
        str,
        typer.Argument(help="Dir for conv_probe.parquet / .csv"),
    ],
    k: Annotated[int, typer.Option(help="K to probe")] = 20,
    iters: Annotated[
        str, typer.Option(help="Comma-separated maxIter grid")
    ] = "10,25,50,100",
    learning_offsets: Annotated[
        str,
        typer.Option(
            help="Comma-separated learningOffset grid; 1024 = the "
            "current (flat-schedule) setting, lower engages decay"
        ),
    ] = "1024,64",
    learning_decay: Annotated[float, typer.Option()] = 0.51,
    subsampling_rate: Annotated[float, typer.Option()] = 0.05,
    optimize_doc_concentration: Annotated[bool, typer.Option()] = True,
    sample_fraction: Annotated[
        float,
        typer.Option(help="Subsample of features (shape-preserving)"),
    ] = 0.15,
    holdout_fraction: Annotated[float, typer.Option()] = 0.1,
    seed: Annotated[int, typer.Option()] = 42,
):
    """Locate the online-LDA convergence plateau (held-out perplexity
    vs maxIter x learningOffset) before the converged retrain."""
    its = [int(x) for x in iters.split(",") if x.strip()]
    los = [float(x) for x in learning_offsets.split(",") if x.strip()]
    with spark_resource() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        df = convergence_probe(
            spark,
            preprocess_path,
            k,
            its,
            los,
            learning_decay,
            subsampling_rate,
            optimize_doc_concentration,
            sample_fraction,
            holdout_fraction,
            seed,
        )
    os.makedirs(output_path, exist_ok=True)
    df = df.sort_values(["learning_offset", "max_iter"]).reset_index(
        drop=True
    )
    df.to_parquet(os.path.join(output_path, "conv_probe.parquet"))
    df.to_csv(os.path.join(output_path, "conv_probe.csv"), index=False)
    print("\n=== convergence probe (lower log_perplexity = better) ===")
    print(df.to_string(index=False))
