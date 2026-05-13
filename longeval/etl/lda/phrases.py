"""Spark-native NPMI bigram phrase mining.

Replaces the prior driver-side gensim ``Phrases`` step. Counts unigrams and
adjacent bigrams in pure Spark SQL, scores each candidate with NPMI, and
emits a parquet of ``(a, b, npmi, count)`` rows. Apply step broadcasts the
pair set and greedy-joins adjacent tokens in a Python UDF.

NPMI(a, b) = log(p(a, b) / (p(a) * p(b))) / -log(p(a, b))
           bounded in [-1, 1]; 1 means a and b only ever appear together.
"""

from __future__ import annotations

from pyspark.ml.feature import NGram
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fit_phrases(
    tokens_df: DataFrame,
    tokens_col: str = "tokens",
    min_count: int = 20,
    threshold: float = 0.5,
) -> DataFrame:
    """Mine NPMI bigrams over ``tokens_df``.

    Args:
        tokens_df: DataFrame with an ``array<string>`` column of analyzed tokens.
        tokens_col: Name of that column.
        min_count: Drop unigrams and bigrams seen fewer times than this.
        threshold: Minimum NPMI for a bigram to survive.

    Returns:
        DataFrame[a string, b string, npmi double, count long] of surviving
        phrases, sorted by ``npmi`` descending.
    """
    tokens = F.col(tokens_col)

    # Count unigrams once, then derive both the corpus token total (used as
    # the NPMI denominator) and a filtered view used only as a join key for
    # scoring. Filtering before taking the total would make the NPMI value
    # depend on `min_count`, which is a parameter of the cutoff, not a
    # property of the corpus.
    unigrams_all = (
        tokens_df.select(F.explode(tokens).alias("w"))
        .groupBy("w")
        .count()
        .withColumnRenamed("count", "w_count")
        .cache()
    )
    total_unigrams = unigrams_all.agg(F.sum("w_count")).first()[0] or 0
    if total_unigrams == 0:
        return tokens_df.sql_ctx.createDataFrame(
            [], "a string, b string, npmi double, count long"
        )
    unigrams = unigrams_all.filter(F.col("w_count") >= min_count)

    # Zip tokens with the same array shifted left by one to build (a, b) pairs.
    # slice(arr, 2, size(arr) - 1) drops the first element; arrays_zip pads
    # with NULL on the shorter side, which we then drop.
    n = F.size(tokens)
    pairs = F.arrays_zip(
        F.slice(tokens, F.lit(1), n - 1).alias("a"),
        F.slice(tokens, F.lit(2), n - 1).alias("b"),
    )
    bigrams = (
        tokens_df.filter(n >= 2)
        .select(F.explode(pairs).alias("p"))
        .select(F.col("p.a").alias("a"), F.col("p.b").alias("b"))
        .filter(F.col("a").isNotNull() & F.col("b").isNotNull())
        .groupBy("a", "b")
        .count()
        .withColumnRenamed("count", "ab_count")
        .filter(F.col("ab_count") >= min_count)
    )
    # NPMI's [-1, 1] boundedness requires a consistent denominator. Use the
    # total unigram count for all three probabilities: a bigram's joint prob
    # is `ab_count / total_unigrams`, matching the unigram marginals.
    scored = (
        bigrams.join(
            unigrams.withColumnRenamed("w", "a").withColumnRenamed(
                "w_count", "a_count"
            ),
            on="a",
        )
        .join(
            unigrams.withColumnRenamed("w", "b").withColumnRenamed(
                "w_count", "b_count"
            ),
            on="b",
        )
        .withColumn("p_ab", F.col("ab_count") / F.lit(total_unigrams))
        .withColumn("p_a", F.col("a_count") / F.lit(total_unigrams))
        .withColumn("p_b", F.col("b_count") / F.lit(total_unigrams))
        .withColumn(
            "npmi",
            F.log(F.col("p_ab") / (F.col("p_a") * F.col("p_b")))
            / -F.log(F.col("p_ab")),
        )
        .filter(F.col("npmi") >= threshold)
        .select(
            "a",
            "b",
            "npmi",
            F.col("ab_count").alias("count"),
        )
        .orderBy(F.col("npmi").desc())
    )
    return scored


def apply_phrases(
    df: DataFrame,
    phrases_df: DataFrame,
    id_col: str = "docid",
    input_col: str = "tokens",
    output_col: str = "tokens_phrased",
    separator: str = "_",
) -> DataFrame:
    """Augment tokens with surviving NPMI bigrams.

    Uses ``ml.feature.NGram`` to materialize adjacent bigrams per doc, then a
    broadcast hash join against the phrase set to keep only the survivors,
    then concatenates the kept bigrams (as ``a<sep>b`` tokens) back onto the
    original tokens array.

    Semantics: original unigrams are *preserved* (not replaced). A doc whose
    tokens contain "develop durabl" will end up with ``["develop", "durabl",
    "develop_durabl"]``. CountVectorizer / ``maxDF`` downstream handle any
    double-counting concerns. This trades exact replace-on-match semantics
    for a fully Spark-native pipeline (no Python UDF, no driver collect).

    Caller must supply a stable unique ``id_col`` in ``df`` (default
    ``docid``); we re-aggregate by it after the explode+join. This avoids
    the cache-the-DataFrame-to-pin-monotonic-id-assignment pattern.
    """
    bigram_col = "__doc_bigrams"
    df_b = NGram(n=2, inputCol=input_col, outputCol=bigram_col).transform(df)

    keys = phrases_df.select(
        F.concat_ws(" ", "a", "b").alias("phrase_key"),
        F.concat_ws(separator, "a", "b").alias("phrase_token"),
    )

    matched = (
        df_b.select(id_col, F.explode(bigram_col).alias("phrase_key"))
        .join(F.broadcast(keys), on="phrase_key", how="inner")
        .groupBy(id_col)
        .agg(F.collect_list("phrase_token").alias("__kept_bigrams"))
    )

    return (
        df_b.join(matched, on=id_col, how="left")
        .withColumn(
            output_col,
            F.concat(
                F.col(input_col),
                F.coalesce(
                    F.col("__kept_bigrams"),
                    F.array().cast("array<string>"),
                ),
            ),
        )
        .drop(bigram_col, "__kept_bigrams")
    )
