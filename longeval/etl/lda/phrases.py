"""Spark-native NPMI bigram phrase mining.

Replaces the prior driver-side gensim ``Phrases`` step. Counts unigrams and
adjacent bigrams in pure Spark SQL, scores each candidate with NPMI, and
emits a parquet of ``(a, b, npmi, count)`` rows. Apply step broadcasts the
pair set and greedy-joins adjacent tokens in a Python UDF.

NPMI(a, b) = log(p(a, b) / (p(a) * p(b))) / -log(p(a, b))
           bounded in [-1, 1]; 1 means a and b only ever appear together.

Surviving bigrams are additionally screened for *degenerate fixed-list*
artifacts: a pair that is near-perfectly collocated (npmi -> 1) and in the
extreme upper tail of co-occurrence frequency is a scraped UI dropdown
chain (country / currency / car-brand / departement selectors), not a
phrase. The frequency cut is corpus-derived (a percentile of the count
distribution over the high-npmi subset), so it is scale-invariant.
"""

from __future__ import annotations

from pyspark.ml.feature import NGram
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fit_phrases(
    tokens_df: DataFrame,
    output_path: str,
    *,
    tokens_col: str = "tokens",
    min_count: int = 20,
    threshold: float = 0.5,
    npmi_ceiling: float = 0.95,
    count_pctile: float = 99.9,
) -> None:
    """Mine NPMI bigrams over ``tokens_df`` and write to ``output_path``.

    Writes a parquet of ``(a, b, npmi, count)`` rows sorted by NPMI
    descending. Doing the write inside this function lets us bracket
    the ``unigrams_all`` cache with ``try/finally`` — the cache is
    needed by both the corpus-total aggregation and the two scoring
    joins, but Spark would otherwise hold it in memory until the
    session closes.

    Args:
        tokens_df: DataFrame with an ``array<string>`` column of analyzed tokens.
        output_path: Destination parquet path. Overwritten on each call.
        tokens_col: Name of the token-array column on ``tokens_df``.
        min_count: Drop unigrams and bigrams seen fewer times than this.
        threshold: Minimum NPMI for a bigram to survive.
        npmi_ceiling: A surviving pair with ``npmi >= npmi_ceiling`` is a
            degenerate-list *candidate* (only rejected if also in the
            count tail). Set ``> 1.0`` to disable the screen entirely
            (no pair can exceed npmi 1, so nothing is rejected).
        count_pctile: Percentile (0-100) of the ``count`` distribution
            over the ``npmi >= npmi_ceiling`` subset; candidates whose
            count is at/above that percentile are rejected as scraped
            dropdown-list chains. Corpus-derived at fit time, hence
            scale-invariant (an absolute floor would differ between
            ``sample_fraction`` 0.05 and 1.0).
    """
    tokens = F.col(tokens_col)
    spark = tokens_df.sparkSession
    phrases_schema = "a string, b string, npmi double, count long"

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
    try:
        total_unigrams = unigrams_all.agg(F.sum("w_count")).first()[0] or 0
        if total_unigrams == 0:
            (
                spark.createDataFrame([], phrases_schema)
                .write.mode("overwrite")
                .parquet(output_path)
            )
            return
        unigrams = unigrams_all.filter(F.col("w_count") >= min_count)

        # Zip tokens with the same array shifted left by one to build
        # (a, b) pairs. slice(arr, 2, size(arr) - 1) drops the first
        # element; arrays_zip pads with NULL on the shorter side, which
        # we then drop.
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
        # NPMI's [-1, 1] boundedness requires a consistent denominator.
        # Use the total unigram count for all three probabilities: a
        # bigram's joint prob is ``ab_count / total_unigrams``, matching
        # the unigram marginals.
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
            .select("a", "b", "npmi", F.col("ab_count").alias("count"))
            .cache()
        )
        try:
            # Degenerate fixed-list rejection. A pair that is
            # near-perfectly collocated (npmi -> 1) *and* in the extreme
            # upper tail of co-occurrence frequency is a scraped UI
            # dropdown chain (country / currency / car-brand /
            # departement selectors), not a meaningful phrase. Real
            # frequent phrases sit at moderate npmi (promiscuous
            # component words); real high-npmi phrases are low-count.
            # The frequency cut is a percentile of the count
            # distribution over the high-npmi subset, derived from the
            # corpus at fit time -- scale-invariant, unlike an absolute
            # floor. approxQuantile is one Spark action; caching
            # ``scored`` keeps the bigram/join pipeline from recomputing
            # for the subsequent write. Empty high-npmi subset ->
            # approxQuantile returns [] -> no rejection.
            hi = scored.filter(F.col("npmi") >= npmi_ceiling)
            q = hi.approxQuantile("count", [count_pctile / 100.0], 1e-3)
            cut = q[0] if q else None
            out = scored
            if cut is not None:
                out = scored.filter(
                    ~(
                        (F.col("npmi") >= npmi_ceiling)
                        & (F.col("count") >= cut)
                    )
                )
            (
                out.orderBy(F.col("npmi").desc())
                .write.mode("overwrite")
                .parquet(output_path)
            )
        finally:
            scored.unpersist()
    finally:
        unigrams_all.unpersist()


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
