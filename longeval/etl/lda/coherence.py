"""Document-level NPMI topic coherence for LDA model selection.

Computes coherence by counting how often each pair among a topic's top-N
words co-occur in the same document, then averaging NPMI across pairs and
across topics.

This is a document-window approximation of c_npmi (Röder et al. 2015),
which uses a sliding length-10 window. The document approximation
preserves the *ranking* of models by K while being far cheaper in Spark:
shuffle volume scales with documents instead of tokens.

Probability denominator is the total number of input docs — *not* the
number of docs containing some topic word — so scores remain comparable
across different K values.
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


def doc_level_npmi_coherence(
    tokens_df: DataFrame,
    topic_words: list[list[str]],
    tokens_col: str = "tokens_phrased",
    top_n: int = 10,
) -> dict:
    """Doc-level NPMI coherence + diversity for a topic model.

    Args:
        tokens_df: DataFrame with an array<string> token column, one row per
            doc. Tokens must match the model's vocabulary (same analyzer,
            same phrasing) or pairs will not be found.
        topic_words: For each topic, the ordered top-K words. Truncated to
            ``top_n`` internally.
        tokens_col: Name of the token-array column on ``tokens_df``.
        top_n: Number of top words per topic to score (default 10).

    Returns:
        ``{"mean_npmi", "per_topic_npmi", "topic_diversity", "n_docs"}``.
        ``topic_diversity`` is the fraction of unique words across all
        top-N lists, in [0, 1]; values near 1 mean topics share little
        vocabulary, near 0 mean they're degenerate copies of one topic.
        ``n_docs`` is the total eval-corpus document count (the NPMI
        denominator), not the number of docs containing a topic word.
    """
    topic_words = [list(t[:top_n]) for t in topic_words]
    vocab = sorted({w for t in topic_words for w in t})
    diversity = _topic_diversity(topic_words)
    if not vocab:
        return {
            "mean_npmi": 0.0,
            "per_topic_npmi": [0.0] * len(topic_words),
            "topic_diversity": diversity,
            "n_docs": 0,
        }

    # Pre-compute the exact (a, b) pairs any topic asks us to score. The
    # union vocab can produce ~C(|V|, 2) pairs (≈1.3M for K=160) but only
    # K * C(top_n, 2) of them (≈7.2k for K=160) are actually scored.
    # Filtering during the explode keeps shuffle proportional to the
    # latter, not the former. Pairs are stored in canonical sorted order.
    needed_pairs = set()
    for topic in topic_words:
        for i in range(len(topic)):
            for j in range(i + 1, len(topic)):
                a, b = topic[i], topic[j]
                needed_pairs.add((a, b) if a < b else (b, a))

    spark = tokens_df.sparkSession
    needed_pairs_bc = spark.sparkContext.broadcast(needed_pairs)

    # Push the vocab filter into Spark SQL via array_intersect against a
    # broadcast literal. array_distinct gives doc-frequency semantics: a
    # word in a doc counts once regardless of in-doc token frequency.
    # array_sort lets the pair UDF rely on v[i] < v[j].
    vocab_array = F.array(*[F.lit(w) for w in vocab])
    restricted = (
        tokens_df.select(
            F.array_sort(
                F.array_distinct(F.array_intersect(F.col(tokens_col), vocab_array))
            ).alias("v")
        )
        .cache()
    )

    # NPMI probability denominator: *all* docs, including ones with no
    # topic-word overlap. Counting only the non-empty restricted set would
    # condition probabilities on "contains a top word", which is a
    # K-dependent baseline and breaks cross-K comparison.
    n_docs = restricted.count()
    if n_docs == 0:
        restricted.unpersist()
        return {
            "mean_npmi": 0.0,
            "per_topic_npmi": [0.0] * len(topic_words),
            "topic_diversity": diversity,
            "n_docs": 0,
        }

    nonempty = restricted.filter(F.size("v") > 0)

    unigram_rows = (
        nonempty.select(F.explode("v").alias("w"))
        .groupBy("w")
        .count()
        .collect()
    )
    p_w = {row.w: row["count"] / n_docs for row in unigram_rows}

    pair_schema = ArrayType(
        StructType(
            [StructField("a", StringType()), StructField("b", StringType())]
        )
    )

    @F.udf(pair_schema)
    def emit_pairs(v):
        if not v or len(v) < 2:
            return []
        pairs = needed_pairs_bc.value
        out = []
        for i in range(len(v)):
            wi = v[i]
            for j in range(i + 1, len(v)):
                wj = v[j]
                # v is array_sort-ed so (wi, wj) is already canonical.
                if (wi, wj) in pairs:
                    out.append((wi, wj))
        return out

    pair_rows = (
        nonempty.select(F.explode(emit_pairs("v")).alias("p"))
        .select(F.col("p.a").alias("a"), F.col("p.b").alias("b"))
        .groupBy("a", "b")
        .count()
        .collect()
    )
    p_ab = {(row.a, row.b): row["count"] / n_docs for row in pair_rows}

    restricted.unpersist()

    per_topic = [_topic_npmi(words, p_w, p_ab) for words in topic_words]
    return {
        "mean_npmi": sum(per_topic) / len(per_topic) if per_topic else 0.0,
        "per_topic_npmi": per_topic,
        "topic_diversity": diversity,
        "n_docs": n_docs,
    }


def _topic_npmi(words, p_w, p_ab):
    """Mean NPMI over all unordered pairs in ``words``.

    Uses Gensim-style epsilon smoothing on the joint probability so that
    pairs which never co-occur in the corpus contribute a strongly
    negative (but finite) score rather than blowing up the log.
    """
    if len(words) < 2:
        return 0.0
    eps = 1e-12
    scores = []
    for i, wi in enumerate(words):
        pi = p_w.get(wi, 0.0)
        for wj in words[i + 1 :]:
            pj = p_w.get(wj, 0.0)
            a, b = (wi, wj) if wi < wj else (wj, wi)
            p_ij = p_ab.get((a, b), 0.0)
            if pi <= 0 or pj <= 0:
                continue
            p_ij_s = p_ij + eps
            pmi = math.log(p_ij_s / (pi * pj))
            npmi = pmi / -math.log(p_ij_s)
            scores.append(npmi)
    return sum(scores) / len(scores) if scores else 0.0


def _topic_diversity(topic_words):
    total = sum(len(t) for t in topic_words)
    if total == 0:
        return 0.0
    unique = len({w for t in topic_words for w in t})
    return unique / total
