"""Tests for longeval.etl.lda.coherence.

Split into two layers:
  - Pure-Python tests on ``_topic_npmi`` and ``_topic_diversity``: cheap,
    hand-computable, cover the math edge cases.
  - Spark end-to-end tests on ``doc_level_npmi_coherence``: a tiny
    in-memory corpus with hand-verified expected values, plus a
    regression test for the Pi-review denominator bug.
"""

import math

import pytest

from longeval.etl.lda.coherence import (
    _topic_diversity,
    _topic_npmi,
    doc_level_npmi_coherence,
)
from longeval.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    return get_spark(cores=2, name="pytest-lda-coherence")


# ---------------------------------------------------------------------------
# Pure-Python helpers
# ---------------------------------------------------------------------------


def test_topic_diversity_disjoint():
    assert _topic_diversity([["a", "b", "c"], ["d", "e", "f"]]) == 1.0


def test_topic_diversity_full_overlap():
    # Two topics with identical word lists: only half the slots are unique.
    assert _topic_diversity([["a", "b"], ["a", "b"]]) == 0.5


def test_topic_diversity_empty():
    assert _topic_diversity([]) == 0.0
    assert _topic_diversity([[]]) == 0.0


def test_topic_npmi_single_word_topic_is_zero():
    # No pairs to score => coherence undefined; convention is 0.
    assert _topic_npmi(["a"], {"a": 0.5}, {}) == 0.0


def test_topic_npmi_perfect_correlation_is_one():
    # When p(a, b) == p(a) == p(b), PMI = -log(p) and NPMI = 1 exactly.
    p_w = {"a": 0.1, "b": 0.1}
    p_ab = {("a", "b"): 0.1}
    assert _topic_npmi(["a", "b"], p_w, p_ab) == pytest.approx(1.0, abs=1e-9)


def test_topic_npmi_never_cooccur_is_strongly_negative():
    # Eps-smoothed branch: pairs that never co-occur get a large negative
    # NPMI (bounded above -1 since the joint is eps, not zero).
    p_w = {"a": 0.1, "b": 0.1}
    p_ab = {}
    score = _topic_npmi(["a", "b"], p_w, p_ab)
    assert -1.0 < score < -0.5


def test_topic_npmi_skips_missing_marginal():
    # A topic word that doesn't appear in p_w (e.g. analyzer mismatch)
    # contributes nothing rather than dividing by zero.
    p_w = {"a": 0.1}  # "b" missing
    p_ab = {}
    assert _topic_npmi(["a", "b"], p_w, p_ab) == 0.0


# ---------------------------------------------------------------------------
# Spark end-to-end
# ---------------------------------------------------------------------------


def _docs_df(spark, docs):
    return spark.createDataFrame(
        [(d,) for d in docs],
        "tokens_phrased: array<string>",
    )


def test_doc_level_smoke_matches_hand_computed(spark):
    """5-doc corpus with hand-verified NPMI values per topic.

    Corpus: [a,b,c], [a,b], [c,d], [e], [].
    Vocab counts: a=2, b=2, c=2, d=1, e=1; n_docs=5 (incl. empty).

    Topic 1 [a, b, c]:
        NPMI(a,b) = log(0.4/0.16)/-log(0.4) = 1.0      (a,b co-occur in 2/2 a-docs)
        NPMI(a,c) = log(0.2/0.16)/-log(0.2) ≈ 0.139
        NPMI(b,c) = NPMI(a,c) by symmetry  ≈ 0.139
        mean ≈ 0.426
    Topic 2 [c, d, e]:
        NPMI(c,d) ≈ 0.569
        NPMI(c,e), NPMI(d,e) hit eps-smoothed branch ≈ -0.91, -0.88
        mean ≈ -0.408
    """
    df = _docs_df(spark, [["a", "b", "c"], ["a", "b"], ["c", "d"], ["e"], []])
    out = doc_level_npmi_coherence(
        df, topic_words=[["a", "b", "c"], ["c", "d", "e"]], top_n=3
    )

    assert out["n_docs"] == 5
    assert out["topic_diversity"] == pytest.approx(5 / 6)
    assert out["per_topic_npmi"][0] == pytest.approx(0.426, abs=0.01)
    assert out["per_topic_npmi"][1] == pytest.approx(-0.408, abs=0.01)
    assert out["mean_npmi"] == pytest.approx(
        (out["per_topic_npmi"][0] + out["per_topic_npmi"][1]) / 2
    )


def test_denominator_is_total_docs_not_topic_word_docs(spark):
    """Regression test for the Pi-review denominator bug.

    If NPMI used ``restricted.count()`` (docs containing any topic word)
    as the denominator, adding noise docs without topic-word overlap
    would not change the score — they'd be filtered out before counting.
    With the correct ``tokens_df.count()`` denominator, the marginals
    shrink and the NPMI score must shift.

    The design here flips the sign: in the small corpus a/b are
    *under-correlated* relative to chance (each appears alone in some
    doc), giving negative NPMI. Adding 3 noise docs makes a/b appear
    *more* correlated than chance (joint shrinks slower than the product
    of marginals), flipping the score positive.
    """
    base = [["a", "b"], ["a"], ["b"]]
    noise = [["z1"], ["z2"], ["z3"]]
    topics = [["a", "b"]]

    out_small = doc_level_npmi_coherence(_docs_df(spark, base), topics, top_n=2)
    out_large = doc_level_npmi_coherence(
        _docs_df(spark, base + noise), topics, top_n=2
    )

    # Sanity: n_docs reflects the *input* corpus, not the filtered one.
    assert out_small["n_docs"] == 3
    assert out_large["n_docs"] == 6

    # Hand-computed: small corpus has p(a)=p(b)=2/3, p(a,b)=1/3 →
    # PMI=log(0.75)≈-0.288, NPMI≈-0.262. Large corpus halves marginals
    # and joint → p(a)=p(b)=1/3, p(a,b)=1/6, PMI=log(1.5)≈0.405,
    # NPMI≈0.226.
    assert out_small["per_topic_npmi"][0] == pytest.approx(-0.262, abs=0.01)
    assert out_large["per_topic_npmi"][0] == pytest.approx(0.226, abs=0.01)


def test_empty_inputs_return_zeros(spark):
    df = _docs_df(spark, [[], [], []])
    out = doc_level_npmi_coherence(df, topic_words=[["a", "b"]], top_n=2)
    # No vocab overlap with any doc → vocab still resolves but counts are 0.
    assert out["n_docs"] == 3
    # Both marginals end up 0; pair is skipped, yielding 0.0 mean.
    assert out["per_topic_npmi"] == [0.0]
    assert out["mean_npmi"] == 0.0


def test_no_topic_words_short_circuits(spark):
    df = _docs_df(spark, [["a"], ["b"]])
    out = doc_level_npmi_coherence(df, topic_words=[], top_n=3)
    assert out == {
        "mean_npmi": 0.0,
        "per_topic_npmi": [],
        "topic_diversity": 0.0,
        "n_docs": 0,
    }
