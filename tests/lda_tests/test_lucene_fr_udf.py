"""Equivalence guard for the native Lucene-FR analyzer UDF.

The native ``lucene_fr_analyze`` SQL UDF replaced the per-Python-worker
pyjnius ``_analyze`` UDF to kill the embedded-JVM-per-worker memory
multiplier. Correctness rests on it being *byte-identical* to the old
path — same ``FrenchAnalyzer`` + ``AnalyzerUtils.analyze`` bytecode from
the same anserini fatjar, just invoked in the executor JVM. If it ever
diverges, vocab and coherence regress silently against every prior
baseline, so this pins the invariant on inputs that exercise the French
elision/accent/digit/empty edges where an invocation mismatch
(encoding, null handling, token order) would surface.

``spark.jars`` is honored only on the JVM that launches the first
SparkContext in a process. pyspark reuses one JVM per process, so an
in-process test running after another Spark test module would inherit a
jar-less JVM and fail to load the UDF class. The comparison therefore
runs in a dedicated subprocess (clean JVM, order-independent); the test
just asserts that subprocess succeeds. Production is unaffected — each
``longeval`` CLI run is a fresh process whose first ``spark_resource``
(MinePhrases) launches with the jars.
"""

import subprocess
import sys

import pytest

CASES = [
    "Le chat noir mange la souris.",
    "L'eau de la rivière coule aujourd'hui qu'est-ce",
    "Élève à l'école, après-midi, naïve, çà et là, œuf",
    "Données 2022 COVID-19 numéro 42 100% taux",
    "MAJUSCULES et minuscules MÉLANGÉES",
    "English filler the and of with French mélange",
    "ponctuation !!! ... ?? «citation» — tiret",
    "",
    "   ",
    "a",
    "aujourd'hui c'est l'anniversaire de l'enfant d'Amélie",
    "Saint-Étienne Aix-en-Provence porte-monnaie",
    "café\tthé\nlait   espaces   multiples",
    "123 456 7890 numérique pur 0042",
    "Ça, c'est l'œuvre d'un génie français: l'État.",
]


def _run_comparison() -> int:
    """In a clean JVM: native UDF vs the exact deleted ``_analyze`` path
    over CASES + a NULL row. Returns 0 iff byte-identical."""
    from pyserini.analysis import Analyzer, get_lucene_analyzer

    from longeval.etl.lda.workflow import _ANALYZER_UDF_NAME, _analyzer_jars
    from longeval.spark import get_spark

    spark = get_spark(
        cores=1,
        memory="2g",
        app_name="pytest-lucene-fr-udf",
        **{"spark.jars": _analyzer_jars()},
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.udf.registerJavaFunction(
        _ANALYZER_UDF_NAME, "com.longeval.LuceneFrAnalyzerUDF", "array<string>"
    )
    try:
        df = spark.createDataFrame(
            [(i, t) for i, t in enumerate(CASES)], ["i", "c"]
        )
        rows = (
            df.selectExpr("i", f"{_ANALYZER_UDF_NAME}(c) AS toks")
            .orderBy("i")
            .collect()
        )
        got = [list(r.toks) for r in rows]
        # Mirrors the deleted _analyze: None/falsy -> "".
        ref = Analyzer(get_lucene_analyzer(language="fr"))
        expected = [ref.analyze(t or "") for t in CASES]

        ok = True
        for case, g, e in zip(CASES, got, expected):
            if g != e:
                ok = False
                print(f"MISMATCH {case!r}\n native={g}\n pyserini={e}")

        # SQL NULL must analyze to [] (old _analyze did `text or ""`).
        ndf = spark.createDataFrame([(1, None)], "i int, c string")
        [nrow] = ndf.selectExpr(f"{_ANALYZER_UDF_NAME}(c) AS toks").collect()
        if list(nrow.toks) != []:
            ok = False
            print(f"NULL MISMATCH: {list(nrow.toks)!r} != []")
        return 0 if ok else 1
    finally:
        spark.stop()


def test_native_udf_matches_pyserini():
    proc = subprocess.run(
        [sys.executable, __file__],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, (
        "native UDF diverged from pyserini path:\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr[-2000:]}"
    )


if __name__ == "__main__":
    sys.exit(_run_comparison())
