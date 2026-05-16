"""LDA topic modeling pipeline.

Runs on a single-driver Spark session (``local[N]`` via ``longeval.spark``).
Scaling to a real cluster requires changes to ``longeval/spark.py`` — every
task here opens its own ``spark_resource()`` context.
"""

import functools
import glob
import hashlib
import json
import os

import luigi
import matplotlib.pyplot as plt
import pandas as pd
import typer
from pyspark.ml.clustering import LDA, LocalLDAModel
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, StopWordsRemover
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import functions as F
from sklearn.decomposition import PCA
from sklearn.random_projection import GaussianRandomProjection
from typing_extensions import Annotated

from longeval.collection import ParquetCollection
from longeval.etl.lda.coherence import doc_level_npmi_coherence
from longeval.etl.lda.phrases import apply_phrases, fit_phrases
from longeval.spark import spark_resource


def _write_stamp(path: str, config: dict) -> None:
    """Write a task-completion stamp to ``path`` as sorted JSON."""
    with open(path, "w") as f:
        json.dump(config, f, indent=2, sort_keys=True)


def _stamp_matches(path: str, expected: dict) -> bool:
    """True iff ``path`` exists and contains ``expected`` verbatim."""
    if not os.path.exists(path):
        return False
    try:
        with open(path) as f:
            return json.load(f) == expected
    except (OSError, json.JSONDecodeError):
        return False


def _canonical_hash(cfg: dict) -> str:
    """SHA-256 of a config dict's canonical (sorted-key) JSON."""
    return hashlib.sha256(
        json.dumps(cfg, sort_keys=True).encode()
    ).hexdigest()


def _phrases_hash(task) -> str:
    """Hash of the *requested* MinePhrases config, derived from the
    task's own params — NOT read from the materialized on-disk stamp.

    Embedded in the BuildFeatures stamp so a phrase-param change
    invalidates BuildFeatures' ``complete()`` directly.
    """
    return _canonical_hash(
        {
            "input_path": str(task.input_path),
            "date": str(task.date),
            "sample_fraction": float(task.sample_fraction),
            "phrases_min_count": int(task.phrases_min_count),
            "phrases_threshold": float(task.phrases_threshold),
        }
    )


def _preprocess_hash(task) -> str:
    """Hash of the *requested* preprocessing config (phrases + vocab),
    derived from the task's own params — NOT read from the materialized
    on-disk preprocess stamp.

    Every per-K / root stamp embeds this. Keying off the *requested*
    config (not the last-built one) is what makes a preprocessing param
    change flip ``complete()`` at the leaves, so Luigi descends and
    re-runs BuildFeatures/MinePhrases instead of short-circuiting on a
    stale materialized hash.
    """
    return _canonical_hash(
        {
            "input_path": str(task.input_path),
            "date": str(task.date),
            "sample_fraction": float(task.sample_fraction),
            "min_df": float(task.min_df),
            "max_df": float(task.max_df),
            "vocab_size": int(task.vocab_size),
            "phrases_min_count": int(task.phrases_min_count),
            "phrases_threshold": float(task.phrases_threshold),
        }
    )


def _build_lda(
    k,
    max_iter,
    seed,
    learning_offset,
    learning_decay,
    subsampling_rate,
    optimize_doc_concentration,
):
    """Construct an online (variational) LDA estimator.

    Online is the only optimizer: EM's GraphX doc-term graph never scaled
    past a small sample (it OOM-killed the driver on the full snapshot),
    and online is PySpark's documented default with O(K*V) driver memory.
    """
    return (
        LDA(
            k=k,
            maxIter=max_iter,
            featuresCol="features",
            optimizer="online",
            seed=seed,
        )
        .setLearningOffset(learning_offset)
        .setLearningDecay(learning_decay)
        .setSubsamplingRate(subsampling_rate)
        .setOptimizeDocConcentration(optimize_doc_concentration)
    )


def _train_requirement(task):
    """Route a per-K consumer to the shared sweep trainer or the
    single-K trainer based on whether ``k_values`` is set.

    ``TrainLDASweep`` fits every K in one cached Spark session via
    ``fitMultiple`` and writes the exact per-K ``lda_model`` +
    ``_train_config.json`` layout ``TrainLDAModel`` would, so every per-K
    consumer in a sweep dedupes onto a single training node. With
    ``k_values=None`` the single-K path is byte-unchanged.
    """
    common = dict(
        input_path=task.input_path,
        preprocess_path=task.preprocess_path,
        date=task.date,
        sample_fraction=task.sample_fraction,
        min_df=task.min_df,
        max_df=task.max_df,
        vocab_size=task.vocab_size,
        seed=task.seed,
        max_iter=task.max_iter,
        phrases_min_count=task.phrases_min_count,
        phrases_threshold=task.phrases_threshold,
        learning_offset=task.learning_offset,
        learning_decay=task.learning_decay,
        subsampling_rate=task.subsampling_rate,
        optimize_doc_concentration=task.optimize_doc_concentration,
    )
    if task.k_values is not None:
        # The sweep root is the parent of the shared preprocess dir
        # (always <root>/preprocess), so every per-K consumer derives the
        # SAME TrainLDASweep regardless of its own per-K output_path —
        # Luigi dedupes them onto one training node.
        sweep_root = os.path.dirname(task.preprocess_path)
        return TrainLDASweep(
            output_path=sweep_root, k_values=task.k_values, **common
        )
    return TrainLDAModel(
        output_path=task.output_path, num_topics=task.num_topics, **common
    )


def _infer_requirement(task):
    """Inference analogue of ``_train_requirement``: route a per-K
    consumer to the shared ``InferLDASweep`` (one cached session, all K)
    or the single-K ``RunLDAInference``.

    ``InferLDASweep`` writes the exact per-K ``docTopicDistribution`` +
    ``_inference_config.json`` layout ``RunLDAInference`` would, so a
    sweep's consumers dedupe onto one inference node — no per-K JVM or
    features re-read. ``k_values=None`` keeps the single-K path unchanged.
    """
    common = dict(
        input_path=task.input_path,
        preprocess_path=task.preprocess_path,
        date=task.date,
        sample_fraction=task.sample_fraction,
        min_df=task.min_df,
        max_df=task.max_df,
        vocab_size=task.vocab_size,
        seed=task.seed,
        max_iter=task.max_iter,
        phrases_min_count=task.phrases_min_count,
        phrases_threshold=task.phrases_threshold,
        learning_offset=task.learning_offset,
        learning_decay=task.learning_decay,
        subsampling_rate=task.subsampling_rate,
        optimize_doc_concentration=task.optimize_doc_concentration,
    )
    if task.k_values is not None:
        sweep_root = os.path.dirname(task.preprocess_path)
        return InferLDASweep(
            output_path=sweep_root, k_values=task.k_values, **common
        )
    return RunLDAInference(
        output_path=task.output_path, num_topics=task.num_topics, **common
    )


def _build_lucene_fr_analyzer():
    """Return a fresh Lucene FR analyzer (lowercase + Snowball + FR stops).

    Driver-only: used once by ``_build_stop_stems`` to derive stopword
    stems. The per-row corpus tokenization no longer goes through this
    pyjnius path — see the native SQL UDF below. Boots one driver JVM,
    not one per Spark worker, so it is not the memory multiplier the old
    ``_analyze`` UDF was.
    """
    from pyserini.analysis import Analyzer, get_lucene_analyzer

    return Analyzer(get_lucene_analyzer(language="fr"))


_ANALYZER_UDF_NAME = "lucene_fr_analyze"


def _analyzer_jars() -> str:
    """Comma-separated ``spark.jars`` for the native FR analyzer UDF: our
    thin jar + the anserini fatjar pyserini ships.

    The fatjar bundles the exact ``FrenchAnalyzer`` + ``AnalyzerUtils``
    bytecode the old pyjnius path called, so executor-side tokenization
    is byte-identical — equivalence by shared bytecode, not by a
    reimplementation.
    """
    import pyserini

    our_jar = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "jvm",
            "longeval-analyzer.jar",
        )
    )
    if not os.path.exists(our_jar):
        raise FileNotFoundError(
            f"Native analyzer jar missing: {our_jar}. Build it once with "
            "`bash jvm/build.sh` (needs JAVA_HOME; .env has it)."
        )
    fatjars = glob.glob(
        os.path.join(
            os.path.dirname(pyserini.__file__),
            "resources", "jars", "anserini-*-fatjar.jar",
        )
    )
    if not fatjars:
        raise FileNotFoundError(
            "anserini fatjar not found under the pyserini package."
        )
    return f"{our_jar},{fatjars[0]}"


def _register_fr_analyzer(spark) -> None:
    """Register the executor-JVM Lucene-FR tokenizer as a SQL function.

    Replaces the old ``_analyze`` UDF, which booted an embedded pyjnius
    JVM in *each* Python worker (the local[N] memory multiplier). This
    runs in the single executor JVM that already exists.
    """
    spark.udf.registerJavaFunction(
        _ANALYZER_UDF_NAME,
        "com.longeval.LuceneFrAnalyzerUDF",
        ArrayType(StringType()),
    )


def _build_stop_stems() -> list[str]:
    """NLTK FR+EN stopwords passed through the FR analyzer to match the
    Snowball stems present in document tokens. Raw forms are unioned in
    as a fallback for tokens the analyzer leaves untouched.
    """
    import nltk

    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        nltk.download("stopwords", quiet=True)
    from nltk.corpus import stopwords

    analyzer = _build_lucene_fr_analyzer()
    raw = set(stopwords.words("french")) | set(stopwords.words("english"))
    stems: set[str] = set()
    for w in raw:
        stems.update(analyzer.analyze(w))
    stems |= raw
    return sorted(stems)


class MinePhrases(luigi.Task):
    """Analyze + NPMI-bigram the corpus — the expensive, vocab-independent
    half of preprocessing.

    The per-row Lucene analyze (a native executor-JVM SQL UDF — see
    ``_register_fr_analyzer``) is the pipeline's dominant compute cost and
    depends only on date/sample/phrase params, never on vocab pruning.
    Splitting it out so a min_df/max_df/vocab_size change reuses this pass
    instead of re-running it. Writes into the shared ``<root>/preprocess``
    dir (layout unchanged):

    - ``tokens_phrased.parquet`` — analyzed + phrase-augmented tokens per
      ``docid``; the load-bearing handoff BuildFeatures and coherence read.
    - ``phrases.parquet``        — NPMI bigram set (debug byproduct).
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def _config_dict(self):
        return {
            "task": "MinePhrases",
            "input_path": str(self.input_path),
            "date": str(self.date),
            "sample_fraction": float(self.sample_fraction),
            "phrases_min_count": int(self.phrases_min_count),
            "phrases_threshold": float(self.phrases_threshold),
        }

    def _config_path(self):
        return os.path.join(self.output_path, "_phrases_config.json")

    def complete(self):
        phrased = os.path.join(self.output_path, "tokens_phrased.parquet")
        if not os.path.exists(phrased):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def run(self):
        with spark_resource(**{"spark.jars": _analyzer_jars()}) as spark:
            spark.sparkContext.setLogLevel("ERROR")
            _register_fr_analyzer(spark)

            collection = ParquetCollection(spark, self.input_path)
            docs = collection.documents
            # date="all" pools every slice in the parquet into one
            # cross-slice corpus; any other value selects a single
            # slice. "all" threads through every stamp / _preprocess_hash
            # exactly like a real date, so pooled artifacts never collide
            # with per-date ones.
            if str(self.date) != "all":
                docs = docs.filter(F.col("date") == self.date)
            docs = docs.sample(fraction=self.sample_fraction, seed=42)
            # Cleanup: Lucene FR analyzer -> StopWordsRemover (NLTK FR+EN
            # stems) -> length/numeric filter. The extra StopWordsRemover
            # pass exists because the analyzer's built-in FR stops don't
            # cover English fillers ("the", "and") present in this corpus.
            remover = StopWordsRemover(
                inputCol="tokens_raw",
                outputCol="tokens_no_stop",
                stopWords=_build_stop_stems(),
            )
            # Cache the cleaned token stream: fit_phrases scans it twice
            # (unigram + bigram counts) and apply_phrases scans it once.
            # Project to docid + tokens so the full `contents` column
            # isn't pinned in memory.
            analyzed = (
                remover.transform(
                    docs.select(
                        "docid",
                        F.expr(
                            f"{_ANALYZER_UDF_NAME}(contents)"
                        ).alias("tokens_raw"),
                    )
                )
                .withColumn(
                    "tokens",
                    F.expr(
                        "filter(tokens_no_stop, t -> "
                        "length(t) >= 3 AND NOT (t rlike '^[0-9]+$'))"
                    ),
                )
                .select("docid", "tokens")
                .cache()
            )

            os.makedirs(self.output_path, exist_ok=True)
            phrases_path = os.path.join(self.output_path, "phrases.parquet")
            phrased_path = os.path.join(self.output_path, "tokens_phrased.parquet")
            try:
                fit_phrases(
                    analyzed,
                    output_path=phrases_path,
                    tokens_col="tokens",
                    min_count=self.phrases_min_count,
                    threshold=self.phrases_threshold,
                )
                phrases_df = spark.read.parquet(phrases_path)

                # Materialize phrased tokens once — the load-bearing
                # artifact BuildFeatures and EvaluateLDAModel read so the
                # JVM analyze never re-runs on a vocab-param change.
                (
                    apply_phrases(analyzed, phrases_df)
                    .select("docid", "tokens_phrased")
                    .write.mode("overwrite")
                    .parquet(phrased_path)
                )
            finally:
                analyzed.unpersist()

            # Stamp last so a partial run doesn't look complete.
            _write_stamp(self._config_path(), self._config_dict())
            print("MinePhrases: phrases + tokens_phrased written.")

    def output(self):
        return luigi.LocalTarget(self._config_path())


class BuildFeatures(luigi.Task):
    """CountVectorizer over MinePhrases' ``tokens_phrased`` — the
    vocab-dependent half of preprocessing.

    Given a fixed phrasing this depends only on min_df/max_df/vocab_size,
    so a vocab-param change re-runs only this and reuses the expensive
    analyze pass. Its stamp keeps the name ``_config.json`` (unchanged) so
    the downstream ``preprocess_hash`` cascade is untouched, and embeds
    the ``MinePhrases`` stamp hash so a phrase-param change propagates
    into ``preprocess_hash`` and cascades through every per-K task.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def requires(self):
        return MinePhrases(
            input_path=self.input_path,
            output_path=self.output_path,
            date=self.date,
            sample_fraction=self.sample_fraction,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def _config_dict(self):
        return {
            "task": "BuildFeatures",
            "min_df": float(self.min_df),
            "max_df": float(self.max_df),
            "vocab_size": int(self.vocab_size),
            "phrases_hash": _phrases_hash(self),
        }

    def _config_path(self):
        return os.path.join(self.output_path, "_config.json")

    def complete(self):
        features_path = os.path.join(self.output_path, "features.parquet")
        if not os.path.exists(features_path):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            phrased_path = os.path.join(self.output_path, "tokens_phrased.parquet")
            features_path = os.path.join(self.output_path, "features.parquet")

            # Cache phrased across vectorizer.fit + transform so we read
            # the multi-GB phrased parquet from disk exactly once.
            phrased = spark.read.parquet(phrased_path).cache()
            try:
                # min_df/max_df/vocab_size are the standard knobs for
                # auto-pruning the LDA vocabulary: drop terms that are
                # too rare (likely noise) or too common (likely fillers
                # the stopword list missed), and cap total vocab so the
                # feature matrix stays tractable.
                vectorizer = CountVectorizer(
                    inputCol="tokens_phrased",
                    outputCol="features",
                    minDF=float(self.min_df),
                    maxDF=float(self.max_df),
                    vocabSize=int(self.vocab_size),
                )
                vector_model = vectorizer.fit(phrased)
                vector_model.write().overwrite().save(
                    os.path.join(self.output_path, "vector_model")
                )

                # Materialize features once. TrainLDAModel reads this
                # directly so the LDA fit for each K sees a sparse-vector
                # parquet, not a token array.
                (
                    vector_model.transform(phrased)
                    .select("docid", "features")
                    .write.mode("overwrite")
                    .parquet(features_path)
                )
            finally:
                phrased.unpersist()

            # Stamp last so a partial run doesn't look complete.
            _write_stamp(self._config_path(), self._config_dict())
            print("BuildFeatures: vector_model + features written.")

    def output(self):
        return luigi.LocalTarget(self._config_path())


class TrainLDAModel(luigi.Task):
    """Fit one LDA model given precomputed phrases + vector_model.

    Reads the K-independent preprocessing artifacts from
    ``preprocess_path`` (defaults to ``output_path`` for single-K runs)
    and writes ``lda_model`` into ``output_path``. In a K sweep,
    ``preprocess_path`` is a shared root and each K writes its own
    ``lda_model`` to a per-K subdir.

    Completion is gated by a ``_train_config.json`` stamp containing this
    task's own params plus a SHA-256 of the upstream preprocess stamp;
    changing K / seed / maxIter / any preprocessing param invalidates this
    task and (transitively, via their own stamps) every downstream consumer.
    The stamp filename is distinct from ``_config.json`` so the Train and
    Fit stamps don't collide when ``preprocess_path == output_path`` (the
    default in single-K runs).
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _config_path(self):
        return os.path.join(self.output_path, "_train_config.json")

    def _config_dict(self):
        return {
            "task": "TrainLDAModel",
            "num_topics": int(self.num_topics),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        model_path = os.path.join(self.output_path, "lda_model")
        if not os.path.exists(model_path):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def requires(self):
        return BuildFeatures(
            input_path=self.input_path,
            output_path=self._preprocess_path(),
            date=self.date,
            sample_fraction=self.sample_fraction,
            min_df=self.min_df,
            max_df=self.max_df,
            vocab_size=self.vocab_size,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            # Cache features before LDA.fit. Online VB iterates maxIter
            # times over mini-batches of the input; without an external
            # cache, Spark MLlib would re-read features.parquet each pass.
            features = spark.read.parquet(
                os.path.join(self._preprocess_path(), "features.parquet")
            ).cache()
            try:
                lda = _build_lda(
                    k=self.num_topics,
                    max_iter=self.max_iter,
                    seed=self.seed,
                    learning_offset=self.learning_offset,
                    learning_decay=self.learning_decay,
                    subsampling_rate=self.subsampling_rate,
                    optimize_doc_concentration=self.optimize_doc_concentration,
                )
                lda_model = lda.fit(features)
            finally:
                features.unpersist()

            os.makedirs(self.output_path, exist_ok=True)
            # overwrite() so an interrupted prior run doesn't permanently
            # poison the output dir; LocalTarget existence is not enough.
            lda_model.write().overwrite().save(
                os.path.join(self.output_path, "lda_model")
            )
            _write_stamp(self._config_path(), self._config_dict())
            print(f"LDA model saved (k={self.num_topics}).")

    def output(self):
        return luigi.LocalTarget(self._config_path())


class TrainLDASweep(luigi.Task):
    """Fit every K in one Spark session via ``LDA.fitMultiple``.

    Reads + caches ``features.parquet`` exactly once, then fits all
    ``k_values`` against that single cached DataFrame, writing each model
    to ``<output_path>/k{K}/lda_model`` with a per-K ``_train_config.json``
    stamp **byte-identical** to what a per-K ``TrainLDAModel`` would write.
    That identity is load-bearing: every per-K downstream consumer
    (``EvaluateLDAModel`` / ``RunLDAInference``) routes here via
    ``_train_requirement`` and its ``_stamp_matches`` check against
    ``k{K}/_train_config.json`` passes without that per-K Train ever
    running — so the sweep trains once, evaluates per K, and the old
    per-K-JVM + per-K features re-read are gone.

    ``fitMultiple`` is consumed sequentially for predictable driver
    memory. Online LDA fits are independent ``LocalLDAModel``s with no
    shared GraphX state, so a thread pool over the iterator would be
    memory-safe (unlike the deleted EM optimizer) — left as a future
    option, not enabled.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    k_values = luigi.ListParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _k_dir(self, k):
        return os.path.join(self.output_path, f"k{int(k)}")

    def _train_config_path(self, k):
        return os.path.join(self._k_dir(k), "_train_config.json")

    def _train_config_dict(self, k):
        # MUST stay byte-identical to TrainLDAModel._config_dict for this
        # K (output_path is intentionally absent from that dict, so the
        # per-K dir location doesn't matter — only the params + upstream
        # preprocess hash do).
        return {
            "task": "TrainLDAModel",
            "num_topics": int(k),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        for k in self.k_values:
            if not os.path.exists(os.path.join(self._k_dir(k), "lda_model")):
                return False
            if not _stamp_matches(
                self._train_config_path(k), self._train_config_dict(k)
            ):
                return False
        return True

    def requires(self):
        return BuildFeatures(
            input_path=self.input_path,
            output_path=self._preprocess_path(),
            date=self.date,
            sample_fraction=self.sample_fraction,
            min_df=self.min_df,
            max_df=self.max_df,
            vocab_size=self.vocab_size,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            # Read + cache the (multi-GB at full slice) feature matrix
            # ONCE; every K's fit reuses this cache instead of the old
            # per-K parquet re-read + fresh JVM.
            features = spark.read.parquet(
                os.path.join(self._preprocess_path(), "features.parquet")
            ).cache()
            try:
                features.count()
                lda = _build_lda(
                    k=int(self.k_values[0]),
                    max_iter=self.max_iter,
                    seed=self.seed,
                    learning_offset=self.learning_offset,
                    learning_decay=self.learning_decay,
                    subsampling_rate=self.subsampling_rate,
                    optimize_doc_concentration=self.optimize_doc_concentration,
                )
                param_maps = [{lda.k: int(k)} for k in self.k_values]
                for index, model in lda.fitMultiple(features, param_maps):
                    k = int(self.k_values[index])
                    k_dir = self._k_dir(k)
                    os.makedirs(k_dir, exist_ok=True)
                    model.write().overwrite().save(
                        os.path.join(k_dir, "lda_model")
                    )
                    _write_stamp(
                        self._train_config_path(k),
                        self._train_config_dict(k),
                    )
                    print(f"LDA model saved (k={k}).")
            finally:
                features.unpersist()

    def output(self):
        return [
            luigi.LocalTarget(self._train_config_path(k))
            for k in self.k_values
        ]


class RunLDAInference(luigi.Task):
    """Score every doc against the trained LDA, write wide doc-topic parquet.

    Stays in Spark end-to-end: ``vector_to_array`` turns the distribution
    vector into an array column, ``array_position(arr, array_max(arr))``
    gives the dominant topic, and the K probabilities are projected into
    ``topic_0`` ... ``topic_{K-1}`` columns. Previously this task collected
    every doc-topic distribution to the driver via ``toPandas()``, which
    capped scale at ~``spark.driver.maxResultSize`` worth of dense floats.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)
    # Pure requires()-routing knob: when set, this task depends on the
    # shared TrainLDASweep (one fitMultiple over all K) instead of a
    # per-K TrainLDAModel. Deliberately excluded from _config_dict so
    # the same trained model isn't re-stamped per sweep size.
    k_values = luigi.OptionalListParameter(default=None)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _config_path(self):
        return os.path.join(self.output_path, "_inference_config.json")

    def _config_dict(self):
        return {
            "task": "RunLDAInference",
            "num_topics": int(self.num_topics),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        out = os.path.join(self.output_path, "docTopicDistribution_lda.parquet")
        if not os.path.exists(out):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def requires(self):
        return _train_requirement(self)

    def run(self):
        with spark_resource() as spark:
            preprocess = self._preprocess_path()
            lda_model = LocalLDAModel.load(
                os.path.join(self.output_path, "lda_model")
            )
            vector_model = CountVectorizerModel.load(
                os.path.join(preprocess, "vector_model")
            )
            features = spark.read.parquet(
                os.path.join(preprocess, "features.parquet")
            )

            vocab = vector_model.vocabulary
            if not vocab:
                raise ValueError("Empty CountVectorizer vocabulary.")

            # describeTopics is K rows — collect is fine, no scale concern.
            topics = lda_model.describeTopics(maxTermsPerTopic=100)
            topic_words = {
                row.topic: [vocab[i] for i in row.termIndices]
                for row in topics.collect()
            }
            pd.DataFrame(
                list(topic_words.items()), columns=["Topic", "Words"]
            ).to_csv(os.path.join(self.output_path, "topicWords_lda.txt"), index=False)

            # Doc-topic distribution: stay in Spark. array_position is
            # 1-indexed so subtract 1 for a 0-indexed topic id.
            scored = (
                lda_model.transform(features)
                .select("docid", vector_to_array("topicDistribution").alias("td"))
                .withColumn(
                    "highest_topic",
                    (F.array_position(F.col("td"), F.array_max(F.col("td"))) - 1).cast(
                        "int"
                    ),
                )
            )
            projected = scored.select(
                F.col("docid"),
                F.col("highest_topic"),
                *[F.col("td")[i].alias(f"topic_{i}") for i in range(self.num_topics)],
            )
            result_parquet_path = os.path.join(
                self.output_path, "docTopicDistribution_lda.parquet"
            )
            projected.write.mode("overwrite").parquet(result_parquet_path)
            _write_stamp(self._config_path(), self._config_dict())
            print("Inference completed.")

    def output(self):
        return luigi.LocalTarget(self._config_path())


class InferLDASweep(luigi.Task):
    """Inference analogue of ``TrainLDASweep``: score every K's model in
    one Spark session.

    Loads ``vector_model`` and caches ``features.parquet`` once, then for
    each K loads ``k{K}/lda_model``, writes
    ``k{K}/docTopicDistribution_lda.parquet`` + ``topicWords_lda.txt`` and
    a per-K ``_inference_config.json`` **byte-identical** to what a per-K
    ``RunLDAInference`` would write. That identity lets the per-K
    ``RunLDAInference`` (which ``PlotResults`` and the sweep reach via
    ``_infer_requirement``) be satisfied without ever running — one
    inference node, no per-K JVM or features re-read.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    k_values = luigi.ListParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _k_dir(self, k):
        return os.path.join(self.output_path, f"k{int(k)}")

    def _infer_config_path(self, k):
        return os.path.join(self._k_dir(k), "_inference_config.json")

    def _infer_config_dict(self, k):
        # MUST stay byte-identical to RunLDAInference._config_dict for
        # this K (output_path is absent there, so the per-K dir doesn't
        # matter — only params + upstream preprocess hash do).
        return {
            "task": "RunLDAInference",
            "num_topics": int(k),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        for k in self.k_values:
            out = os.path.join(
                self._k_dir(k), "docTopicDistribution_lda.parquet"
            )
            if not os.path.exists(out):
                return False
            if not _stamp_matches(
                self._infer_config_path(k), self._infer_config_dict(k)
            ):
                return False
        return True

    def requires(self):
        return TrainLDASweep(
            input_path=self.input_path,
            output_path=self.output_path,
            k_values=self.k_values,
            preprocess_path=self.preprocess_path,
            date=self.date,
            sample_fraction=self.sample_fraction,
            min_df=self.min_df,
            max_df=self.max_df,
            vocab_size=self.vocab_size,
            seed=self.seed,
            max_iter=self.max_iter,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
            learning_offset=self.learning_offset,
            learning_decay=self.learning_decay,
            subsampling_rate=self.subsampling_rate,
            optimize_doc_concentration=self.optimize_doc_concentration,
        )

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            preprocess = self._preprocess_path()
            vector_model = CountVectorizerModel.load(
                os.path.join(preprocess, "vector_model")
            )
            vocab = vector_model.vocabulary
            if not vocab:
                raise ValueError("Empty CountVectorizer vocabulary.")

            # Cache the feature matrix once; every K's transform reuses
            # it instead of the old per-K parquet re-read + fresh JVM.
            features = spark.read.parquet(
                os.path.join(preprocess, "features.parquet")
            ).cache()
            try:
                features.count()
                for k in self.k_values:
                    k = int(k)
                    k_dir = self._k_dir(k)
                    os.makedirs(k_dir, exist_ok=True)
                    lda_model = LocalLDAModel.load(
                        os.path.join(k_dir, "lda_model")
                    )

                    topics = lda_model.describeTopics(maxTermsPerTopic=100)
                    topic_words = {
                        row.topic: [vocab[i] for i in row.termIndices]
                        for row in topics.collect()
                    }
                    pd.DataFrame(
                        list(topic_words.items()), columns=["Topic", "Words"]
                    ).to_csv(
                        os.path.join(k_dir, "topicWords_lda.txt"), index=False
                    )

                    # Stay in Spark; array_position is 1-indexed.
                    scored = (
                        lda_model.transform(features)
                        .select(
                            "docid",
                            vector_to_array("topicDistribution").alias("td"),
                        )
                        .withColumn(
                            "highest_topic",
                            (
                                F.array_position(
                                    F.col("td"), F.array_max(F.col("td"))
                                )
                                - 1
                            ).cast("int"),
                        )
                    )
                    projected = scored.select(
                        F.col("docid"),
                        F.col("highest_topic"),
                        *[
                            F.col("td")[i].alias(f"topic_{i}")
                            for i in range(k)
                        ],
                    )
                    projected.write.mode("overwrite").parquet(
                        os.path.join(k_dir, "docTopicDistribution_lda.parquet")
                    )
                    _write_stamp(
                        self._infer_config_path(k),
                        self._infer_config_dict(k),
                    )
                    print(f"Inference completed (k={k}).")
            finally:
                features.unpersist()

    def output(self):
        return [
            luigi.LocalTarget(self._infer_config_path(k))
            for k in self.k_values
        ]


class EvaluateLDAModel(luigi.Task):
    """Doc-level NPMI coherence + topic diversity for one trained LDA model.

    Loads the saved LDA + CountVectorizer, reconstructs the training corpus
    (same date filter + seed as TrainLDAModel), and scores the top-N words
    per topic. Output is a single-row parquet so a sweep over K can be
    concatenated downstream.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    top_n = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)
    # See RunLDAInference: routing-only, excluded from _config_dict.
    k_values = luigi.OptionalListParameter(default=None)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _config_path(self):
        return os.path.join(self.output_path, "_eval_config.json")

    def _config_dict(self):
        return {
            "task": "EvaluateLDAModel",
            "num_topics": int(self.num_topics),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "top_n": int(self.top_n),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        if not os.path.exists(self._coherence_path()):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def requires(self):
        return _train_requirement(self)

    def _coherence_path(self):
        return os.path.join(self.output_path, "coherence.parquet")

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            preprocess = self._preprocess_path()
            lda_model = LocalLDAModel.load(
                os.path.join(self.output_path, "lda_model")
            )
            vector_model = CountVectorizerModel.load(
                os.path.join(preprocess, "vector_model")
            )
            # Read the phrased tokens MinePhrases already materialized —
            # same corpus the model fit on, no need to re-run the
            # analyze UDF or apply_phrases per K.
            phrased = spark.read.parquet(
                os.path.join(preprocess, "tokens_phrased.parquet")
            )

            vocab = vector_model.vocabulary
            if not vocab:
                raise ValueError("Empty CountVectorizer vocabulary.")
            topic_rows = sorted(
                lda_model.describeTopics(maxTermsPerTopic=self.top_n).collect(),
                key=lambda r: r.topic,
            )
            topic_words = [
                [vocab[i] for i in row.termIndices] for row in topic_rows
            ]

            result = doc_level_npmi_coherence(
                phrased,
                topic_words=topic_words,
                tokens_col="tokens_phrased",
                top_n=self.top_n,
            )

            row = {
                "k": int(self.num_topics),
                "mean_npmi": float(result["mean_npmi"]),
                "topic_diversity": float(result["topic_diversity"]),
                "coherence_diversity": float(
                    result["mean_npmi"] * result["topic_diversity"]
                ),
                "n_docs": int(result["n_docs"]),
                "per_topic_npmi": [float(x) for x in result["per_topic_npmi"]],
                "top_words": topic_words,
            }
            schema = (
                "k int, mean_npmi double, topic_diversity double, "
                "coherence_diversity double, n_docs long, "
                "per_topic_npmi array<double>, top_words array<array<string>>"
            )
            (
                spark.createDataFrame([row], schema)
                .write.mode("overwrite")
                .parquet(self._coherence_path())
            )
            _write_stamp(self._config_path(), self._config_dict())
            print(
                f"k={self.num_topics}: mean_npmi={row['mean_npmi']:.4f}, "
                f"diversity={row['topic_diversity']:.4f}, "
                f"coh*div={row['coherence_diversity']:.4f}"
            )

    def output(self):
        return luigi.LocalTarget(self._config_path())


class PlotResults(luigi.Task):
    """Project doc-topic distributions to 2D for visual sanity checks.

    Samples down to ``plot_sample_size`` docs before pulling to pandas;
    PCA / random projection on a 50k subsample is indistinguishable from
    the full set visually but is bounded in driver memory.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    plot_sample_size = luigi.IntParameter(default=50_000)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)
    # See RunLDAInference: routing-only, excluded from _config_dict.
    k_values = luigi.OptionalListParameter(default=None)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def _config_path(self):
        return os.path.join(self.output_path, "_plot_config.json")

    def _config_dict(self):
        return {
            "task": "PlotResults",
            "num_topics": int(self.num_topics),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "plot_sample_size": int(self.plot_sample_size),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        pngs = [
            os.path.join(self.output_path, "pca_lda_topics.png"),
            os.path.join(self.output_path, "rp_lda_topics.png"),
        ]
        if not all(os.path.exists(p) for p in pngs):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def requires(self):
        return _infer_requirement(self)

    def run(self):
        with spark_resource() as spark:
            doc_topic_spark_df = spark.read.parquet(
                os.path.join(self.output_path, "docTopicDistribution_lda.parquet")
            ).cache()
            try:
                total = doc_topic_spark_df.count()
                frac = min(1.0, float(self.plot_sample_size) / max(total, 1))
                sample = doc_topic_spark_df.sample(frac, seed=42).toPandas()
            finally:
                doc_topic_spark_df.unpersist()
            doc_topic_array = sample.iloc[:, 2:].values
            dominant_topics = sample["highest_topic"].values

            pca = PCA(n_components=2)
            doc_topic_2d_pca = pca.fit_transform(doc_topic_array)
            plt.figure(figsize=(10, 6))
            plt.scatter(
                doc_topic_2d_pca[:, 0],
                doc_topic_2d_pca[:, 1],
                c=dominant_topics,
                cmap="tab20",
                alpha=0.6,
            )
            plt.colorbar(label="Topic")
            plt.title("LDA PCA Topic Clusters")
            plt.savefig(os.path.join(self.output_path, "pca_lda_topics.png"))

            rp = GaussianRandomProjection(n_components=2, random_state=42)
            doc_topic_2d_rp = rp.fit_transform(doc_topic_array)
            plt.figure(figsize=(10, 6))
            plt.scatter(
                doc_topic_2d_rp[:, 0],
                doc_topic_2d_rp[:, 1],
                c=dominant_topics,
                cmap="tab20",
                alpha=0.6,
            )
            plt.colorbar(label="Topic")
            plt.title("LDA RP Topic Clusters")
            plt.savefig(os.path.join(self.output_path, "rp_lda_topics.png"))

            _write_stamp(self._config_path(), self._config_dict())

    def output(self):
        return luigi.LocalTarget(self._config_path())


class Workflow(luigi.WrapperTask):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    plot_sample_size = luigi.IntParameter(default=50_000)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)

    def requires(self):
        shared = dict(
            input_path=self.input_path,
            output_path=self.output_path,
            num_topics=self.num_topics,
            date=self.date,
            sample_fraction=self.sample_fraction,
            min_df=self.min_df,
            max_df=self.max_df,
            vocab_size=self.vocab_size,
            seed=self.seed,
            max_iter=self.max_iter,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
            learning_offset=self.learning_offset,
            learning_decay=self.learning_decay,
            subsampling_rate=self.subsampling_rate,
            optimize_doc_concentration=self.optimize_doc_concentration,
        )
        return [
            PlotResults(plot_sample_size=self.plot_sample_size, **shared),
            EvaluateLDAModel(**shared),
        ]


class AggregateLDASweep(luigi.Task):
    """The sweep root: train all K once, evaluate per K, union coherence.

    Preprocessing is shared at ``<output_path>/preprocess/`` (runs once).
    Each K's model + coherence live under ``<output_path>/k<K>/``. All
    per-K ``EvaluateLDAModel``s carry ``k_values`` so they route through
    one shared ``TrainLDASweep`` (``LDA.fitMultiple`` over a single cached
    feature matrix) — the old per-K JVM + per-K parquet re-read are gone.
    Replaces the former ``SweepLDAK`` wrapper; emits a single
    ``sweep_coherence.parquet`` so K-selection needs no manual globbing.

    Tasks still run with the default Luigi ``workers=1`` (one Spark
    session per task; concurrent sessions would contend for driver
    memory). The preprocess analyze is now a native executor-JVM UDF, so
    it no longer boots an embedded Lucene JVM per Python worker — see
    ``_register_fr_analyzer``.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    k_values = luigi.ListParameter()
    date = luigi.Parameter(default="2023-02")
    sample_fraction = luigi.FloatParameter(default=0.3)
    min_df = luigi.FloatParameter(default=50.0)
    max_df = luigi.FloatParameter(default=0.2)
    vocab_size = luigi.IntParameter(default=20_000)
    seed = luigi.IntParameter(default=42)
    max_iter = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)
    learning_offset = luigi.FloatParameter(default=1024.0)
    learning_decay = luigi.FloatParameter(default=0.51)
    subsampling_rate = luigi.FloatParameter(default=0.05)
    optimize_doc_concentration = luigi.BoolParameter(default=True)

    def _preprocess_path(self):
        return os.path.join(self.output_path, "preprocess")

    def _config_path(self):
        return os.path.join(self.output_path, "_aggregate_config.json")

    def _summary_path(self):
        return os.path.join(self.output_path, "sweep_coherence.parquet")

    def _config_dict(self):
        return {
            "task": "AggregateLDASweep",
            "k_values": sorted(int(k) for k in self.k_values),
            "seed": int(self.seed),
            "max_iter": int(self.max_iter),
            "learning_offset": float(self.learning_offset),
            "learning_decay": float(self.learning_decay),
            "subsampling_rate": float(self.subsampling_rate),
            "optimize_doc_concentration": bool(self.optimize_doc_concentration),
            "preprocess_hash": _preprocess_hash(self),
        }

    def complete(self):
        if not os.path.exists(self._summary_path()):
            return False
        return _stamp_matches(self._config_path(), self._config_dict())

    def requires(self):
        preprocess = self._preprocess_path()
        evals = [
            EvaluateLDAModel(
                input_path=self.input_path,
                output_path=os.path.join(self.output_path, f"k{int(k)}"),
                num_topics=int(k),
                preprocess_path=preprocess,
                k_values=self.k_values,
                date=self.date,
                sample_fraction=self.sample_fraction,
                min_df=self.min_df,
                max_df=self.max_df,
                vocab_size=self.vocab_size,
                seed=self.seed,
                max_iter=self.max_iter,
                phrases_min_count=self.phrases_min_count,
                phrases_threshold=self.phrases_threshold,
                learning_offset=self.learning_offset,
                learning_decay=self.learning_decay,
                subsampling_rate=self.subsampling_rate,
                optimize_doc_concentration=self.optimize_doc_concentration,
            )
            for k in self.k_values
        ]
        # Also materialize per-K doc–topic distributions for the
        # forthcoming retrieval-augmentation work. One InferLDASweep node
        # (deduped with the TrainLDASweep the evals already route to).
        infer = InferLDASweep(
            input_path=self.input_path,
            output_path=self.output_path,
            k_values=self.k_values,
            preprocess_path=preprocess,
            date=self.date,
            sample_fraction=self.sample_fraction,
            min_df=self.min_df,
            max_df=self.max_df,
            vocab_size=self.vocab_size,
            seed=self.seed,
            max_iter=self.max_iter,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
            learning_offset=self.learning_offset,
            learning_decay=self.learning_decay,
            subsampling_rate=self.subsampling_rate,
            optimize_doc_concentration=self.optimize_doc_concentration,
        )
        return evals + [infer]

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            cols = [
                "k",
                "mean_npmi",
                "topic_diversity",
                "coherence_diversity",
                "n_docs",
            ]
            frames = [
                spark.read.parquet(
                    os.path.join(self.output_path, f"k{int(k)}", "coherence.parquet")
                ).select(*cols)
                for k in self.k_values
            ]
            summary = functools.reduce(
                lambda a, b: a.unionByName(b), frames
            ).orderBy("k")
            summary.write.mode("overwrite").parquet(self._summary_path())
            _write_stamp(self._config_path(), self._config_dict())
            print(f"Sweep coherence written for k={sorted(int(k) for k in self.k_values)}.")

    def output(self):
        return luigi.LocalTarget(self._config_path())


def main(
        num_topics: Annotated[
            int, typer.Argument(help="Number of topics")
        ] = 20,
        input_path: Annotated[
            str, typer.Argument(help="Input root directory")
        ] = "/mnt/data/longeval",
        output_path: Annotated[
            str, typer.Argument(help="Output root directory")
        ] = "/mnt/data/longeval",
        date: Annotated[
            str, typer.Option(help="Snapshot date, e.g. 2022-06 or 2023-02")
        ] = "2023-02",
        sample_fraction: Annotated[
            float, typer.Option(help="Document sampling fraction")
        ] = 0.3,
        min_df: Annotated[
            float, typer.Option(help="CountVectorizer minDF (absolute count or fraction)")
        ] = 50.0,
        max_df: Annotated[
            float, typer.Option(help="CountVectorizer maxDF (absolute count or fraction)")
        ] = 0.2,
        vocab_size: Annotated[
            int, typer.Option(help="CountVectorizer vocabSize cap")
        ] = 20_000,
        seed: Annotated[int, typer.Option(help="LDA optimizer seed")] = 42,
        max_iter: Annotated[
            int, typer.Option(help="LDA maxIter (online variational passes)")
        ] = 10,
        phrases_min_count: Annotated[
            int, typer.Option(help="Min unigram+bigram count for NPMI phrases")
        ] = 20,
        phrases_threshold: Annotated[
            float, typer.Option(help="Min NPMI for a bigram to survive")
        ] = 0.5,
        plot_sample_size: Annotated[
            int, typer.Option(help="Driver-side doc cap for PCA/RP plots")
        ] = 50_000,
        learning_offset: Annotated[
            float, typer.Option(help="Online LDA learningOffset (down-weights early iters)")
        ] = 1024.0,
        learning_decay: Annotated[
            float, typer.Option(help="Online LDA learningDecay in (0.5, 1.0]")
        ] = 0.51,
        subsampling_rate: Annotated[
            float, typer.Option(help="Online LDA mini-batch fraction per iteration")
        ] = 0.05,
        optimize_doc_concentration: Annotated[
            bool, typer.Option(help="Online LDA: also estimate docConcentration")
        ] = True,
        scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    """Train + evaluate one LDA model for the given K."""

    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [Workflow(
            num_topics=num_topics,
            input_path=input_path,
            output_path=output_path,
            date=date,
            sample_fraction=sample_fraction,
            min_df=min_df,
            max_df=max_df,
            vocab_size=vocab_size,
            seed=seed,
            max_iter=max_iter,
            phrases_min_count=phrases_min_count,
            phrases_threshold=phrases_threshold,
            plot_sample_size=plot_sample_size,
            learning_offset=learning_offset,
            learning_decay=learning_decay,
            subsampling_rate=subsampling_rate,
            optimize_doc_concentration=optimize_doc_concentration,
        )],
        **kwargs,
    )


def sweep_main(
        k_values: Annotated[
            str,
            typer.Argument(
                help="Comma-separated K values to sweep, e.g. '2,3,5,10,20,30,40,80,160'"
            ),
        ] = "2,3,5,10,20,30,40,80,160",
        input_path: Annotated[
            str, typer.Argument(help="Input root directory")
        ] = "/mnt/data/longeval",
        output_path: Annotated[
            str, typer.Argument(help="Sweep output root; per-K subdirs created under it")
        ] = "/mnt/data/longeval/lda-sweep",
        date: Annotated[
            str, typer.Option(help="Snapshot date, e.g. 2022-06 or 2023-02")
        ] = "2023-02",
        sample_fraction: Annotated[
            float, typer.Option(help="Document sampling fraction")
        ] = 0.3,
        min_df: Annotated[
            float, typer.Option(help="CountVectorizer minDF (absolute count or fraction)")
        ] = 50.0,
        max_df: Annotated[
            float, typer.Option(help="CountVectorizer maxDF (absolute count or fraction)")
        ] = 0.2,
        vocab_size: Annotated[
            int, typer.Option(help="CountVectorizer vocabSize cap")
        ] = 20_000,
        seed: Annotated[int, typer.Option(help="LDA optimizer seed")] = 42,
        max_iter: Annotated[
            int, typer.Option(help="LDA maxIter (online variational passes)")
        ] = 10,
        phrases_min_count: Annotated[
            int, typer.Option(help="Min unigram+bigram count for NPMI phrases")
        ] = 20,
        phrases_threshold: Annotated[
            float, typer.Option(help="Min NPMI for a bigram to survive")
        ] = 0.5,
        learning_offset: Annotated[
            float, typer.Option(help="Online LDA learningOffset (down-weights early iters)")
        ] = 1024.0,
        learning_decay: Annotated[
            float, typer.Option(help="Online LDA learningDecay in (0.5, 1.0]")
        ] = 0.51,
        subsampling_rate: Annotated[
            float, typer.Option(help="Online LDA mini-batch fraction per iteration")
        ] = 0.05,
        optimize_doc_concentration: Annotated[
            bool, typer.Option(help="Online LDA: also estimate docConcentration")
        ] = True,
        scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    """Train + evaluate an LDA model for each K, writing per-K subdirs."""
    ks = [int(x) for x in k_values.split(",") if x.strip()]
    if not ks:
        raise typer.BadParameter("k_values must contain at least one integer")

    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [AggregateLDASweep(
            input_path=input_path,
            output_path=output_path,
            k_values=ks,
            date=date,
            sample_fraction=sample_fraction,
            min_df=min_df,
            max_df=max_df,
            vocab_size=vocab_size,
            seed=seed,
            max_iter=max_iter,
            phrases_min_count=phrases_min_count,
            phrases_threshold=phrases_threshold,
            learning_offset=learning_offset,
            learning_decay=learning_decay,
            subsampling_rate=subsampling_rate,
            optimize_doc_concentration=optimize_doc_concentration,
        )],
        **kwargs,
    )