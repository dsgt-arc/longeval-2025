import luigi
from longeval.collection import RawCollection
from longeval.spark import spark_resource
from pathlib import Path
import typer
from typing_extensions import Annotated

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, collect_list, expr
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.ml.feature import CountVectorizer, StopWordsRemover
from pyspark.ml.clustering import LDA
import numpy as np
import pandas as pd
import os
from pyspark.sql.functions import col, array_max, array_position
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from longeval.collection import ParquetCollection
from pyspark.ml.clustering import DistributedLDAModel
from pyspark.ml.feature import CountVectorizerModel
from sklearn.manifold import MDS
from sklearn.random_projection import GaussianRandomProjection
from pyspark.sql import functions as F

from longeval.etl.lda.coherence import doc_level_npmi_coherence
from longeval.etl.lda.phrases import apply_phrases, fit_phrases


def _build_lucene_fr_analyzer():
    """Return a fresh Lucene FR analyzer (lowercase + Snowball + FR stops)."""
    from pyserini.analysis import Analyzer, get_lucene_analyzer

    return Analyzer(get_lucene_analyzer(language="fr"))


# Per-worker Lucene analyzer singleton; the JVM via pyjnius boots once per
# worker process and is reused across all rows on that worker.
_ANALYZER = None


def _analyze(text):
    """Lucene-French analyze: lowercase + Snowball stem + French stopwords."""
    global _ANALYZER
    if _ANALYZER is None:
        _ANALYZER = _build_lucene_fr_analyzer()
    return _ANALYZER.analyze(text or "")


_analyze_udf = udf(_analyze, ArrayType(StringType()))


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


class FitLDAPreprocessing(luigi.Task):
    """K-independent preprocessing: materialize the LDA input pipeline.

    Runs the expensive corpus transformations exactly once per (date,
    seed, threshold) config and writes their outputs to disk so every
    downstream K-specific task can ``spark.read.parquet`` them:

    - ``phrases.parquet``        — NPMI bigram set (driver-loadable).
    - ``tokens_phrased.parquet`` — analyzed + phrase-augmented tokens
                                   per ``docid``. Lets coherence and any
                                   future text-level eval reuse the same
                                   tokenization the model trained on.
    - ``vector_model``           — CountVectorizerModel (vocabulary).
    - ``features.parquet``       — sparse feature vectors per ``docid``,
                                   ready to feed ``LDA.fit``. K-independent.

    Without this, a 6-K sweep re-runs ``_analyze_udf`` + ``apply_phrases``
    13× (once per Train and Evaluate task per K). Materialization cuts
    that to one analyze+phrase pass total.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            collection = ParquetCollection(spark, self.input_path)
            docs = (
                collection.documents
                .filter(F.col("date") == "2023-02")
                .sample(fraction=0.3, seed=42)
            )
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
                        _analyze_udf(F.col("contents")).alias("tokens_raw"),
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
            features_path = os.path.join(self.output_path, "features.parquet")
            try:
                fit_phrases(
                    analyzed,
                    output_path=phrases_path,
                    tokens_col="tokens",
                    min_count=self.phrases_min_count,
                    threshold=self.phrases_threshold,
                )
                phrases_df = spark.read.parquet(phrases_path)

                # Materialize phrased tokens once. EvaluateLDAModel will
                # read this directly instead of re-running apply_phrases.
                (
                    apply_phrases(analyzed, phrases_df)
                    .select("docid", "tokens_phrased")
                    .write.mode("overwrite")
                    .parquet(phrased_path)
                )
            finally:
                analyzed.unpersist()

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
                    minDF=50.0,
                    maxDF=0.2,
                    vocabSize=20_000,
                )
                vector_model = vectorizer.fit(phrased)
                vector_model.save(os.path.join(self.output_path, "vector_model"))

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

            print(
                "Preprocessing artifacts saved: phrases, tokens_phrased, "
                "vector_model, features."
            )

    def output(self):
        # features.parquet is the last artifact written, so its presence
        # signals all upstream artifacts also succeeded.
        return luigi.LocalTarget(os.path.join(self.output_path, "features.parquet"))


class TrainLDAModel(luigi.Task):
    """Fit one LDA model given precomputed phrases + vector_model.

    Reads the K-independent preprocessing artifacts from
    ``preprocess_path`` (defaults to ``output_path`` for single-K runs)
    and writes ``lda_model`` into ``output_path``. In a K sweep,
    ``preprocess_path`` is a shared root and each K writes its own
    ``lda_model`` to a per-K subdir.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    seed = luigi.IntParameter(default=42)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def requires(self):
        return FitLDAPreprocessing(
            input_path=self.input_path,
            output_path=self._preprocess_path(),
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            # Cache features before LDA.fit. EM-LDA iterates maxIter
            # times over the input; without an external cache, Spark MLlib
            # would re-read features.parquet on each pass.
            features = spark.read.parquet(
                os.path.join(self._preprocess_path(), "features.parquet")
            ).cache()
            try:
                lda = LDA(
                    k=self.num_topics,
                    maxIter=10,
                    featuresCol="features",
                    optimizer="em",
                    seed=self.seed,
                )
                lda_model = lda.fit(features)
            finally:
                features.unpersist()

            os.makedirs(self.output_path, exist_ok=True)
            lda_model.save(os.path.join(self.output_path, "lda_model"))
            print(f"LDA model saved (k={self.num_topics}).")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "lda_model"))


class RunLDAInference(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    preprocess_path = luigi.OptionalParameter(default=None)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def requires(self):
        return TrainLDAModel(
            input_path=self.input_path,
            output_path=self.output_path,
            num_topics=self.num_topics,
            preprocess_path=self.preprocess_path,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def run(self):
        with spark_resource() as spark:
            preprocess = self._preprocess_path()
            lda_model = DistributedLDAModel.load(
                os.path.join(self.output_path, "lda_model")
            )
            vector_model = CountVectorizerModel.load(
                os.path.join(preprocess, "vector_model")
            )
            # Reuse the materialized features from FitLDAPreprocessing.
            # Previously this task re-sampled with seed=94, then re-ran
            # _analyze_udf + apply_phrases + vector_model.transform — work
            # the preprocessor has already done and written to disk. The
            # seed=94 sample wasn't truly held-out (same month, 30% draw
            # from the same population) so dropping it costs nothing
            # interpretively while saving a full analyze pass.
            features = spark.read.parquet(
                os.path.join(preprocess, "features.parquet")
            )

            vocab = vector_model.vocabulary
            if not vocab:
                raise ValueError("Empty CountVectorizer vocabulary.")
            topics = lda_model.describeTopics(maxTermsPerTopic=100)
            topic_words = {
                row.topic: [vocab[i] for i in row.termIndices]
                for row in topics.collect()
            }
            topic_words_file_path = os.path.join(
                self.output_path, "topicWords_lda.txt"
            )
            topic_words_df = pd.DataFrame(
                list(topic_words.items()), columns=["Topic", "Words"]
            )
            topic_words_df.to_csv(topic_words_file_path, index=False)

            topic_distributions = lda_model.transform(features)
            doc_topic_df = topic_distributions.select(
                "docid", "topicDistribution"
            ).toPandas()
            doc_topic_array = np.vstack(
                doc_topic_df["topicDistribution"].apply(lambda v: v.toArray())
            )
            doc_topic_df["highest_topic"] = np.argmax(doc_topic_array, axis=1)

            topic_probabilities_df = pd.DataFrame(
                doc_topic_array,
                columns=[f"topic_{i}" for i in range(self.num_topics)],
            )
            result_df = pd.concat(
                [
                    doc_topic_df[["docid"]],
                    doc_topic_df[["highest_topic"]],
                    topic_probabilities_df,
                ],
                axis=1,
            )
            result_parquet_path = os.path.join(
                self.output_path, "docTopicDistribution_lda.parquet"
            )
            (
                spark.createDataFrame(result_df)
                .write.mode("overwrite")
                .parquet(result_parquet_path)
            )
            print("Inference completed.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "docTopicDistribution_lda.parquet"))


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
    top_n = luigi.IntParameter(default=10)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def _preprocess_path(self):
        return self.preprocess_path or self.output_path

    def requires(self):
        return TrainLDAModel(
            input_path=self.input_path,
            output_path=self.output_path,
            num_topics=self.num_topics,
            preprocess_path=self.preprocess_path,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def _coherence_path(self):
        return os.path.join(self.output_path, "coherence.parquet")

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            preprocess = self._preprocess_path()
            lda_model = DistributedLDAModel.load(
                os.path.join(self.output_path, "lda_model")
            )
            vector_model = CountVectorizerModel.load(
                os.path.join(preprocess, "vector_model")
            )
            # Read the phrased tokens FitLDAPreprocessing already
            # materialized — same corpus the model fit on, no need to
            # re-run _analyze_udf or apply_phrases per K.
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
            print(
                f"k={self.num_topics}: mean_npmi={row['mean_npmi']:.4f}, "
                f"diversity={row['topic_diversity']:.4f}, "
                f"coh*div={row['coherence_diversity']:.4f}"
            )

    def output(self):
        return luigi.LocalTarget(self._coherence_path())


class PlotResults(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()

    def requires(self):
        return RunLDAInference(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics)

    def run(self):
        with spark_resource() as spark:
            doc_topic_spark_df = spark.read.parquet(os.path.join(self.output_path, "docTopicDistribution_lda.parquet"))
            doc_topic_df = doc_topic_spark_df.toPandas()
            doc_topic_array = doc_topic_df.iloc[:, 2:].values
            dominant_topics = doc_topic_df['highest_topic'].values

            # PCA Plot
            pca = PCA(n_components=2)
            doc_topic_2d_pca = pca.fit_transform(doc_topic_array)
            plt.figure(figsize=(10, 6))
            plt.scatter(doc_topic_2d_pca[:, 0], doc_topic_2d_pca[:, 1], c=dominant_topics, cmap='tab20', alpha=0.6)
            plt.colorbar(label='Topic')
            plt.title('LDA PCA Topic Clusters')
            plt.savefig(os.path.join(self.output_path, 'pca_lda_topics.png'))

            # RP plot
            rp = GaussianRandomProjection(n_components=2, random_state=42)
            doc_topic_2d_rp = rp.fit_transform(doc_topic_array)
            plt.figure(figsize=(10, 6))
            plt.scatter(doc_topic_2d_rp[:, 0], doc_topic_2d_rp[:, 1], c=dominant_topics, cmap='tab20', alpha=0.6)
            plt.colorbar(label='Topic')
            plt.title('LDA RP Topic Clusters')
            plt.savefig(os.path.join(self.output_path, 'rp_lda_topics.png'))

    def output(self):
        return [
            luigi.LocalTarget(os.path.join(self.output_path, 'pca_lda_topics.png')),
            luigi.LocalTarget(os.path.join(self.output_path, 'rp_lda_topics.png'))
        ]


class Workflow(luigi.WrapperTask):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()

    def requires(self):
        return [
            PlotResults(
                input_path=self.input_path,
                output_path=self.output_path,
                num_topics=self.num_topics,
            ),
            EvaluateLDAModel(
                input_path=self.input_path,
                output_path=self.output_path,
                num_topics=self.num_topics,
            ),
        ]


class SweepLDAK(luigi.WrapperTask):
    """Run TrainLDAModel + EvaluateLDAModel across a list of K values.

    Preprocessing (phrases + CountVectorizer) is shared at
    ``<output_path>/preprocess/`` so it runs once total, not once per K.
    Each K's LDA model and coherence parquet live under
    ``<output_path>/k<K>/`` to avoid collision.
    """

    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    k_values = luigi.ListParameter()

    def _preprocess_path(self):
        return os.path.join(self.output_path, "preprocess")

    def requires(self):
        preprocess = self._preprocess_path()
        return [
            EvaluateLDAModel(
                input_path=self.input_path,
                output_path=os.path.join(self.output_path, f"k{int(k)}"),
                num_topics=int(k),
                preprocess_path=preprocess,
            )
            for k in self.k_values
        ]


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
        scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    """Convert raw data to parquet"""

    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [Workflow(
            num_topics=num_topics,
            input_path=input_path,
            output_path=output_path
        )],
        **kwargs,
    )


def sweep_main(
        k_values: Annotated[
            str,
            typer.Argument(
                help="Comma-separated K values to sweep, e.g. '5,10,20,40,80,160'"
            ),
        ] = "5,10,20,40,80,160",
        input_path: Annotated[
            str, typer.Argument(help="Input root directory")
        ] = "/mnt/data/longeval",
        output_path: Annotated[
            str, typer.Argument(help="Sweep output root; per-K subdirs created under it")
        ] = "/mnt/data/longeval/lda-sweep",
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
        [SweepLDAK(
            input_path=input_path,
            output_path=output_path,
            k_values=ks,
        )],
        **kwargs,
    )