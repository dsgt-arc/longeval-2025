import luigi
from longeval.collection import RawCollection
from longeval.spark import spark_resource
from pathlib import Path
import pickle
import typer
from typing_extensions import Annotated

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, collect_list, expr
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.ml.feature import CountVectorizer
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


# --- worker-side singletons -------------------------------------------------
# Each Spark Python worker initializes the Lucene analyzer and (lazily) a
# loaded FrozenPhrases on first call, then reuses them across rows. The JVM
# inside pyjnius boots once per worker process and is shared by all rows on
# that worker.

_ANALYZER = None
_PHRASER = None


def _analyze(text):
    """Lucene-French analyze: lowercase + Snowball stem + French stopwords."""
    global _ANALYZER
    if _ANALYZER is None:
        from pyserini.analysis import Analyzer, get_lucene_analyzer

        _ANALYZER = Analyzer(get_lucene_analyzer(language="fr"))
    return _ANALYZER.analyze(text or "")


def _make_apply_phraser(phraser_path: str):
    """Build a UDF that joins strong bigrams using a pickled FrozenPhrases."""

    def _apply(tokens):
        global _PHRASER
        if _PHRASER is None:
            with open(phraser_path, "rb") as f:
                _PHRASER = pickle.load(f)
        return list(_PHRASER[tokens or []])

    return udf(_apply, ArrayType(StringType()))


_analyze_udf = udf(_analyze, ArrayType(StringType()))


class TrainLDAModel(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    phrases_sample_docs = luigi.IntParameter(default=1_000_000)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def run(self):
        from gensim.models.phrases import Phrases

        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")

            collection = ParquetCollection(spark, self.input_path)
            docs = (
                collection.documents
                .filter(F.col("date") == "2023-02")
                .sample(fraction=0.3, seed=42)
                .cache()
            )

            # Step A: Lucene French analyzer (stem + stopwords + lowercase)
            analyzed = docs.withColumn("tokens", _analyze_udf(F.col("contents"))).cache()

            # Step B: fit gensim Phrases on a driver-side sample
            sample = (
                analyzed.select("tokens")
                .orderBy(F.rand(seed=42))
                .limit(self.phrases_sample_docs)
            )
            sentences = [row.tokens for row in sample.collect() if row.tokens]
            phrases = Phrases(
                sentences,
                min_count=self.phrases_min_count,
                threshold=self.phrases_threshold,
                scoring="npmi",
            )
            phraser = phrases.freeze()
            phraser_path = os.path.join(self.output_path, "phraser.pkl")
            os.makedirs(self.output_path, exist_ok=True)
            with open(phraser_path, "wb") as f:
                pickle.dump(phraser, f)

            # Step C: apply phraser to every doc
            apply_phraser_udf = _make_apply_phraser(phraser_path)
            phrased = analyzed.withColumn(
                "tokens_phrased", apply_phraser_udf(F.col("tokens"))
            )

            # Step D: existing CountVectorizer + LDA on the phrased tokens
            vectorizer = CountVectorizer(
                inputCol="tokens_phrased", outputCol="features"
            )
            vector_model = vectorizer.fit(phrased)
            vectorized_docs = vector_model.transform(phrased)

            lda = LDA(
                k=self.num_topics, maxIter=10, featuresCol="features", optimizer="em"
            )
            lda_model = lda.fit(vectorized_docs)

            lda_model.save(os.path.join(self.output_path, "lda_model"))
            vector_model.save(os.path.join(self.output_path, "vector_model"))

            print("LDA model, vectorizer, and phraser saved successfully.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "lda_model"))


class RunLDAInference(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    phrases_sample_docs = luigi.IntParameter(default=1_000_000)
    phrases_min_count = luigi.IntParameter(default=20)
    phrases_threshold = luigi.FloatParameter(default=0.5)

    def requires(self):
        return TrainLDAModel(
            input_path=self.input_path,
            output_path=self.output_path,
            num_topics=self.num_topics,
            phrases_sample_docs=self.phrases_sample_docs,
            phrases_min_count=self.phrases_min_count,
            phrases_threshold=self.phrases_threshold,
        )

    def run(self):
        with spark_resource() as spark:
            lda_model = DistributedLDAModel.load(os.path.join(self.output_path, "lda_model"))
            vector_model = CountVectorizerModel.load(os.path.join(self.output_path, "vector_model"))
            phraser_path = os.path.join(self.output_path, "phraser.pkl")

            collection = ParquetCollection(spark, self.input_path)
            docs = (
                collection.documents
                .filter(F.col("date") == "2023-02")
                .sample(fraction=0.3, seed=94)
                .cache()
            )

            analyzed = docs.withColumn("tokens", _analyze_udf(F.col("contents")))
            apply_phraser_udf = _make_apply_phraser(phraser_path)
            phrased = analyzed.withColumn(
                "tokens_phrased", apply_phraser_udf(F.col("tokens"))
            )
            vectorized_docs = vector_model.transform(phrased)

            topics = lda_model.describeTopics(maxTermsPerTopic=100)
            vocab = vector_model.vocabulary
            if not vocab:
                raise ValueError("Vocabulary is empty. Please check the CountVectorizer step.")
            topic_words = {}
            for row in topics.collect():
                topic_index = row.topic
                term_indices = row.termIndices
                terms = [vocab[index] for index in term_indices]
                topic_words[topic_index] = terms
            topic_words_file_path = os.path.join(self.output_path, "topicWords_lda.txt")
            topic_words_df = pd.DataFrame(list(topic_words.items()), columns=["Topic", "Words"])
            topic_words_df.to_csv(topic_words_file_path, index=False)

            topic_distributions = lda_model.transform(vectorized_docs)
            doc_topic_df = topic_distributions.select("docid", "topicDistribution").toPandas()
            doc_topic_array = np.vstack(doc_topic_df["topicDistribution"].apply(lambda v: v.toArray()))
            doc_topic_df['highest_topic'] = np.argmax(doc_topic_array, axis=1)

            topic_probabilities_df = pd.DataFrame(doc_topic_array,
                                                  columns=[f'topic_{i}' for i in range(self.num_topics)])
            result_df = pd.concat([doc_topic_df[['docid']], doc_topic_df[['highest_topic']], topic_probabilities_df],
                                  axis=1)
            result_parquet_path = os.path.join(self.output_path, "docTopicDistribution_lda.parquet")
            result_df_spark = spark.createDataFrame(result_df)
            result_df_spark.write.parquet(result_parquet_path)
            print("Inference completed.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "docTopicDistribution_lda.parquet"))


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
        return PlotResults(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics)


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