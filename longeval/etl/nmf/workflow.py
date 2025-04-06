import luigi
from longeval.collection import ParquetCollection
from longeval.spark import spark_resource
from pathlib import Path
import typer
from typing_extensions import Annotated

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
import pandas as pd
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import MDS

class TrainNMFModel(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    language = luigi.Parameter()

    def run(self):
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            collection = ParquetCollection(spark, self.input_path)
            docs = collection.documents.select("contents").toPandas()["contents"].tolist()

            nmfLanguage = "english"
            if self.language == "french":
                nmfLanguage = "french"
            tfidf_vectorizer = TfidfVectorizer(stop_words=nmfLanguage)
            tfidf = tfidf_vectorizer.fit_transform(docs)

            nmf_model = NMF(n_components=self.num_topics, random_state=42)
            W = nmf_model.fit_transform(tfidf)
            H = nmf_model.components_

            topic_words = {}
            vocab = tfidf_vectorizer.get_feature_names_out()
            for topic_idx, topic in enumerate(H):
                top_words = [vocab[i] for i in topic.argsort()[:-100:-1]]
                topic_words[topic_idx] = top_words

            pd.DataFrame(topic_words.items(), columns=["Topic", "Words"]).to_csv(os.path.join(self.output_path, "topicWords_nmf.txt"), index=False)
            np.save(os.path.join(self.output_path, "nmf_W.npy"), W)

            print("NMF model trained and saved.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "nmf_W.npy"))

class RunNMFInference(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    language = luigi.Parameter()

    def requires(self):
        return TrainNMFModel(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics, language=self.language)

    def run(self):
        with spark_resource() as spark:
            collection = ParquetCollection(spark, self.input_path)
            docs = collection.documents.select("contents").toPandas()["contents"].tolist()

            tfidf_vectorizer = TfidfVectorizer(stop_words='english')
            tfidf = tfidf_vectorizer.fit_transform(docs)

            nmf_model = NMF(n_components=self.num_topics, random_state=42)
            W = nmf_model.fit_transform(tfidf)

            doc_topic_df = pd.DataFrame(W, columns=[f'topic_{i}' for i in range(self.num_topics)])
            doc_topic_df["highest_topic"] = np.argmax(W, axis=1)
            doc_topic_df.to_parquet(os.path.join(self.output_path, "docTopicDistribution_nmf.parquet"))

            print("NMF inference completed.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "docTopicDistribution_nmf.parquet"))

class PlotResults(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    language = luigi.Parameter()

    def requires(self):
        return RunNMFInference(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics, language=self.language)

    def run(self):
        doc_topic_df = pd.read_parquet(os.path.join(self.output_path, "docTopicDistribution_nmf.parquet"))
        doc_topic_array = doc_topic_df.iloc[:, :-1].values
        dominant_topics = doc_topic_df["highest_topic"].values

        pca = PCA(n_components=2)
        doc_topic_2d_pca = pca.fit_transform(doc_topic_array)
        plt.figure(figsize=(10, 6))
        plt.scatter(doc_topic_2d_pca[:, 0], doc_topic_2d_pca[:, 1], c=dominant_topics, cmap='tab20', alpha=0.6)
        plt.colorbar(label='Topic')
        plt.title('NMF PCA Topic Clusters')
        plt.savefig(os.path.join(self.output_path, 'pca_nmf_topics.png'))

        mds = MDS(n_components=2, random_state=42)
        doc_topic_2d_mds = mds.fit_transform(doc_topic_array)
        plt.figure(figsize=(10, 6))
        plt.scatter(doc_topic_2d_mds[:, 0], doc_topic_2d_mds[:, 1], c=dominant_topics, cmap='tab20', alpha=0.6)
        plt.colorbar(label='Topic')
        plt.title('NMF MDS Topic Clusters')
        plt.savefig(os.path.join(self.output_path, 'mds_nmf_topics.png'))

    def output(self):
        return [
            luigi.LocalTarget(os.path.join(self.output_path, 'pca_nmf_topics.png')),
            luigi.LocalTarget(os.path.join(self.output_path, 'mds_nmf_topics.png'))
        ]

class Workflow(luigi.WrapperTask):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    num_topics = luigi.IntParameter()
    language = luigi.Parameter()

    def requires(self):
        return PlotResults(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics, language=self.language)

def main(
    num_topics: Annotated[int, typer.Argument(help="Number of topics")] = 20,
    language: Annotated[str, typer.Argument(help="Number of topics")] = "english",
    input_path: Annotated[str, typer.Argument(help="Input root directory")] = "/mnt/data/longeval",
    output_path: Annotated[str, typer.Argument(help="Output root directory")] = "/mnt/data/longeval",
    scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build([Workflow(num_topics=num_topics, language=language, input_path=input_path, output_path=output_path)], **kwargs)
