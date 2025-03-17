import luigi
from longeval.collection import RawCollection
from longeval.spark import spark_resource
from pathlib import Path
import typer
from typing_extensions import Annotated

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, collect_list, expr
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
import numpy as np
import pandas as pd
import os
from pyspark.sql.functions import col, array_max, array_position
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE


def create_lda_model(spark, docs, output_path, num_topics):
    tokenizer = Tokenizer(inputCol="contents", outputCol="words")
    tokenized = tokenizer.transform(docs)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered = remover.transform(tokenized)
    vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="features")
    vector_model = vectorizer.fit(filtered)
    vectorized_docs = vector_model.transform(filtered)
    lda = LDA(k=num_topics, maxIter=10, featuresCol="features", optimizer="em")
    lda_model = lda.fit(vectorized_docs)
    topics = lda_model.describeTopics(maxTermsPerTopic=100)
    vocab = vector_model.vocabulary  
    if not vocab:
        raise ValueError("Vocabulary is empty. Please check the CountVectorizer step.")
    topic_words = {}
    for row in topics.collect():
        topic_index = row.topic
        term_indices = row.termIndices
        terms = [vocab[index] for index in term_indices]  # Map indices to words
        topic_words[topic_index] = terms
    topic_words_file_path = os.path.join(output_path, "topicWords.txt")
    with open(topic_words_file_path, 'w') as f:
        for topic, words in topic_words.items():
            f.write(f"Topic {topic}: {', '.join(words)}\n")
    topic_distributions = lda_model.transform(vectorized_docs)
    doc_topic_df = topic_distributions.select("docid", "topicDistribution").toPandas()
    doc_topic_array = np.vstack(doc_topic_df["topicDistribution"].apply(lambda v: v.toArray()))
    doc_topic_df['highest_topic'] = np.argmax(doc_topic_array, axis=1)  # Get index of the highest topic
    topic_probabilities_df = pd.DataFrame(doc_topic_array, columns=[f'topic_{i}' for i in range(doc_topic_array.shape[1])])
    result_df = pd.concat([doc_topic_df[['docid']], 
                        pd.DataFrame({'highest_topic': doc_topic_df['highest_topic']}), 
                        topic_probabilities_df], axis=1)
    
    output_file_path = os.path.join(output_path, "docTopicDistribution.csv")
    result_df.to_csv(output_file_path, index=False)
    topic_distribution = lda_model.transform(vectorized_docs)
    topic_distribution_list = topic_distribution.select("topicDistribution").collect()
    topic_counts = []
    for row in topic_distribution_list:
        topic_counts.append(row.topicDistribution)
    topic_counts_df = pd.DataFrame(topic_counts, columns=[f'Topic {i}' for i in range(num_topics)])
    topic_sums = topic_counts_df.sum().sort_values(ascending=False)
    plt.figure(figsize=(10, 6))
    topic_sums.plot(kind='bar', color='skyblue')
    plt.title('Topic Distribution')
    plt.xlabel('Topics')
    plt.ylabel('Number of Documents')
    plt.xticks(rotation=45)
    output_file_path = os.path.join(output_path, 'topic_distribution.png')
    plt.savefig(output_file_path)
    generatePcaGraph(lda_model, vectorized_docs, output_path, num_topics)
    generateTsneGraph(lda_model, vectorized_docs, output_path, num_topics)

def generatePcaGraph(lda_model, vectorized_docs, output_path, k):
    doc_topics = lda_model.transform(vectorized_docs)  # Output: Soft assignment of docs to topics
    doc_topic_df = doc_topics.select("topicDistribution").toPandas()
    doc_topic_array = np.vstack(doc_topic_df["topicDistribution"].apply(lambda v: v.toArray()))
    pca = PCA(n_components=2)  # Reduce to 2D
    doc_topic_2d = pca.fit_transform(doc_topic_array)
    dominant_topics = np.argmax(doc_topic_array, axis=1)
    df_plot = pd.DataFrame({
        "x": doc_topic_2d[:, 0],
        "y": doc_topic_2d[:, 1],
        "topic": dominant_topics
    })
    plt.figure(figsize=(10, 6))
    cmapValue = "tab20" 
    scatter = plt.scatter(df_plot["x"], df_plot["y"], c=df_plot["topic"], cmap=cmapValue, alpha=0.6)
    cbar = plt.colorbar(scatter, label="Topic")
    cbar.set_ticks(range(k))  
    cbar.set_ticklabels(range(k))  
    plt.xlabel("PCA Dimension 1")
    plt.ylabel("PCA Dimension 2")
    plt.title("LDA Document Clusters via PCA")
    output_file_path = os.path.join(output_path, 'pca_topics.png')
    plt.savefig(output_file_path)

def generateTsneGraph(lda_model, vectorized_docs, output_path, k):
    doc_topics = lda_model.transform(vectorized_docs)  
    doc_topic_df = doc_topics.select("topicDistribution").toPandas()
    doc_topic_array = np.vstack(doc_topic_df["topicDistribution"].apply(lambda v: v.toArray()))
    tsne = TSNE(n_components=2, perplexity=30, random_state=42)
    doc_topic_2d = tsne.fit_transform(doc_topic_array)
    dominant_topics = np.argmax(doc_topic_array, axis=1)
    df_plot = pd.DataFrame({
        "x": doc_topic_2d[:, 0],
        "y": doc_topic_2d[:, 1],
        "topic": dominant_topics
    })
    plt.figure(figsize=(10, 6))
    cmapValue = "tab20" 
    scatter = plt.scatter(df_plot["x"], df_plot["y"], c=df_plot["topic"], cmap=cmapValue, alpha=0.6)
    cbar = plt.colorbar(scatter, label="Topic")
    cbar.set_ticks(range(k))  
    cbar.set_ticklabels(range(k))  
    plt.xlabel("t-SNE Dimension 1")
    plt.ylabel("t-SNE Dimension 2")
    plt.title("LDA Document Clusters via t-SNE")
    output_file_path = os.path.join(output_path, 'tSne_topics.png')
    plt.savefig(output_file_path)

class CreateLDAModel(luigi.Task):
    input_path = luigi.Parameter(default="/mnt/data/longeval")
    output_path = luigi.Parameter(default="/mnt/data/longeval")
    num_topics = luigi.IntParameter(default=20)

    def run(self):
        print("num topics in lda model: ",  self.num_topics)
        with spark_resource() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            collection = RawCollection(spark, self.input_path)
            docs = collection.documents.cache()
            create_lda_model(spark, docs, self.output_path, self.num_topics)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "docTopicDistribution.csv"))
    
class Workflow(luigi.Task):
    num_topics = luigi.IntParameter(default=20)
    input_path = luigi.Parameter(default="/mnt/data/longeval")
    output_path = luigi.Parameter(default="/mnt/data/longeval")

    def requires(self):
        return CreateLDAModel(input_path=self.input_path, output_path=self.output_path, num_topics=self.num_topics)

    def run(self):
        print("output path: ", self.output_path)
        print("input path: ",  self.input_path)
        print("num topics: ",  self.num_topics)

        dummy_file_path = os.path.join(self.output_path, "dummy.txt")
        with open(dummy_file_path, 'w') as f:
            f.write("This is a dummy file to mark the completion of the Workflow task.")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_path, "dummy.txt"))


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