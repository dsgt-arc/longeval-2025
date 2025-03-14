import luigi
from opensearchpy import OpenSearch


class OpenSearchIndexTarget(luigi.Target):
    def __init__(self, index, host="localhost:9200"):
        self.host = host
        self.client = OpenSearch(host)
        self.index = index

    def exists(self):
        return self.client.indices.exists(index=self.index)


class OpenSearchIndexTemplateTarget(luigi.Target):
    def __init__(self, template, host="localhost:9200"):
        self.host = host
        self.client = OpenSearch(host)
        self.template = template

    def exists(self):
        return self.client.indices.exists_template(name=self.template)
