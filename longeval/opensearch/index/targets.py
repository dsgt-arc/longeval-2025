"""
Luigi targets for OpenSearch integration.

This module provides Luigi target implementations for OpenSearch indices
and templates, allowing Luigi workflows to check for their existence.
"""

import luigi
from opensearchpy import OpenSearch


class OpenSearchIndexTarget(luigi.Target):
    """
    Luigi target that represents an OpenSearch index.

    This target is considered to exist if the specified index exists in the
    connected OpenSearch instance.
    """

    def __init__(self, index, host="localhost:9200"):
        self.host = host
        self.client = OpenSearch(host)
        self.index = index

    def exists(self):
        """Check if the target index exists in OpenSearch."""
        return self.client.indices.exists(index=self.index)


class OpenSearchIndexTemplateTarget(luigi.Target):
    """
    Luigi target that represents an OpenSearch index template.

    This target is considered to exist if the specified template exists in the
    connected OpenSearch instance.
    """

    def __init__(self, template, host="localhost:9200"):
        self.host = host
        self.client = OpenSearch(host)
        self.template = template

    def exists(self):
        """Check if the target template exists in OpenSearch."""
        return self.client.indices.exists_template(name=self.template)
