"""Commands for administering the OpenSearch indices.

The one that comes to the front of mind is the ability to delete existing indices
and templates to ensure that the latest version of an index is used. This is mostly
for development purposes.
"""

import typer
from opensearchpy import OpenSearch
import logging

app = typer.Typer(no_args_is_help=True)


@app.command("delete-indices")
def delete_indices(
    indices: str = "longeval-*",
    host: str = "localhost:9200",
    ignore_unavailable: bool = True,
) -> None:
    """Delete specified OpenSearch indices."""
    client = OpenSearch(host)
    logging.info(f"Deleting indices: {indices}")
    response = client.indices.delete(
        index=indices, ignore_unavailable=ignore_unavailable
    )
    logging.info(f"Indices deleted: {response}")


@app.command("delete-templates")
def delete_templates(
    templates: str = "longeval-*",
    host: str = "localhost:9200",
    ignore_unavailable: bool = True,
) -> None:
    """Delete specified OpenSearch index templates."""
    client = OpenSearch(host)
    logging.info(f"Deleting templates: {templates}")
    response = client.indices.delete_template(
        name=templates, ignore_unavailable=ignore_unavailable
    )
    logging.info(f"Templates deleted: {response}")


if __name__ == "__main__":
    app()
