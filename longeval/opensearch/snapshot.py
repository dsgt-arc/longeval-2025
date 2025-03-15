"""Module for snapshotting operations.

This includes creating new snapshots, and restoring from existing snapshots. We
assume that the snapshots are stored to a local filesystem. We synchronize these
snapsots to a remote location using rsync.
"""

from datetime import datetime
from typing import Dict, Any

from opensearchpy import OpenSearch
import typer
import logging

app = typer.Typer(no_args_is_help=True)


def register_repository(client: OpenSearch, repo_name: str, repo_path: str) -> bool:
    """Register a repository for snapshots if it doesn't already exist."""
    existing_repos = client.snapshot.get_repository(repository=repo_name, ignore=404)
    if repo_name in existing_repos:
        logging.info(f"Repository {repo_name} already exists")
        return True

    response = client.snapshot.create_repository(
        repository=repo_name, body={"type": "fs", "settings": {"location": repo_path}}
    )
    if response.get("acknowledged"):
        logging.info(f"Repository {repo_name} created successfully")
        return True
    else:
        logging.error(f"Failed to create repository {repo_name}")
        return False


@app.command("snapshot")
def snapshot(
    repo_name: str,
    snapshot_name: str,
    indices: str = "*",
    host: str = "localhost:9200",
    repo_path: str = "/var/opensearch/snapshots",
    wait_for_completion: bool = True,
) -> Dict[str, Any]:
    """Create a snapshot of OpenSearch indices."""
    client = OpenSearch(host)
    if not register_repository(client, repo_name, repo_path):
        raise Exception(f"Failed to register repository {repo_name}")

    # Generate snapshot name with timestamp if not provided
    if snapshot_name is None:
        snapshot_name = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logging.info(f"Creating snapshot {snapshot_name} in repository {repo_name}")
    response = client.snapshot.create(
        repository=repo_name,
        snapshot=snapshot_name,
        body={
            "indices": indices,
            "ignore_unavailable": True,
            "include_global_state": True,
        },
        wait_for_completion=wait_for_completion,
    )

    logging.info(f"Snapshot {snapshot_name} created successfully")
    return response


@app.command("restore")
def restore(
    repo_name: str,
    snapshot_name: str,
    indices: str = "*",
    repo_path: str = "/var/opensearch/snapshots",
    host: str = "localhost:9200",
    wait_for_completion: bool = True,
) -> Dict[str, Any]:
    """Restore a snapshot from the repository."""
    client = OpenSearch(host)
    if not register_repository(client, repo_name, repo_path):
        raise Exception(f"Failed to register repository {repo_name}")

    # Perform restore
    logging.info(f"Restoring snapshot {snapshot_name} from repository {repo_name}")
    response = client.snapshot.restore(
        repository=repo_name,
        snapshot=snapshot_name,
        body={
            "indices": indices,
            "ignore_unavailable": True,
            "include_global_state": True,
        },
        wait_for_completion=wait_for_completion,
    )

    logging.info(f"Snapshot {snapshot_name} restored successfully")
    return response


if __name__ == "__main__":
    typer.run(snapshot)
