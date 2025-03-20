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


@app.command("create")
def create(
    # Generate snapshot name with timestamp if not provided
    snapshot_name: str = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    repo_name: str = "default",
    indices: str = "*",
    host: str = "localhost:9200",
    repo_path: str = "/var/opensearch/snapshots",
    wait_for_completion: bool = True,
) -> Dict[str, Any]:
    """Create a snapshot of OpenSearch indices."""
    client = OpenSearch(host)
    if not register_repository(client, repo_name, repo_path):
        raise Exception(f"Failed to register repository {repo_name}")

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
    snapshot_name: str,
    repo_name: str = "default",
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


@app.command("list")
def list_snapshots(
    repo_name: str = "default",
    host: str = "localhost:9200",
    repo_path: str = "/var/opensearch/snapshots",
) -> None:
    """List all snapshots in the repository."""
    client = OpenSearch(host)
    if not register_repository(client, repo_name, repo_path):
        raise Exception(f"Failed to register repository {repo_name}")

    logging.info(f"Listing snapshots in repository {repo_name}")
    response = client.snapshot.get(repository=repo_name, snapshot="_all")
    for snapshot in response["snapshots"]:
        print(snapshot["snapshot"])


@app.command("cleanup")
def cleanup_snapshots(
    repo_name: str = "default",
    host: str = "localhost:9200",
    repo_path: str = "/var/opensearch/snapshots",
    retain_last: int = 5,
) -> None:
    """Cleanup old snapshots, retaining only the specified number of latest snapshots."""
    client = OpenSearch(host)
    if not register_repository(client, repo_name, repo_path):
        raise Exception(f"Failed to register repository {repo_name}")

    logging.info(f"Cleaning up snapshots in repository {repo_name}")
    response = client.snapshot.get(repository=repo_name, snapshot="_all")
    snapshots = response["snapshots"]
    snapshots.sort(key=lambda s: s["start_time_in_millis"], reverse=True)

    if len(snapshots) > retain_last:
        snapshots_to_delete = snapshots[retain_last:]
        for snapshot in snapshots_to_delete:
            snapshot_name = snapshot["snapshot"]
            logging.info(f"Deleting snapshot {snapshot_name}")
            client.snapshot.delete(repository=repo_name, snapshot=snapshot_name)
            logging.info(f"Snapshot {snapshot_name} deleted")
