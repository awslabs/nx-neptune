# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Root conftest for integration tests.

Provides a session-scoped ResourceTracker that records every AWS resource
created during the run so the final teardown can verify nothing leaked.
"""

import logging

import boto3
import pytest
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ResourceTracker:
    """Tracks AWS resources created during integration tests."""

    def __init__(self):
        self.graphs = []  # graph IDs
        self.snapshots = []  # snapshot IDs
        self.s3_buckets = []  # bucket names
        self.athena_tables = []  # (database, table) tuples

    def register_graph(self, graph_id: str):
        self.graphs.append(graph_id)

    def register_snapshot(self, snapshot_id: str):
        self.snapshots.append(snapshot_id)

    def register_bucket(self, bucket_name: str):
        self.s3_buckets.append(bucket_name)

    def register_table(self, database: str, table: str):
        self.athena_tables.append((database, table))


@pytest.fixture(scope="session")
def resource_tracker():
    """Session-scoped resource tracker available to all tiers."""
    tracker = ResourceTracker()
    yield tracker
    _cleanup_leaked_resources(tracker)


def _cleanup_leaked_resources(tracker: ResourceTracker):
    """Best-effort cleanup of any resources that tests failed to delete."""
    if tracker.graphs:
        na_client = boto3.client("neptune-graph")
        for graph_id in tracker.graphs:
            try:
                resp = na_client.get_graph(graphIdentifier=graph_id)
                status = resp.get("status", "")
                if status not in ("DELETING", "FAILED"):
                    logger.warning(f"Leaked graph {graph_id} (status={status}), deleting")
                    na_client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
            except ClientError:
                pass  # already gone

    if tracker.snapshots:
        na_client = boto3.client("neptune-graph")
        for snapshot_id in tracker.snapshots:
            try:
                na_client.delete_graph_snapshot(snapshotIdentifier=snapshot_id)
                logger.warning(f"Leaked snapshot {snapshot_id}, deleted")
            except ClientError:
                pass

    if tracker.s3_buckets:
        s3 = boto3.resource("s3")
        for bucket_name in tracker.s3_buckets:
            try:
                bucket = s3.Bucket(bucket_name)
                bucket.objects.all().delete()
                bucket.delete()
                logger.warning(f"Leaked S3 bucket {bucket_name}, deleted")
            except ClientError:
                pass
