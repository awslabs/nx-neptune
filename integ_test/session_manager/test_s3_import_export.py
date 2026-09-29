# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for S3 import/export operations.

Uses a single S3 path (NETWORKX_S3_EXPORT_BUCKET_PATH) for round-trip testing:
export data → import it back → verify.

Requirements:
  - S3 bucket with KMS encryption (SSE-KMS) enabled
  - S3 bucket with versioning enabled
  - IAM permissions for import/export/delete
"""

import asyncio
import os
import time

import pytest
from botocore.exceptions import ClientError

from nx_neptune import empty_s3_bucket
from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")

pytestmark = pytest.mark.skipif(
    not S3_BUCKET,
    reason="NETWORKX_S3_EXPORT_BUCKET_PATH not set"
)


def _read_with_retry(read_fn, attempts=6, delay=10):
    # Neptune can briefly reject queries right after an import completes
    # (UnprocessableException / "resubmit the query"); retry before failing.
    for attempt in range(attempts):
        try:
            return read_fn()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "UnprocessableException" and attempt < attempts - 1:
                time.sleep(delay)
                continue
            raise


class TestExportCsvToS3:

    def test_export_returns_task_id(self, session_manager, seeded_graph, s3_client):
        task_id = asyncio.run(session_manager.export_to_csv(seeded_graph.na_client, S3_BUCKET))
        assert task_id is not None
        assert isinstance(task_id, str)

        # Verify files were written to S3
        bucket_name, prefix = split_s3_arn_to_bucket_and_path(S3_BUCKET)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        assert response.get("KeyCount", 0) > 0

    def test_export_with_filter(self, session_manager, seeded_graph):
        export_filter = {
            "vertexFilter": {"Person": {}},
            "edgeFilter": {"KNOWS": {}},
        }
        task_id = asyncio.run(session_manager.export_to_csv(seeded_graph.na_client, S3_BUCKET, export_filter=export_filter))
        assert task_id is not None


class TestImportCsvFromS3:

    def test_round_trip_export_then_import(self, session_manager, seeded_graph, neptune_graph):
        """Export data, clear graph, import it back, verify."""

        empty_s3_bucket(S3_BUCKET)

        # Export
        asyncio.run(session_manager.export_to_csv(seeded_graph.na_client, S3_BUCKET))

        # Clear and import
        task_id = asyncio.run(session_manager.import_from_csv(neptune_graph.na_client, S3_BUCKET, reset_graph_ahead=True))
        assert task_id is not None

        # Verify data came back
        nodes = _read_with_retry(neptune_graph.get_all_nodes)
        assert len(nodes) >= 3
        node_ids = {n["~id"] for n in nodes}
        assert {"s1", "s2", "s3"}.issubset(node_ids)

        edges = _read_with_retry(neptune_graph.get_all_edges)
        assert len(edges) >= 2
        edge_pairs = {(e["~start"], e["~end"]) for e in edges}
        assert ("s1", "s2") in edge_pairs
        assert ("s2", "s3") in edge_pairs


class TestEmptyS3Bucket:

    def test_empty_bucket_path(self, session_manager, seeded_graph, s3_client):
        """Export data then empty the path, verify nothing remains."""
        asyncio.run(session_manager.export_to_csv(seeded_graph.na_client, S3_BUCKET))

        empty_s3_bucket(S3_BUCKET)

        bucket_name, prefix = split_s3_arn_to_bucket_and_path(S3_BUCKET)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        assert response.get("KeyCount", 0) == 0
