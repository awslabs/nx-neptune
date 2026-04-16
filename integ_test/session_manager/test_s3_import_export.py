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

import pytest

from nx_neptune import (
    Node,
    Edge,
    export_csv_to_s3,
    import_csv_from_s3,
    empty_s3_bucket,
)

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")


class TestExportCsvToS3:

    def test_export_returns_task_id(self, seeded_graph, s3_client):
        task_id = asyncio.get_event_loop().run_until_complete(
            export_csv_to_s3(seeded_graph, S3_BUCKET)
        )
        assert task_id is not None
        assert isinstance(task_id, str)

        # Verify files were written to S3
        from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path
        bucket_name, prefix = split_s3_arn_to_bucket_and_path(S3_BUCKET)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        assert response.get("KeyCount", 0) > 0

    def test_export_with_filter(self, seeded_graph):
        export_filter = {
            "vertexFilter": {"Person": {}},
            "edgeFilter": {"KNOWS": {}},
        }
        task_id = asyncio.get_event_loop().run_until_complete(
            export_csv_to_s3(seeded_graph, S3_BUCKET, export_filter=export_filter)
        )
        assert task_id is not None


class TestImportCsvFromS3:

    def test_round_trip_export_then_import(self, seeded_graph, neptune_graph):
        """Export data, clear graph, import it back, verify."""

        empty_s3_bucket(S3_BUCKET)

        # Export
        asyncio.get_event_loop().run_until_complete(
            export_csv_to_s3(seeded_graph, S3_BUCKET)
        )

        # # Clear and import
        task_id = asyncio.get_event_loop().run_until_complete(
            import_csv_from_s3(neptune_graph, S3_BUCKET, reset_graph_ahead=True)
        )
        assert task_id is not None

        # Verify data came back
        nodes = neptune_graph.get_all_nodes()
        assert len(nodes) >= 3
        node_ids = {n["~id"] for n in nodes}
        assert {"s1", "s2", "s3"}.issubset(node_ids)

        edges = neptune_graph.get_all_edges()
        assert len(edges) >= 2
        edge_pairs = {(e["~start"], e["~end"]) for e in edges}
        assert ("s1", "s2") in edge_pairs
        assert ("s2", "s3") in edge_pairs


class TestEmptyS3Bucket:

    def test_empty_bucket_path(self, seeded_graph, s3_client):
        """Export data then empty the path, verify nothing remains."""
        asyncio.get_event_loop().run_until_complete(
            export_csv_to_s3(seeded_graph, S3_BUCKET)
        )

        empty_s3_bucket(S3_BUCKET)

        from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path
        bucket_name, prefix = split_s3_arn_to_bucket_and_path(S3_BUCKET)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        assert response.get("KeyCount", 0) == 0
