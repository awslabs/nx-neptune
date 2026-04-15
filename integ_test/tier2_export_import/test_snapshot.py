# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for snapshot create and delete operations.

Uses the existing graph (NETWORKX_GRAPH_ID) to create a snapshot, verify it,
then delete it. Does NOT create a new instance from the snapshot (that's tier 4).
"""

import asyncio

import pytest

from nx_neptune import NETWORKX_GRAPH_ID, create_graph_snapshot, delete_graph_snapshot


class TestSnapshotCreateAndDelete:

    def test_create_snapshot_then_delete(self, resource_tracker):
        """Create a snapshot of the test graph, verify, then delete."""
        snapshot_id = asyncio.get_event_loop().run_until_complete(
            create_graph_snapshot(NETWORKX_GRAPH_ID, "integ-t2-snapshot")
        )
        resource_tracker.register_snapshot(snapshot_id)

        assert snapshot_id is not None
        assert isinstance(snapshot_id, str)

        # Delete
        deleted_id = asyncio.get_event_loop().run_until_complete(
            delete_graph_snapshot(snapshot_id)
        )
        assert deleted_id == snapshot_id
        resource_tracker.snapshots.remove(snapshot_id)
