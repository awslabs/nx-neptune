# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for snapshot create and delete operations.

Uses the existing graph (NETWORKX_GRAPH_ID) to create a snapshot, verify it,
then delete it. Does NOT create a new instance from the snapshot (that's tier 4).
"""

import asyncio

import pytest

from nx_neptune import NETWORKX_GRAPH_ID


class TestSnapshotCreateAndDelete:

    def test_create_snapshot_then_delete(self, session_manager, resource_tracker):
        """Create a snapshot of the test graph, verify, then delete."""
        graph = session_manager.get_graph(NETWORKX_GRAPH_ID)

        snapshot_id = asyncio.run(session_manager.create_snapshot(graph, "integ-t2-snapshot"))
        resource_tracker.register_snapshot(snapshot_id)

        assert snapshot_id is not None
        assert isinstance(snapshot_id, str)

        # Delete
        deleted_id = asyncio.run(session_manager.delete_snapshot(snapshot_id))
        assert deleted_id == snapshot_id
        resource_tracker.snapshots.remove(snapshot_id)
