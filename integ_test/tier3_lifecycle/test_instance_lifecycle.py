# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for Neptune Analytics instance lifecycle.

WARNING: These tests create and destroy real instances. ~10-15 min runtime.
"""

import asyncio

import pytest

from nx_neptune import (
    create_na_instance,
    delete_na_instance,
    create_graph_snapshot,
    delete_graph_snapshot,
    create_na_instance_from_snapshot,
    start_na_instance,
    stop_na_instance,
)


class TestCreateAndDeleteInstance:

    def test_create_then_delete(self, resource_tracker):
        """Create a minimal instance, verify it exists, then delete it."""
        graph_id = asyncio.get_event_loop().run_until_complete(
            create_na_instance(
                config={"provisionedMemory": 16, "publicConnectivity": False},
                graph_name_prefix="integ-t4",
            )
        )
        resource_tracker.register_graph(graph_id)

        assert graph_id is not None
        assert graph_id.startswith("g-")

        # Delete
        deleted_id = asyncio.get_event_loop().run_until_complete(
            delete_na_instance(graph_id)
        )
        assert deleted_id == graph_id
        resource_tracker.graphs.remove(graph_id)


class TestSnapshotLifecycle:

    def test_create_snapshot_then_restore_then_cleanup(self, resource_tracker):
        """Create instance → snapshot → restore from snapshot → delete all."""
        # Create source instance
        source_id = asyncio.get_event_loop().run_until_complete(
            create_na_instance(
                config={"provisionedMemory": 16, "publicConnectivity": False},
                graph_name_prefix="integ-t4-snap-src",
            )
        )
        resource_tracker.register_graph(source_id)

        # Create snapshot
        snapshot_id = asyncio.get_event_loop().run_until_complete(
            create_graph_snapshot(source_id, "integ-t4-snapshot")
        )
        resource_tracker.register_snapshot(snapshot_id)
        assert snapshot_id is not None

        # Restore from snapshot
        restored_id = asyncio.get_event_loop().run_until_complete(
            create_na_instance_from_snapshot(
                snapshot_id,
                graph_name_prefix="integ-t4-snap-rst",
            )
        )
        resource_tracker.register_graph(restored_id)
        assert restored_id is not None
        assert restored_id != source_id

        # Cleanup
        asyncio.get_event_loop().run_until_complete(delete_na_instance(restored_id))
        resource_tracker.graphs.remove(restored_id)

        asyncio.get_event_loop().run_until_complete(delete_graph_snapshot(snapshot_id))
        resource_tracker.snapshots.remove(snapshot_id)

        asyncio.get_event_loop().run_until_complete(delete_na_instance(source_id))
        resource_tracker.graphs.remove(source_id)


class TestStopAndStart:

    def test_stop_then_start(self, resource_tracker):
        """Create instance → stop → start → delete."""
        graph_id = asyncio.get_event_loop().run_until_complete(
            create_na_instance(
                config={"provisionedMemory": 16, "publicConnectivity": False},
                graph_name_prefix="integ-t4-stopstart",
            )
        )
        resource_tracker.register_graph(graph_id)

        # Stop
        stopped_id = asyncio.get_event_loop().run_until_complete(
            stop_na_instance(graph_id)
        )
        assert stopped_id == graph_id

        # Start
        started_id = asyncio.get_event_loop().run_until_complete(
            start_na_instance(graph_id)
        )
        assert started_id == graph_id

        # Cleanup
        asyncio.get_event_loop().run_until_complete(delete_na_instance(graph_id))
        resource_tracker.graphs.remove(graph_id)
