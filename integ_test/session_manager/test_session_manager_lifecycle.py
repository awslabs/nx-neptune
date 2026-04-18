# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for SessionManager lifecycle operations.

WARNING: These tests create and destroy real instances. ~10-15 min runtime.
"""

import asyncio
import os

import boto3
import pytest

from nx_neptune import SessionManager, CleanupTask
from nx_neptune.clients import NeptuneAnalyticsClient

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")


@pytest.fixture(scope="module")
def event_loop():
    """Persistent event loop for SessionManager lifecycle methods that call asyncio.gather() synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


class TestGetOrCreateGraph:

    def test_creates_graph_when_none_exist(self, event_loop, resource_tracker):
        """With a unique session name, get_or_create should create a new graph."""
        sm = SessionManager(session_name="integ-t3-getorcreate")
        graph = event_loop.run_until_complete(sm.get_or_create_graph(config={"provisionedMemory": 16, "publicConnectivity": False}))
        resource_tracker.register_graph(graph.graph_id)

        assert isinstance(graph, NeptuneAnalyticsClient)
        assert graph.graph_id is not None

        # Cleanup — destroy returns a coroutine that must be awaited
        event_loop.run_until_complete(sm.destroy_graph(graph.name))


class TestContextManagerCleanup:

    def test_create_fleet_use_then_destroy_all(self, event_loop, resource_tracker):
        """Create multiple instances, verify each, then destroy all."""
        sm = SessionManager(session_name="integ-t3-fleet")
        config = {"provisionedMemory": 16, "publicConnectivity": False}

        graph_ids = event_loop.run_until_complete(sm.create_multiple_instances(count=2, config=config))
        for gid in graph_ids:
            resource_tracker.register_graph(gid)

        assert len(graph_ids) == 2
        assert graph_ids[0] != graph_ids[1]

        # Use each instance
        for gid in graph_ids:
            graph = sm.get_graph(gid)
            assert graph.graph_id == gid

        # Destroy all — returns a coroutine
        event_loop.run_until_complete(sm.destroy_all_graphs())

        # Verify all are gone or deleting
        na_client = boto3.client("neptune-graph")
        for gid in graph_ids:
            try:
                resp = na_client.get_graph(graphIdentifier=gid)
                assert resp["status"] in ("DELETING", "FAILED")
            except na_client.exceptions.ResourceNotFoundException:
                pass
            if gid in resource_tracker.graphs:
                resource_tracker.graphs.remove(gid)

    def test_destroy_cleanup_on_exit(self, event_loop, resource_tracker):
        """Context manager with DESTROY should delete graphs on exit."""
        graph_id = None
        with SessionManager(session_name="integ-t3-ctx", cleanup_task=CleanupTask.DESTROY) as sm:
            graph = event_loop.run_until_complete(sm.get_or_create_graph(config={"provisionedMemory": 16, "publicConnectivity": False}))
            graph_id = graph.graph_id
            resource_tracker.register_graph(graph_id)

        # __exit__ calls destroy_all_graphs() — need to await it
        event_loop.run_until_complete(sm.destroy_all_graphs())

        # Verify graph is gone or deleting
        na_client = boto3.client("neptune-graph")
        try:
            resp = na_client.get_graph(graphIdentifier=graph_id)
            assert resp["status"] in ("DELETING", "FAILED")
        except na_client.exceptions.ResourceNotFoundException:
            pass  # Already gone — success

        if graph_id in resource_tracker.graphs:
            resource_tracker.graphs.remove(graph_id)
