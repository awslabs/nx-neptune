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


CONFIG = {"provisionedMemory": 16, "publicConnectivity": False}


def _force_delete(graph_id, resource_tracker):
    """Delete a graph regardless of status or deletion protection."""
    na_client = boto3.client("neptune-graph")
    try:
        na_client.update_graph(graphIdentifier=graph_id, deletionProtection=False)
        na_client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
    except na_client.exceptions.ResourceNotFoundException:
        pass
    if graph_id in resource_tracker.graphs:
        resource_tracker.graphs.remove(graph_id)


class TestStopCleanupOnExit:
    """The STOP counterpart to test_destroy_cleanup_on_exit, which had none.

    Uses ``async with``: the create methods are ``async``, so this is how async
    callers are meant to use a session, and ``__aexit__`` awaits the cleanup on
    the caller's own loop.
    """

    @pytest.mark.asyncio
    async def test_stop_cleanup_on_exit(self, resource_tracker):
        """Leaving the block with STOP must leave the graph actually STOPPED."""
        graph_id = None
        async with SessionManager(
            session_name="integ-t3-stop", cleanup_task=CleanupTask.STOP
        ) as sm:
            graph = await sm.get_or_create_graph(config=CONFIG)
            graph_id = graph.graph_id
            resource_tracker.register_graph(graph_id)

        try:
            # No extra await needed: __aexit__ ran the stop to completion.
            na_client = boto3.client("neptune-graph")
            status = na_client.get_graph(graphIdentifier=graph_id)["status"]
            assert status == "STOPPED", f"expected STOPPED after exit, got {status}"
        finally:
            _force_delete(graph_id, resource_tracker)

    @pytest.mark.asyncio
    async def test_destroy_does_not_delete_a_stopped_graph(self, resource_tracker):
        """DESTROY is AVAILABLE-only, so a STOPPED graph survives it.

        A stopped graph may be stopped precisely to keep its data, and
        delete_na_instance clears deletion protection and skips the snapshot -
        so the narrow status filter is what stands between a stop and an
        unrecoverable delete.
        """
        sm = SessionManager(session_name="integ-t3-stopped-keep")
        graph = await sm.get_or_create_graph(config=CONFIG)
        graph_id = graph.graph_id
        resource_tracker.register_graph(graph_id)

        try:
            await sm.stop_graph(graph.name)
            na_client = boto3.client("neptune-graph")
            assert na_client.get_graph(graphIdentifier=graph_id)["status"] == "STOPPED"

            # The graph is in this session, but not AVAILABLE, so it is skipped.
            await sm.destroy_all_graphs()

            status = na_client.get_graph(graphIdentifier=graph_id)["status"]
            assert status == "STOPPED", f"STOPPED graph was not left alone, got {status}"
        finally:
            _force_delete(graph_id, resource_tracker)


class TestSessionMembershipAgainstRealGraphs:
    """Session membership decides whose graphs a cleanup touches.

    A graph is this session's only when its name is exactly
    ``{session_name}-{uuid4}``. A prefix test is not enough, because session
    names here are routinely hyphenated: under ``startswith('integ-sib-')``,
    session ``integ-sib`` would claim ``integ-sib-child``'s graphs, and RESET
    wipes data while DESTROY deletes without a snapshot.
    """

    @pytest.mark.asyncio
    async def test_a_hyphenated_sibling_session_is_not_claimed(self, resource_tracker):
        parent = SessionManager(session_name="integ-sib")
        child = SessionManager(session_name="integ-sib-child")

        graph = await child.get_or_create_graph(config=CONFIG)
        graph_id = graph.graph_id
        resource_tracker.register_graph(graph_id)

        try:
            assert graph.name.startswith("integ-sib-child-")

            child_ids = [g.graph_id for g in child.list_graphs()]
            parent_ids = [g.graph_id for g in parent.list_graphs()]

            assert graph_id in child_ids, "the owning session lost its own graph"
            assert graph_id not in parent_ids, (
                f"session 'integ-sib' claimed {graph.name}, which belongs to "
                f"session 'integ-sib-child'"
            )
        finally:
            _force_delete(graph_id, resource_tracker)
