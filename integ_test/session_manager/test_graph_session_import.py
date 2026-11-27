# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""End-to-end integration test for a Neptune Graph Session instance.

One scenario, start to finish:
  1. Upload known CSVs to a unique S3 prefix.
  2. Create a Graph Session instance (provisionedMemory parametrized over 4 and 8).
  3. Assert it is detected as a session graph.
  4. Import the uploaded data via the import path (routes to neptune.load).
  5. Assert node/edge counts and identities match what was uploaded.
  6. Explicitly reset the graph and assert it is empty.
  7. Delete the instance.

A Graph Session does not support the asynchronous import task; the library routes
its imports through the synchronous `neptune.load` procedure instead.

WARNING: Creates and destroys a real, billable graph. Several minutes runtime.

Requirements:
  NETWORKX_S3_EXPORT_BUCKET_PATH - writable S3 path (a unique sub-prefix is used)
"""

import asyncio
import logging
import os
import time
import uuid

import pytest
from botocore.exceptions import ClientError

from nx_neptune import SessionManager
from nx_neptune.clients import NeptuneAnalyticsClient
from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path
from nx_neptune.instance_management import GraphType, _get_graph_type

logging.getLogger("nx_neptune").setLevel(logging.DEBUG)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")

pytestmark = pytest.mark.skipif(
    not S3_BUCKET, reason="NETWORKX_S3_EXPORT_BUCKET_PATH not set"
)

# Known fixture data in Neptune load format: 3 nodes, 2 edges.
NODE_CSV = (
    "~id,~label,name:String\n"
    "u1,Person,Alice\n"
    "u2,Person,Bob\n"
    "u3,Person,Carol\n"
)
EDGE_CSV = (
    "~id,~from,~to,~label\n"
    "e1,u1,u2,KNOWS\n"
    "e2,u2,u3,KNOWS\n"
)
EXPECTED_NODE_IDS = {"u1", "u2", "u3"}
EXPECTED_EDGE_PAIRS = {("u1", "u2"), ("u2", "u3")}


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


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.mark.parametrize("memory", [4, 8])
def test_tiny_graph_end_to_end(event_loop, resource_tracker, s3_client, memory):
    # --- 1. Upload known CSVs to a unique staging prefix --------------------
    # A unique prefix ensures neptune.load only picks up this run's files.
    bucket, base_prefix = split_s3_arn_to_bucket_and_path(S3_BUCKET)
    run_prefix = (
        f"{base_prefix.rstrip('/')}/integ-tiny-{memory}gb-{uuid.uuid4()}/"
    ).lstrip("/")
    s3_source = f"s3://{bucket}/{run_prefix}"
    s3_client.put_object(Bucket=bucket, Key=f"{run_prefix}nodes.csv", Body=NODE_CSV.encode())
    s3_client.put_object(Bucket=bucket, Key=f"{run_prefix}edges.csv", Body=EDGE_CSV.encode())
    log.info("STEP 1 starting: upload CSVs to %s", s3_source)

    try:
        with SessionManager(session_name=f"integ-tiny-graph-{memory}gb") as sm:
            # --- 2. Create a Graph Session instance ------------------------
            # publicConnectivity=True so the graph endpoint is reachable for
            # queries from outside the VPC. deletionProtection=False so the
            # teardown delete is not blocked.
            log.info("STEP 2 starting: create Graph Session (memory=%d)", memory)
            graph = event_loop.run_until_complete(
                sm.get_or_create_graph(
                    config={
                        "provisionedMemory": memory,
                        "publicConnectivity": True,
                        "deletionProtection": False,
                    }
                )
            )
            resource_tracker.register_graph(graph.graph_id)
            assert isinstance(graph, NeptuneAnalyticsClient), (
                f"expected a NeptuneAnalyticsClient, got {type(graph).__name__}"
            )

            # --- 3. Assert the graph type is a session graph ----------------
            log.info("STEP 3 starting: assert graph type is SESSION")
            gtype = _get_graph_type(graph.graph_id, graph.client)
            assert gtype == GraphType.SESSION, (
                f"graph {graph.graph_id} resolved as {gtype}, expected "
                f"{GraphType.SESSION} (Returned graph should be a session graph)"
            )

            # --- 4. Import the uploaded data --------------------------------
            # (for a session graph) routes to neptune.load, returning a loadId.
            log.info("STEP 4 starting: import_from_csv(reset_graph_ahead=True)")
            load_id = event_loop.run_until_complete(
                sm.import_from_csv(graph, s3_source)
            )
            assert isinstance(load_id, str) and load_id, (
                f"expected a non-empty loadId string from neptune.load, got {load_id!r}"
            )

            # --- 5. Assert node/edge counts and identities ------------------
            log.info("STEP 5 starting: assert node/edge counts and identities")
            node_count = _read_with_retry(
                lambda: graph.execute_query("MATCH (n) RETURN count(n) AS c")
            )[0]["c"]
            assert node_count == len(EXPECTED_NODE_IDS), (
                f"node count {node_count} != expected {len(EXPECTED_NODE_IDS)}"
            )

            edge_count = graph.execute_query(
                "MATCH ()-[r]->() RETURN count(r) AS c"
            )[0]["c"]
            assert edge_count == len(EXPECTED_EDGE_PAIRS), (
                f"edge count {edge_count} != expected {len(EXPECTED_EDGE_PAIRS)}"
            )

            node_ids = {
                row["id"]
                for row in graph.execute_query(
                    "MATCH (n:Person) RETURN n.`~id` AS id"
                )
            }
            assert node_ids == EXPECTED_NODE_IDS, (
                f"node ids {sorted(node_ids)} != expected "
                f"{sorted(EXPECTED_NODE_IDS)}"
            )

            edge_pairs = {
                (row["src"], row["dst"])
                for row in graph.execute_query(
                    "MATCH (a)-[:KNOWS]->(b) RETURN a.`~id` AS src, b.`~id` AS dst"
                )
            }
            assert edge_pairs == EXPECTED_EDGE_PAIRS, (
                f"edge pairs {sorted(edge_pairs)} != expected "
                f"{sorted(EXPECTED_EDGE_PAIRS)}"
            )

            # --- 6. Explicit reset, then assert the graph is empty ----------
            # TBC: Until Graph Session support
            # log.info("STEP 6 starting: reset_graph then assert empty")
            # event_loop.run_until_complete(sm.reset_graph(graph.name))
            #
            # node_count_after = _read_with_retry(
            #     lambda: graph.execute_query("MATCH (n) RETURN count(n) AS c")
            # )[0]["c"]
            # assert node_count_after == 0, (
            #     f"after reset, node count {node_count_after} != 0"
            # )
            #
            # edge_count_after = graph.execute_query(
            #     "MATCH ()-[r]->() RETURN count(r) AS c"
            # )[0]["c"]
            # assert edge_count_after == 0, (
            #     f"after reset, edge count {edge_count_after} != 0"
            # )

        # --- 7. Delete the instance -----------------------------------------
        log.info("STEP 7 starting: destroy_all_graphs()")
        event_loop.run_until_complete(sm.destroy_all_graphs())
    finally:
        # Clean up the uploaded staging objects regardless of outcome.
        s3_client.delete_object(Bucket=bucket, Key=f"{run_prefix}nodes.csv")
        s3_client.delete_object(Bucket=bucket, Key=f"{run_prefix}edges.csv")
