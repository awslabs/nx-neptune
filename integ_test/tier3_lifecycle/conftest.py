# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tier 4 fixtures — creates and destroys Neptune Analytics instances.

WARNING: These tests create real AWS resources and take ~10-15 minutes.

Env vars:
  NETWORKX_GRAPH_ID              - existing graph (for snapshot source)
  NETWORKX_S3_EXPORT_BUCKET_PATH - S3 path (for create_from_csv tests)
"""

import logging
import os

import pytest

from nx_neptune import NETWORKX_GRAPH_ID, SessionManager, CleanupTask

logging.basicConfig(level=logging.INFO)
logging.getLogger("nx_neptune").setLevel(logging.INFO)

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")
SESSION_PREFIX = "integ-t4"


@pytest.fixture(scope="module", autouse=True)
def _require_config():
    if not NETWORKX_GRAPH_ID:
        pytest.skip("NETWORKX_GRAPH_ID not set")


@pytest.fixture(scope="module")
def session_manager():
    """SessionManager with DESTROY cleanup — deletes all created graphs on exit."""
    sm = SessionManager(session_name=SESSION_PREFIX, cleanup_task=CleanupTask.DESTROY)
    yield sm
    # Cleanup: destroy any graphs left from this session
    sm.destroy_all_graphs()
