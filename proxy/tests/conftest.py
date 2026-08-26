# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.services.db import get_connection


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://localhost",
        headers={"X-Requested-With": "nx-neptune"},
    )


@pytest.fixture
def bare_client():
    """Client without X-Requested-With header — for CSRF rejection tests."""
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://localhost")


@pytest.fixture(autouse=True)
def clear_store():
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()
    yield
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()
