# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""get_settings — the single validated Settings instance shared process-wide."""

import pytest

import nx_neptune_proxy.config as config_module
from nx_neptune_proxy.config import get_settings


@pytest.fixture(autouse=True)
def reset_settings_cache():
    """Ensure each test starts and ends with a clean settings cache, since
    get_settings caches in a module global."""
    config_module._settings = None
    yield
    config_module._settings = None


def test_returns_same_instance_across_calls(monkeypatch):
    """Validate-and-enforce parity: every consumer gets the exact same
    object, so the instance validated on first access is the instance
    everyone (e.g. the managed-graph guard) later reads."""
    monkeypatch.setenv("GRAPH_PREFIX", "nxp-")
    first = get_settings()
    second = get_settings()
    assert first is second


def test_validates_on_first_access(monkeypatch):
    """An empty GRAPH_PREFIX is rejected the first time settings are read,
    not silently accepted."""
    monkeypatch.setenv("GRAPH_PREFIX", "")
    with pytest.raises(SystemExit):
        get_settings()


def test_guard_reads_the_validated_instance(monkeypatch):
    """The guard's prefix comes from the same shared instance."""
    monkeypatch.setenv("GRAPH_PREFIX", "custom-")
    from nx_neptune_proxy.utils.aws_helper import assert_managed_graph

    # Populate the shared instance, then confirm the guard honors its prefix.
    assert get_settings().graph_prefix == "custom-"
    assert_managed_graph("custom-graph")  # should not raise

    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        assert_managed_graph("nxp-graph")
