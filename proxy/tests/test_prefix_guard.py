# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from nx_neptune_proxy.services.prefix_guard import assert_managed_graph


@patch("nx_neptune_proxy.services.prefix_guard.Settings")
def test_assert_managed_graph_passes_with_prefix(mock_settings):
    mock_settings.from_env.return_value.graph_prefix = "nxp-"
    # Should not raise
    assert_managed_graph("nxp-my-graph")


@patch("nx_neptune_proxy.services.prefix_guard.Settings")
def test_assert_managed_graph_rejects_wrong_prefix(mock_settings):
    mock_settings.from_env.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph("other-graph")
    assert exc_info.value.status_code == 403
    assert "not managed" in exc_info.value.detail


@patch("nx_neptune_proxy.services.prefix_guard.Settings")
def test_assert_managed_graph_rejects_none(mock_settings):
    mock_settings.from_env.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph(None)
    assert exc_info.value.status_code == 403


@patch("nx_neptune_proxy.services.prefix_guard.Settings")
def test_assert_managed_graph_rejects_empty_string(mock_settings):
    mock_settings.from_env.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph("")
    assert exc_info.value.status_code == 403
