# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from fastapi import HTTPException

from nx_neptune_proxy.utils.aws_helper import (
    assert_managed_graph,
    get_graph_or_exception,
)


@patch("nx_neptune_proxy.utils.aws_helper.get_settings")
def test_assert_managed_graph_passes_with_prefix(mock_get_settings):
    mock_get_settings.return_value.graph_prefix = "nxp-"
    # Should not raise
    assert_managed_graph("nxp-my-graph")


@patch("nx_neptune_proxy.utils.aws_helper.get_settings")
def test_assert_managed_graph_rejects_wrong_prefix(mock_get_settings):
    mock_get_settings.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph("other-graph")
    assert exc_info.value.status_code == 403
    assert "not managed" in exc_info.value.detail


@patch("nx_neptune_proxy.utils.aws_helper.get_settings")
def test_assert_managed_graph_rejects_none(mock_get_settings):
    mock_get_settings.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph(None)
    assert exc_info.value.status_code == 403


@patch("nx_neptune_proxy.utils.aws_helper.get_settings")
def test_assert_managed_graph_rejects_empty_string(mock_get_settings):
    mock_get_settings.return_value.graph_prefix = "nxp-"
    with pytest.raises(HTTPException) as exc_info:
        assert_managed_graph("")
    assert exc_info.value.status_code == 403


# --- get_graph_or_exception ---


def test_get_graph_or_exception_returns_response():
    mock_client = MagicMock()
    mock_client.get_graph.return_value = {"name": "nxp-test", "status": "AVAILABLE"}
    result = get_graph_or_exception(mock_client, "g-123")
    assert result == {"name": "nxp-test", "status": "AVAILABLE"}
    mock_client.get_graph.assert_called_once_with(graphIdentifier="g-123")


def test_get_graph_or_exception_raises_404_on_not_found():
    mock_client = MagicMock()
    mock_client.get_graph.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
        "GetGraph",
    )
    with pytest.raises(HTTPException) as exc_info:
        get_graph_or_exception(mock_client, "g-999")
    assert exc_info.value.status_code == 404


def test_get_graph_or_exception_reraises_other_errors():
    mock_client = MagicMock()
    mock_client.get_graph.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "Boom"}}, "GetGraph"
    )
    with pytest.raises(ClientError):
        get_graph_or_exception(mock_client, "g-123")
