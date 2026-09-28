# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Settings.validate — reports all configuration errors at once."""

import pytest

from nx_neptune_proxy.config import Settings


def test_valid_config_does_not_raise():
    Settings(graph_prefix="nxp-").validate()  # should not raise


def test_empty_prefix_reported():
    with pytest.raises(SystemExit) as exc_info:
        Settings(graph_prefix="").validate()
    message = str(exc_info.value)
    assert "invalid configuration" in message
    assert "GRAPH_PREFIX" in message


def test_errors_reported_as_a_list():
    """Errors are collected and rendered as a bulleted list, so multiple
    problems would all appear in one report rather than one-at-a-time."""
    with pytest.raises(SystemExit) as exc_info:
        Settings(graph_prefix="").validate()
    message = str(exc_info.value)
    # Each collected error is rendered as its own bullet.
    assert message.count("  - ") == 1
