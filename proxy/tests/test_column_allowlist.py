# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQLite update column allowlist enforcement."""

import pytest

from nx_neptune_proxy.services.projection_store import store


class TestColumnAllowlist:
    """Store.update() must reject columns not in _ALLOWED_UPDATE_COLUMNS."""

    def test_invalid_column_raises_value_error(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, id="injected-id")

    def test_invalid_column_created_at_rejected(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, created_at="2020-01-01")

    def test_arbitrary_column_rejected(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, drop_table="yes")

    def test_multiple_invalid_columns_rejected(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, malicious="x", another_bad="y")

    def test_valid_column_accepted(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        result = store.update(p.id, status="importing")
        assert result.status == "importing"
