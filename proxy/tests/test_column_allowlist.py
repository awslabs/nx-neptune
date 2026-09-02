# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQLite update column allowlist enforcement."""

import pytest

from nx_neptune_proxy.services.projection_store import store


class TestColumnAllowlist:
    """Store.update() must reject columns not in _ALLOWED_UPDATE_COLUMNS."""

    @pytest.mark.parametrize(
        "bad_kwargs",
        [
            pytest.param({"id": "injected-id"}, id="primary_key_id_rejected"),
            pytest.param(
                {"created_at": "2020-01-01"}, id="immutable_created_at_rejected"
            ),
            pytest.param({"drop_table": "yes"}, id="arbitrary_unknown_column_rejected"),
            pytest.param(
                {"malicious": "x", "another_bad": "y"},
                id="multiple_invalid_columns_rejected",
            ),
        ],
    )
    def test_invalid_column_raises_value_error(self, test_project_id, bad_kwargs):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, **bad_kwargs)

    def test_valid_column_accepted(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        result = store.update(p.id, status="importing")
        assert result.status == "importing"
