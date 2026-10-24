# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for per-run S3 staging prefixes.

A failed projection used to leave its exported CSVs under the shared
``<bucket>/<projection-id>/`` prefix. The retry wrote a second set of CSVs
beside them and ``StartImportTask`` imported both, duplicating every edge
(vertices dedupe by ``~id``; edges do not).

The pipeline now stages each run under ``<bucket>/<projection-id>/<run-id>/``
and removes only that run's prefix in a ``finally`` block.
"""

import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nx_neptune_proxy.services.pipeline import (
    _build_staging_prefix,
    _cleanup_staging_prefix,
    _has_non_empty_key,
    run_pipeline,
)
from nx_neptune_proxy.services.projection_store import Projection

BUCKET = "s3://test-bucket/staging/"


def _make_projection(**overrides) -> Projection:
    defaults = dict(
        id=str(uuid.uuid4()),
        status="draft",
        database="mydb",
        node_query="SELECT * FROM t",
        graph_name="my-graph",
        s3_staging_bucket=BUCKET,
    )
    defaults.update(overrides)
    return Projection(**defaults)


def _configure_session_manager(mock_sm_cls, import_side_effect=None):
    sm = mock_sm_cls.return_value
    graph = MagicMock()
    graph.graph_id = "g-test"
    graph.name = "nxp-my-graph"
    sm.get_or_create_graph = AsyncMock(return_value=graph)
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(
        side_effect=import_side_effect, return_value="task-1"
    )
    return sm


def _assert_only_run_prefix_cleaned(mock_empty, prefix: str, projection: Projection):
    """Assert empty_s3_bucket only ever saw this run's non-empty prefix."""
    shared_prefix = f"{BUCKET}{projection.id}/"
    assert prefix.startswith(shared_prefix)
    assert prefix != shared_prefix
    for call in mock_empty.call_args_list:
        location = call.args[0]
        assert location != BUCKET
        assert location != BUCKET.rstrip("/")
        assert location != shared_prefix
        assert location.startswith(shared_prefix)


# --- Prefix construction and cleanup safety ---


def test_build_staging_prefix_is_unique_per_run():
    projection = _make_projection()
    first = _build_staging_prefix(
        projection.s3_staging_bucket, projection.id, uuid.uuid4().hex
    )
    second = _build_staging_prefix(
        projection.s3_staging_bucket, projection.id, uuid.uuid4().hex
    )
    shared_prefix = f"{BUCKET}{projection.id}/"

    assert first.startswith(shared_prefix)
    assert second.startswith(shared_prefix)
    assert first.endswith("/")
    assert first != second
    assert first != shared_prefix


def test_build_staging_prefix_normalizes_bucket_trailing_slash():
    assert (
        _build_staging_prefix("s3://bucket/staging/", "projection-id", "run-id")
        == "s3://bucket/staging/projection-id/run-id/"
    )


@pytest.mark.parametrize("bucket", [None, "", "   "])
def test_build_staging_prefix_rejects_empty_bucket(bucket):
    with pytest.raises(ValueError):
        _build_staging_prefix(bucket, "projection-id", "run-id")


@pytest.mark.parametrize(
    "location",
    [None, "", "s3://bucket", "s3://bucket/", "bucket/prefix/"],
)
def test_has_non_empty_key_rejects_bucket_root(location):
    assert not _has_non_empty_key(location)


def test_has_non_empty_key_accepts_run_prefix():
    assert _has_non_empty_key("s3://bucket/projection-id/run-id/")


@pytest.mark.parametrize(
    "location",
    [None, "", "s3://bucket", "s3://bucket/", "bucket/prefix/"],
)
def test_cleanup_refuses_to_empty_bucket_root(location):
    with patch("nx_neptune.instance_management.empty_s3_bucket") as mock_empty:
        _cleanup_staging_prefix(location)
    mock_empty.assert_not_called()


def test_cleanup_failure_is_logged_and_swallowed(caplog):
    with (
        patch(
            "nx_neptune.instance_management.empty_s3_bucket",
            side_effect=RuntimeError("access denied"),
        ) as mock_empty,
        caplog.at_level(logging.ERROR),
    ):
        _cleanup_staging_prefix("s3://bucket/projection-id/run-id/")

    mock_empty.assert_called_once_with("s3://bucket/projection-id/run-id/")
    assert "Failed to clean up staging prefix" in caplog.text


# --- run_pipeline integration ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.services.pipeline.projection_service")
@patch("nx_neptune.instance_management.empty_s3_bucket")
@patch("nx_neptune.session_manager.SessionManager")
async def test_successful_runs_use_and_clean_unique_run_prefixes(
    mock_sm_cls, mock_empty, mock_service
):
    projection = _make_projection()
    sm = _configure_session_manager(mock_sm_cls)
    mock_service.list_query_sql.return_value = ["SELECT * FROM t"]

    await run_pipeline(projection)
    await run_pipeline(projection)

    prefixes = [
        call.kwargs["s3_location"] for call in sm.import_from_table.await_args_list
    ]
    shared_prefix = f"{BUCKET}{projection.id}/"

    assert len(prefixes) == 2
    assert prefixes[0] != prefixes[1]
    for prefix in prefixes:
        assert prefix.startswith(shared_prefix)
        assert prefix != shared_prefix
        assert prefix[len(shared_prefix) :].strip("/")

    # Cleanup targets exactly the same per-run prefix that was imported.
    assert [call.args[0] for call in mock_empty.call_args_list] == prefixes
    assert mock_empty.call_count == 2
    for prefix in prefixes:
        _assert_only_run_prefix_cleaned(mock_empty, prefix, projection)


@pytest.mark.asyncio
@patch("nx_neptune_proxy.services.pipeline.projection_service")
@patch("nx_neptune.instance_management.empty_s3_bucket")
@patch("nx_neptune.session_manager.SessionManager")
async def test_failed_import_still_cleans_only_that_run_prefix(
    mock_sm_cls, mock_empty, mock_service
):
    projection = _make_projection()
    sm = _configure_session_manager(
        mock_sm_cls, import_side_effect=RuntimeError("StartImportTask failed")
    )
    mock_service.list_query_sql.return_value = ["SELECT * FROM t"]

    # The pipeline records the failure but must not re-raise in the background task.
    await run_pipeline(projection)

    prefix = sm.import_from_table.call_args.kwargs["s3_location"]
    mock_empty.assert_called_once_with(prefix)
    _assert_only_run_prefix_cleaned(mock_empty, prefix, projection)
    mock_service.update.assert_any_call(
        projection.id, status="failed", error="StartImportTask failed"
    )


@pytest.mark.asyncio
@patch("nx_neptune_proxy.services.pipeline.projection_service")
@patch("nx_neptune.instance_management.empty_s3_bucket")
@patch("nx_neptune.session_manager.SessionManager")
async def test_cleanup_failure_does_not_mask_import_error(
    mock_sm_cls, mock_empty, mock_service, caplog
):
    projection = _make_projection()
    sm = _configure_session_manager(
        mock_sm_cls, import_side_effect=RuntimeError("original failure")
    )
    mock_service.list_query_sql.return_value = ["SELECT * FROM t"]
    mock_empty.side_effect = RuntimeError("cleanup denied")

    with caplog.at_level(logging.ERROR):
        await run_pipeline(projection)

    prefix = sm.import_from_table.call_args.kwargs["s3_location"]
    mock_empty.assert_called_once_with(prefix)
    mock_service.update.assert_any_call(
        projection.id, status="failed", error="original failure"
    )
    assert "Failed to clean up staging prefix" in caplog.text
