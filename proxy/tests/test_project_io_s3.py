# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

import pytest

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store


class TestExportToS3:
    @pytest.mark.anyio
    async def test_export_to_s3_no_bucket_configured(self, client):
        """Returns 404 when export bucket not configured."""
        p = project_store.create(name="Test")
        resp = await client.post(f"/api/v0/project/{p.id}/export/s3")
        assert resp.status_code == 404
        assert "not configured" in resp.json()["detail"]

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_export_to_s3_success(self, mock_factory, mock_settings, client):
        """Exports project JSON to S3 with correct key format."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket/exports", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3
        # head_object raises 404 (key doesn't exist)
        from botocore.exceptions import ClientError
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        mock_s3.put_object.return_value = {}

        p = project_store.create(name="Fraud Detection")
        projection_store.create(database="fraud_db", project_id=p.id)

        resp = await client.post(f"/api/v0/project/{p.id}/export/s3")
        assert resp.status_code == 200

        data = resp.json()
        assert "filename" in data
        assert data["filename"].startswith("Fraud_Detection_")
        assert data["filename"].endswith(".json")
        assert data["key"].startswith("exports/")

        # Verify put_object was called with correct params
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["ContentType"] == "application/json"
        assert call_kwargs["Tagging"] == "graph_studio=true"

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_export_to_s3_duplicate_key(self, mock_factory, mock_settings, client):
        """Returns 409 if S3 key already exists."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3
        # head_object succeeds (key exists)
        mock_s3.head_object.return_value = {}

        p = project_store.create(name="Test")

        resp = await client.post(f"/api/v0/project/{p.id}/export/s3")
        assert resp.status_code == 409

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_export_to_s3_permission_denied(self, mock_factory, mock_settings, client):
        """Returns 502 with friendly message on permission error."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3
        from botocore.exceptions import ClientError
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )

        p = project_store.create(name="Test")

        resp = await client.post(f"/api/v0/project/{p.id}/export/s3")
        assert resp.status_code == 502
        assert "Permission denied" in resp.json()["detail"]

    @pytest.mark.anyio
    async def test_export_to_s3_project_not_found(self, client):
        """Returns 404 for non-existent project."""
        with patch("nx_neptune_proxy.routers.project_io.Settings.from_env") as mock_settings:
            mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")
            resp = await client.post("/api/v0/project/nonexistent/export/s3")
            assert resp.status_code == 404


class TestListS3Exports:
    @pytest.mark.anyio
    async def test_list_no_bucket_configured(self, client):
        """Returns 404 when export bucket not configured."""
        resp = await client.get("/api/v0/project/import/s3/list")
        assert resp.status_code == 404

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_list_returns_tagged_files(self, mock_factory, mock_settings, client):
        """Lists only files tagged with graph_studio=true, sorted by date."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket/exports", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3

        now = datetime(2026, 7, 31, 14, 0, 0, tzinfo=timezone.utc)
        earlier = datetime(2026, 7, 30, 10, 0, 0, tzinfo=timezone.utc)

        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "exports/proj_a.json", "LastModified": earlier},
                {"Key": "exports/proj_b.json", "LastModified": now},
                {"Key": "exports/untagged.json", "LastModified": now},
            ]
        }

        def mock_tagging(Bucket, Key):
            if Key == "exports/untagged.json":
                return {"TagSet": []}
            return {"TagSet": [{"Key": "graph_studio", "Value": "true"}]}

        mock_s3.get_object_tagging.side_effect = mock_tagging

        resp = await client.get("/api/v0/project/import/s3/list")
        assert resp.status_code == 200

        data = resp.json()
        assert len(data["files"]) == 2
        # Most recent first
        assert data["files"][0]["filename"] == "proj_b.json"
        assert data["files"][1]["filename"] == "proj_a.json"

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_list_empty_bucket(self, mock_factory, mock_settings, client):
        """Returns empty list when no files in bucket."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        resp = await client.get("/api/v0/project/import/s3/list")
        assert resp.status_code == 200
        assert resp.json()["files"] == []

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_list_caps_at_10(self, mock_factory, mock_settings, client):
        """Returns at most 10 files."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3

        # 15 files all tagged
        objects = [
            {"Key": f"file_{i}.json", "LastModified": datetime(2026, 7, 31, i, 0, 0, tzinfo=timezone.utc)}
            for i in range(15)
        ]
        mock_s3.list_objects_v2.return_value = {"Contents": objects}
        mock_s3.get_object_tagging.return_value = {"TagSet": [{"Key": "graph_studio", "Value": "true"}]}

        resp = await client.get("/api/v0/project/import/s3/list")
        assert resp.status_code == 200
        assert len(resp.json()["files"]) == 10


class TestImportFromS3:
    @pytest.mark.anyio
    async def test_import_from_s3_no_bucket(self, client):
        """Returns 404 when export bucket not configured."""
        resp = await client.post("/api/v0/project/import/s3", content=json.dumps({"key": "test.json"}))
        assert resp.status_code == 404

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_import_from_s3_success(self, mock_factory, mock_settings, client):
        """Imports a project from S3 file."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket/exports", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3

        export_data = {
            "version": "1.0",
            "project": {"name": "From S3"},
            "projections": [{"database": "s3_db", "graph_name": "nxp-s3"}],
        }
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(export_data).encode()
        mock_s3.get_object.return_value = {"Body": mock_body}

        resp = await client.post(
            "/api/v0/project/import/s3",
            content=json.dumps({"key": "exports/test.json"}),
        )
        assert resp.status_code == 201

        data = resp.json()
        assert data["imported"]["name"] == "From S3"

        # Verify in DB
        projects = project_store.list()
        assert len(projects) == 1
        assert projects[0].name == "From S3"

        projections = projection_store.list()
        assert len(projections) == 1
        assert projections[0].database == "s3_db"

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_import_from_s3_file_not_found(self, mock_factory, mock_settings, client):
        """Returns 502 when S3 key doesn't exist."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3
        from botocore.exceptions import ClientError
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}, "GetObject"
        )

        resp = await client.post(
            "/api/v0/project/import/s3",
            content=json.dumps({"key": "nonexistent.json"}),
        )
        assert resp.status_code == 502
        assert "not found" in resp.json()["detail"].lower()

    @pytest.mark.anyio
    @patch("nx_neptune_proxy.routers.project_io.Settings.from_env")
    @patch("nx_neptune_proxy.routers.project_io.ClientFactory")
    async def test_import_from_s3_invalid_json(self, mock_factory, mock_settings, client):
        """Returns 400 when S3 file contains invalid JSON."""
        mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")

        mock_s3 = MagicMock()
        mock_factory.return_value.s3.return_value = mock_s3

        mock_body = MagicMock()
        mock_body.read.return_value = b"not json at all"
        mock_s3.get_object.return_value = {"Body": mock_body}

        resp = await client.post(
            "/api/v0/project/import/s3",
            content=json.dumps({"key": "bad.json"}),
        )
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_import_from_s3_missing_key_param(self, client):
        """Returns 400 when key is missing from request body."""
        with patch("nx_neptune_proxy.routers.project_io.Settings.from_env") as mock_settings:
            mock_settings.return_value = MagicMock(export_bucket="my-bucket", region="us-west-2")
            resp = await client.post("/api/v0/project/import/s3", content=json.dumps({}))
            assert resp.status_code == 400
            assert "key" in resp.json()["detail"].lower()


class TestSanitizeName:
    def test_spaces_to_underscores(self):
        from nx_neptune_proxy.routers.project_io import _sanitize_name
        assert _sanitize_name("Fraud Detection") == "Fraud_Detection"

    def test_special_characters_stripped(self):
        from nx_neptune_proxy.routers.project_io import _sanitize_name
        assert _sanitize_name("My Project! (v2)") == "My_Project_v2"

    def test_hyphens_kept(self):
        from nx_neptune_proxy.routers.project_io import _sanitize_name
        assert _sanitize_name("fraud-detection") == "fraud-detection"

    def test_underscores_kept(self):
        from nx_neptune_proxy.routers.project_io import _sanitize_name
        assert _sanitize_name("my_project") == "my_project"


class TestParseBucketConfig:
    def test_bucket_only(self):
        from nx_neptune_proxy.routers.project_io import _parse_bucket_config
        bucket, prefix = _parse_bucket_config("my-bucket")
        assert bucket == "my-bucket"
        assert prefix == ""

    def test_bucket_with_prefix(self):
        from nx_neptune_proxy.routers.project_io import _parse_bucket_config
        bucket, prefix = _parse_bucket_config("my-bucket/team/exports")
        assert bucket == "my-bucket"
        assert prefix == "team/exports"
