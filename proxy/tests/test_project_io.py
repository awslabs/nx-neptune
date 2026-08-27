# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store


class TestProjectExport:
    @pytest.mark.anyio
    async def test_export_project(self, client):
        p = project_store.create(name="Test Project")
        projection_store.create(
            catalog="AwsDataCatalog",
            database="test_db",
            node_query="SELECT id, name FROM nodes",
            graph_name="nxp-test",
            graph_memory_gb=32,
            s3_staging_bucket="s3://bucket/staging",
            project_id=p.id,
        )

        resp = await client.get(f"/api/v0/project/{p.id}/export")
        assert resp.status_code == 200

        data = resp.json()
        assert data["version"] == "1.0"
        assert data["project"]["name"] == "Test Project"
        assert len(data["projections"]) == 1
        assert data["projections"][0]["database"] == "test_db"
        assert data["projections"][0]["graph_name"] == "nxp-test"
        assert data["projections"][0]["graph_memory_gb"] == 32

    @pytest.mark.anyio
    async def test_export_excludes_runtime_state(self, client):
        p = project_store.create(name="Test Project")
        pr = projection_store.create(
            database="db",
            graph_name="nxp-test",
            project_id=p.id,
        )
        # Simulate runtime state
        projection_store.update(
            pr.id, status="running", step="import", progress=0.5, error="oops"
        )

        resp = await client.get(f"/api/v0/project/{p.id}/export")
        data = resp.json()
        proj_export = data["projections"][0]

        # Runtime fields should not be present
        assert "status" not in proj_export
        assert "step" not in proj_export
        assert "progress" not in proj_export
        assert "error" not in proj_export
        assert "graph_id" not in proj_export
        assert "graph_endpoint" not in proj_export
        assert "id" not in proj_export

    @pytest.mark.anyio
    async def test_export_not_found(self, client):
        resp = await client.get("/api/v0/project/nonexistent/export")
        assert resp.status_code == 404


class TestProjectImport:
    @pytest.mark.anyio
    async def test_import_project(self, client):
        payload = {
            "version": "1.0",
            "project": {"name": "Imported Project"},
            "projections": [
                {
                    "catalog": "AwsDataCatalog",
                    "database": "fraud_db",
                    "node_query": "SELECT id FROM accounts",
                    "edge_query": "SELECT src, dst FROM transfers",
                    "graph_name": "nxp-fraud",
                    "graph_memory_gb": 32,
                    "s3_staging_bucket": "s3://my-bucket/staging",
                }
            ],
        }

        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 201

        data = resp.json()
        assert data["imported"]["name"] == "Imported Project"

        # Verify in DB
        projects = project_store.list()
        assert len(projects) == 1
        assert projects[0].name == "Imported Project"

        projections = projection_store.list()
        assert len(projections) == 1
        assert projections[0].database == "fraud_db"
        assert projections[0].project_id == projects[0].id

    @pytest.mark.anyio
    async def test_import_creates_new_ids(self, client):
        """Importing the same file twice creates duplicates with different IDs."""
        payload = {
            "version": "1.0",
            "project": {"name": "Test"},
            "projections": [{"database": "db"}],
        }
        content = json.dumps(payload)

        await client.post("/api/v0/project/import", content=content)
        await client.post("/api/v0/project/import", content=content)

        projects = project_store.list()
        assert len(projects) == 2
        assert projects[0].id != projects[1].id

    @pytest.mark.anyio
    async def test_import_invalid_json(self, client):
        resp = await client.post("/api/v0/project/import", content=b"not json")
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_import_extra_fields_rejected(self, client):
        payload = {
            "version": "1.0",
            "project": {"name": "Test"},
            "projections": [{"database": "db", "hacker_field": "evil"}],
        }

        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_import_too_large(self, client):
        # 6 MB of data
        payload = {
            "version": "1.0",
            "project": {"name": "x" * (6 * 1024 * 1024)},
            "projections": [],
        }

        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 413

    @pytest.mark.anyio
    async def test_import_missing_project(self, client):
        payload = {"version": "1.0"}

        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 400


class TestRoundTrip:
    @pytest.mark.anyio
    async def test_export_then_import(self, client):
        """Export a project, then import it — should recreate correctly."""
        p = project_store.create(name="Round Trip")
        projection_store.create(
            database="rt_db",
            node_query="SELECT 1",
            graph_name="nxp-roundtrip",
            graph_memory_gb=64,
            project_id=p.id,
        )

        # Export
        export_resp = await client.get(f"/api/v0/project/{p.id}/export")
        assert export_resp.status_code == 200
        export_data = export_resp.content

        # Import
        import_resp = await client.post(
            "/api/v0/project/import",
            content=export_data,
        )
        assert import_resp.status_code == 201

        # Should now have 2 projects (original + imported)
        projects = project_store.list()
        assert len(projects) == 2

        imported = [pr for pr in projects if pr.id != p.id][0]
        assert imported.name == "Round Trip"

        # Imported projection should match
        projections = [
            pr for pr in projection_store.list() if pr.project_id == imported.id
        ]
        assert len(projections) == 1
        assert projections[0].database == "rt_db"
        assert projections[0].graph_name == "nxp-roundtrip"
        assert projections[0].graph_memory_gb == 64


# --- Additional validation tests ---


class TestImportValidation:
    @pytest.mark.anyio
    async def test_import_empty_project_name(self, client):
        """Empty project name should be rejected."""
        payload = {"version": "1.0", "project": {"name": "   "}, "projections": []}
        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 400
        assert "name" in resp.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_missing_project_name(self, client):
        """Missing project name should be rejected."""
        payload = {"version": "1.0", "project": {}, "projections": []}
        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_import_project_name_too_long(self, client):
        """Project name exceeding 100 chars should be rejected."""
        payload = {"version": "1.0", "project": {"name": "x" * 101}, "projections": []}
        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == 400
        assert "too long" in resp.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_invalid_content_length(self, client):
        """Non-numeric Content-Length should return 400."""
        payload = {"version": "1.0", "project": {"name": "Test"}, "projections": []}
        resp = await client.post(
            "/api/v0/project/import",
            content=json.dumps(payload),
            headers={"content-length": "not-a-number"},
        )
        assert resp.status_code == 400
        assert "Content-Length" in resp.json()["detail"]


class TestExportFilename:
    @pytest.mark.anyio
    async def test_export_sanitizes_filename(self, client):
        """Special characters in project name should be sanitized in the filename."""
        p = project_store.create(name='My Project / "Special" <chars>')
        resp = await client.get(f"/api/v0/project/{p.id}/export")
        assert resp.status_code == 200
        disposition = resp.headers["content-disposition"]
        # Should not contain the raw special characters
        assert "/" not in disposition.split("filename=")[1]
        assert "<" not in disposition.split("filename=")[1]
        assert '"' not in disposition.split("filename=")[1].strip('"')
