# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Absence of credentials or secrets in local storage."""

from nx_neptune_proxy.services.db import get_connection
from nx_neptune_proxy.services.projection_store import store


class TestNoSecretsInDb:
    """SQLite should contain no credentials, tokens, or secrets."""

    SENSITIVE_PATTERNS = [
        "AKIA",
        "aws_secret",
        "aws_session",
        "BEGIN RSA",
        "BEGIN PRIVATE",
        "password",
    ]

    def test_no_secrets_after_creating_projections(self, test_project_id):
        """After CRUD operations, DB should contain no credential patterns."""
        # Create several projections with various data
        store.create(
            database="production_db",
            graph_name="fraud-graph",
            s3_staging_bucket="s3://my-bucket/staging/",
            node_query="SELECT user_id, name FROM users",
            project_id=test_project_id,
        )
        store.create(
            database="analytics",
            graph_name="social-graph",
            s3_staging_bucket="s3://other-bucket/data/",
            node_query="SELECT id AS `~id`, type AS `~label` FROM nodes",
            edge_query="SELECT src AS `~from`, dst AS `~to` FROM edges",
            project_id=test_project_id,
        )

        # Read the raw database content
        conn = get_connection()
        cursor = conn.execute("SELECT * FROM projections")
        rows = cursor.fetchall()
        conn.close()

        # Serialize all values to check for sensitive content
        all_values = []
        for row in rows:
            for key in row.keys():
                val = row[key]
                if val is not None:
                    all_values.append(str(val).lower())

        full_content = " ".join(all_values)

        for pattern in self.SENSITIVE_PATTERNS:
            assert (
                pattern.lower() not in full_content
            ), f"Sensitive pattern '{pattern}' found in SQLite data"

    def test_no_secrets_after_update_with_error(self, test_project_id):
        """Error messages stored in DB should not contain credentials."""
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        store.update(
            p.id,
            status="failed",
            error="AccessDeniedException: User arn:aws:iam::123456789012:user/dev is not authorized",
        )

        conn = get_connection()
        row = conn.execute(
            "SELECT error FROM projections WHERE id = ?", (p.id,)
        ).fetchone()
        conn.close()

        error_text = row["error"].lower()
        for pattern in self.SENSITIVE_PATTERNS:
            assert (
                pattern.lower() not in error_text
            ), f"Sensitive pattern '{pattern}' found in error message"
