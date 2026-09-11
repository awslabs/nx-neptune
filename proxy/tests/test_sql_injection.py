# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL injection resistance via parameterized queries."""

from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.services.query_store import query_store


class TestSqlInjection:
    """Parameterized queries must store injection payloads as literal strings."""

    def test_injection_in_graph_name(self, test_project_id):
        payload = "'; DROP TABLE projections; --"
        p = store.create(
            database="testdb", graph_name=payload, project_id=test_project_id
        )
        # Table still exists and projection is retrievable
        retrieved = store.get(p.id)
        assert retrieved is not None
        assert retrieved.graph_name == payload

    def test_injection_in_database_field(self, test_project_id):
        payload = "x' OR '1'='1"
        p = store.create(
            database=payload, graph_name="test", project_id=test_project_id
        )
        retrieved = store.get(p.id)
        assert retrieved.database == payload

    def test_injection_in_update_value(self, test_project_id):
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        payload = "'; DELETE FROM projections WHERE '1'='1"
        store.update(p.id, error=payload)
        # Projection still exists with payload stored as literal
        retrieved = store.get(p.id)
        assert retrieved.error == payload
        # Verify other projections are not affected
        p2 = store.create(
            database="testdb2", graph_name="test2", project_id=test_project_id
        )
        assert store.get(p2.id) is not None

    def test_injection_in_node_query_field(self, test_project_id):
        payload = "SELECT * FROM t; DROP TABLE projections;--"
        p = store.create(
            database="testdb",
            node_query=payload,
            graph_name="test",
            project_id=test_project_id,
        )
        retrieved = store.get(p.id)
        assert retrieved.node_query == payload

    def test_unicode_injection(self, test_project_id):
        payload = "test\u0000'; DROP TABLE projections;--"
        p = store.create(
            database="testdb", graph_name=payload, project_id=test_project_id
        )
        retrieved = store.get(p.id)
        assert retrieved is not None
        assert retrieved.graph_name == payload

    def test_injection_in_edge_query_field(self, test_project_id):
        payload = "SELECT * FROM t; DROP TABLE edge_queries;--"
        p = store.create(
            database="testdb", graph_name="test", project_id=test_project_id
        )
        query_store.save_edge_queries(p.id, [{"sql": payload}])
        retrieved = query_store.list_edge_queries(p.id)
        assert len(retrieved) == 1
        assert retrieved[0].sql == payload

    def test_query_isolation_across_projections(self, test_project_id):
        """Queries saved to one projection must not leak to another."""
        p1 = store.create(database="db1", graph_name="g1", project_id=test_project_id)
        p2 = store.create(database="db2", graph_name="g2", project_id=test_project_id)
        query_store.save_node_queries(p1.id, [{"sql": "SELECT secret FROM p1"}])
        query_store.save_node_queries(p2.id, [{"sql": "SELECT public FROM p2"}])
        assert len(query_store.list_node_queries(p1.id)) == 1
        assert query_store.list_node_queries(p1.id)[0].sql == "SELECT secret FROM p1"
        assert len(query_store.list_node_queries(p2.id)) == 1
        assert query_store.list_node_queries(p2.id)[0].sql == "SELECT public FROM p2"
