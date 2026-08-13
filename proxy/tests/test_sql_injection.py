# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL injection resistance via parameterized queries."""

from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.services.query_store import query_store


class TestSqlInjection:
    """Parameterized queries must store injection payloads as literal strings."""

    def test_injection_in_graph_name(self):
        payload = "'; DROP TABLE projections; --"
        p = store.create(database="testdb", graph_name=payload)
        # Table still exists and projection is retrievable
        retrieved = store.get(p.id)
        assert retrieved is not None
        assert retrieved.graph_name == payload

    def test_injection_in_database_field(self):
        payload = "x' OR '1'='1"
        p = store.create(database=payload, graph_name="test")
        retrieved = store.get(p.id)
        assert retrieved.database == payload

    def test_injection_in_update_value(self):
        p = store.create(database="testdb", graph_name="test")
        payload = "'; DELETE FROM projections WHERE '1'='1"
        store.update(p.id, error=payload)
        # Projection still exists with payload stored as literal
        retrieved = store.get(p.id)
        assert retrieved.error == payload
        # Verify other projections are not affected
        p2 = store.create(database="testdb2", graph_name="test2")
        assert store.get(p2.id) is not None

    def test_injection_in_node_query_field(self):
        payload = "SELECT * FROM t; DROP TABLE projections;--"
        p = store.create(database="testdb", graph_name="test")
        query_store.save_node_queries(p.id, [{"sql": payload}])
        retrieved = query_store.list_node_queries(p.id)
        assert len(retrieved) == 1
        assert retrieved[0].sql == payload

    def test_unicode_injection(self):
        payload = "test\u0000'; DROP TABLE projections;--"
        p = store.create(database="testdb", graph_name=payload)
        retrieved = store.get(p.id)
        assert retrieved is not None
        assert retrieved.graph_name == payload
