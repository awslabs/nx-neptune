# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import unittest
from nx_neptune.clients.opencypher_builder import (
    match_all_nodes,
    match_all_edges,
    clear_query,
    bfs_query,
    pagerank_query,
    Node,
    Edge,
    delete_node,
    delete_edge,
    insert_node,
    insert_edge,
    update_node,
    update_edge,
)


class TestOpencypherBuilder(unittest.TestCase):
    """
    Test cases for the opencypher_builder module functions.
    """

    def test_match_all_nodes(self):
        """
        Test the match_all_nodes function to ensure it generates the correct OpenCypher query.
        """
        # Call the function
        query = match_all_nodes()

        # Check that the query is exactly as expected
        expected_query = " MATCH (n) RETURN n"
        self.assertEqual(query, expected_query)

    def test_match_all_edges(self):
        """
        Test the match_all_edges function to ensure it generates the correct OpenCypher query.
        """
        # Call the function
        query = match_all_edges()

        # Check that the query is exactly as expected
        expected_query = " MATCH (a)-[r]->(b) RETURN r"
        self.assertEqual(query, expected_query)

    def test_clear_query(self):
        """
        Test the clear_query function to ensure it generates the correct OpenCypher query
        for clearing all nodes and relationships in the graph.
        """
        # Call the function
        query = clear_query()

        # Check that the query is exactly as expected
        expected_query = " MATCH (n) DETACH DELETE n"
        self.assertEqual(query, expected_query)

    def test_bfs_query(self):
        """
        Test the bfs_query function to ensure it generates the correct OpenCypher query
        for executing a Breadth-First Search algorithm on Neptune Analytics.
        """
        # Call the function with test parameters
        source_node = "n"
        where_condition = {"id(n)": "Alice"}
        parameters = {"maxDepth": 1, "traversalDirection": "inbound"}
        query = bfs_query(source_node, where_condition, parameters)

        # Check that the query is exactly as expected
        self.assertEqual(
            query[0],
            ' MATCH (n) WHERE id(n) = $0 CALL neptune.algo.bfs.parents(n, {maxDepth:1, traversalDirection:"inbound"}) YIELD parent AS parent, node AS node RETURN parent, node',
        )
        self.assertEqual(query[1], {"0": "Alice"})

    def test_pagerank_query_default(self):
        """Test the pagerank_query function with default parameters."""
        # Test case 1: Default parameters (no parameters provided)
        query_str, params = pagerank_query()
        print(query_str)
        self.assertEqual(
            query_str,
            " MATCH (n) CALL neptune.algo.pageRank(n) YIELD rank AS rank RETURN n, rank",
        )
        self.assertEqual(params, {})

    def test_pagerank_query_with_parameters(self):
        """Test the pagerank_query function with various parameters."""
        # Define test cases as (description, parameters, expected_query)
        test_cases = [
            (
                "With dampingFactor parameter",
                {"dampingFactor": 0.5},
                " MATCH (n) CALL neptune.algo.pageRank(n, {dampingFactor:0.5}) YIELD rank AS rank RETURN n, rank",
            ),
            (
                "With numOfIterations parameter",
                {"numOfIterations": 20},
                " MATCH (n) CALL neptune.algo.pageRank(n, {numOfIterations:20}) YIELD rank AS rank RETURN n, rank",
            ),
            (
                "With numOfIterations parameter",
                {"tolerance": 0.1},
                " MATCH (n) CALL neptune.algo.pageRank(n, {tolerance:0.1}) YIELD rank AS rank RETURN n, rank",
            ),
            (
                "With numOfIterations parameter",
                {"edgeWeightProperty": "test_field", "edgeWeightType": "double"},
                ' MATCH (n) CALL neptune.algo.pageRank(n, {edgeWeightProperty:"test_field", edgeWeightType:"double"}) YIELD rank AS rank RETURN n, rank',
            ),
        ]

        for desc, parameters, expected_query in test_cases:
            with self.subTest(description=desc):
                query_str, params = pagerank_query(parameters)
                self.assertEqual(query_str, expected_query)

    def test_insert_edge_parameterized(self):
        """
        Parameterized test for inserting edges with various configurations using Edge with embedded nodes.
        """
        # Define test cases as a list of tuples: (description, edge, expected_query, expected_params)
        test_cases = [
            # Basic edge between nodes
            (
                "Basic edge",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"name": "Bob"}
                    ),
                ),
                " MERGE (a: Person {name : $0, `~id` : $1}) MERGE (b: Person {name : $2, `~id` : $3}) MERGE (a)-[r: FRIEND_WITH]->(b)",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
            # Edge without properties
            (
                "Edge without properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"]),
                    node_dest=Node(id="456", labels=["Person"]),
                ),
                " MERGE (a: Person {`~id` : $0}) MERGE (b: Person {`~id` : $1}) MERGE (a)-[r: FRIEND_WITH]->(b)",
                {"0": "123", "1": "456"},
            ),
            # Edge with properties
            (
                "Edge with properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2020"},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"name": "Bob"}
                    ),
                ),
                " MERGE (a: Person {name : $0, `~id` : $1}) MERGE (b: Person {name : $2, `~id` : $3}) MERGE (a)-[r: FRIEND_WITH {since : $4}]->(b)",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456", "4": "2020"},
            ),
            # Edge between nodes with multiple labels
            (
                "Edge between nodes with multiple labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(
                        id="123",
                        labels=["Person", "Employee"],
                        properties={"name": "Alice"},
                    ),
                    node_dest=Node(
                        id="456",
                        labels=["Person", "Manager"],
                        properties={"name": "Bob"},
                    ),
                ),
                " MERGE (a: Person: Employee {name : $0, `~id` : $1}) MERGE (b: Person: Manager {name : $2, `~id` : $3}) MERGE (a)-[r: REPORTS_TO]->(b)",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
            # Edge with numeric property values
            (
                "Edge with numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={"quantity": 5, "price": 29.99},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Product"], properties={"id": "1001"}
                    ),
                ),
                " MERGE (a: Person {name : $0, `~id` : $1}) MERGE (b: Product {id : $2, `~id` : $3}) MERGE (a)-[r: PURCHASED {quantity : $4, price : $5}]->(b)",
                {"0": "Alice", "1": "123", "2": "1001", "3": "456", "4": 5, "5": 29.99},
            ),
            # Edge with boolean property values
            (
                "Edge with boolean property values",
                Edge(
                    label="LIKED",
                    properties={"public": True, "notified": False},
                    node_src=Node(
                        id="123", labels=["User"], properties={"username": "alice123"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Content"], properties={"id": "content-456"}
                    ),
                ),
                " MERGE (a: User {username : $0, `~id` : $1}) MERGE (b: Content {id : $2, `~id` : $3}) MERGE (a)-[r: LIKED {public : $4, notified : $5}]->(b)",
                {
                    "0": "alice123",
                    "1": "123",
                    "2": "content-456",
                    "3": "456",
                    "4": True,
                    "5": False,
                },
            ),
            # Edge with different node type
            (
                "Edge with different node type",
                Edge(
                    label="WORKS_FOR",
                    properties={"role": "Engineer"},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Company"], properties={"name": "ACME Inc"}
                    ),
                ),
                " MERGE (a: Person {name : $0, `~id` : $1}) MERGE (b: Company {name : $2, `~id` : $3}) MERGE (a)-[r: WORKS_FOR {role : $4}]->(b)",
                {
                    "0": "Alice",
                    "1": "123",
                    "2": "ACME Inc",
                    "3": "456",
                    "4": "Engineer",
                },
            ),
        ]

        # Run each test case
        for desc, edge, expected_query, expected_params in test_cases:
            with self.subTest(description=desc):
                query_result = insert_edge(edge)
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)

    def test_insert_node_parameterized(self):
        """
        Parameterized test for inserting nodes with various configurations.
        """
        test_cases = [
            # (description, node, expected_query, expected_params)
            (
                "Single label with properties",
                Node(id="123", labels=["Person"], properties={"name": "Alice"}),
                " CREATE (: Person {name : $0, `~id` : $1})",
                {"0": "Alice", "1": "123"},
            ),
            (
                "Multiple labels with properties",
                Node(
                    id="456", labels=["Person", "Employee"], properties={"id": "12345"}
                ),
                " CREATE (: Person: Employee {id : $0, `~id` : $1})",
                {"0": "12345", "1": "456"},
            ),
            (
                "Single label without properties",
                Node(id="789", labels=["Person"]),
                " CREATE (: Person {`~id` : $0})",
                {"0": "789"},
            ),
            (
                "No labels with properties",
                Node(id="111", labels=[], properties={"name": "Alice"}),
                " CREATE ( {name : $0, `~id` : $1})",
                {"0": "Alice", "1": "111"},
            ),
            (
                "Numeric property values",
                Node(
                    id="222",
                    labels=["Product"],
                    properties={"price": 29.99, "stock": 100},
                ),
                " CREATE (: Product {price : $0, stock : $1, `~id` : $2})",
                {"0": 29.99, "1": 100, "2": "222"},
            ),
            (
                "Boolean property values",
                Node(
                    id="333",
                    labels=["User"],
                    properties={"active": True, "verified": False},
                ),
                " CREATE (: User {active : $0, verified : $1, `~id` : $2})",
                {"0": True, "1": False, "2": "333"},
            ),
        ]

        for desc, node, expected_query, expected_params in test_cases:
            with self.subTest(description=desc):
                query_result = insert_node(node)
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)

    def test_update_edge_parameterized(self):
        """
        Parameterized test for updating edges with various configurations.
        """
        test_cases = [
            # (description, edge, ref_name_src, ref_name_edge, ref_name_des, where_filters, properties_set, expected_query, expected_params)
            (
                "Single property update",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.since": "2020"},
                " MATCH (a: Person {`~id` : $0})-[r: FRIEND_WITH]->(b: Person {`~id` : $1}) WHERE a.name = $2 AND b.name = $3 SET r.since = $4",
                {"0": "123", "1": "456", "2": "Alice", "3": "Bob", "4": "2020"},
            ),
            (
                "Multiple properties update",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.since": "2020", "r.strength": "strong", "r.active": "true"},
                " MATCH (a: Person {`~id` : $0})-[r: FRIEND_WITH]->(b: Person {`~id` : $1}) WHERE a.name = $2 AND b.name = $3 SET r.since = $4, r.strength = $5, r.active = $6",
                {
                    "0": "123",
                    "1": "456",
                    "2": "Alice",
                    "3": "Bob",
                    "4": "2020",
                    "5": "strong",
                    "6": "true",
                },
            ),
            (
                "Different node types",
                Edge(
                    label="WORKS_FOR",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Company"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "ACME Inc"},
                {"r.role": "Engineer", "r.startDate": "2022-01-15"},
                " MATCH (a: Person {`~id` : $0})-[r: WORKS_FOR]->(b: Company {`~id` : $1}) WHERE a.name = $2 AND b.name = $3 SET r.role = $4, r.startDate = $5",
                {
                    "0": "123",
                    "1": "456",
                    "2": "Alice",
                    "3": "ACME Inc",
                    "4": "Engineer",
                    "5": "2022-01-15",
                },
            ),
            (
                "With node properties in MATCH",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2019"},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"age": "30"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"age": "28"}
                    ),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.strength": "very strong"},
                " MATCH (a: Person {age : $0, `~id` : $1})-[r: FRIEND_WITH]->(b: Person {age : $2, `~id` : $3}) WHERE a.name = $4 AND b.name = $5 SET r.strength = $6",
                {
                    "0": "30",
                    "1": "123",
                    "2": "28",
                    "3": "456",
                    "4": "Alice",
                    "5": "Bob",
                    "6": "very strong",
                },
            ),
            (
                "Multiple node labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(
                        id="123", labels=["Person", "Employee"], properties={}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person", "Manager"], properties={}
                    ),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.department": "Engineering"},
                " MATCH (a: Person: Employee {`~id` : $0})-[r: REPORTS_TO]->(b: Person: Manager {`~id` : $1}) WHERE a.name = $2 AND b.name = $3 SET r.department = $4",
                {"0": "123", "1": "456", "2": "Alice", "3": "Bob", "4": "Engineering"},
            ),
            (
                "Multiple WHERE conditions",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {
                    "a.name": "Alice",
                    "a.age": "30",
                    "b.name": "Bob",
                    "b.city": "Seattle",
                    "r.since": "2018",
                },
                {"r.strength": "best friends"},
                " MATCH (a: Person {`~id` : $0})-[r: FRIEND_WITH]->(b: Person {`~id` : $1}) WHERE a.name = $2 AND a.age = $3 AND b.name = $4 AND b.city = $5 AND r.since = $6 SET r.strength = $7",
                {
                    "0": "123",
                    "1": "456",
                    "2": "Alice",
                    "3": "30",
                    "4": "Bob",
                    "5": "Seattle",
                    "6": "2018",
                    "7": "best friends",
                },
            ),
            (
                "Numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Product"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.id": "1001"},
                {"r.quantity": 5, "r.price": 29.99},
                " MATCH (a: Person {`~id` : $0})-[r: PURCHASED]->(b: Product {`~id` : $1}) WHERE a.name = $2 AND b.id = $3 SET r.quantity = $4, r.price = $5",
                {"0": "123", "1": "456", "2": "Alice", "3": "1001", "4": 5, "5": 29.99},
            ),
            (
                "Boolean property values",
                Edge(
                    label="LIKED",
                    properties={},
                    node_src=Node(id="123", labels=["User"], properties={}),
                    node_dest=Node(id="456", labels=["Content"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.username": "alice123", "b.id": "content-456"},
                {"r.public": True, "r.notified": False},
                " MATCH (a: User {`~id` : $0})-[r: LIKED]->(b: Content {`~id` : $1}) WHERE a.username = $2 AND b.id = $3 SET r.public = $4, r.notified = $5",
                {
                    "0": "123",
                    "1": "456",
                    "2": "alice123",
                    "3": "content-456",
                    "4": True,
                    "5": False,
                },
            ),
            (
                "Null property value",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"], properties={}),
                    node_dest=Node(id="456", labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.endDate": None},
                " MATCH (a: Person {`~id` : $0})-[r: FRIEND_WITH]->(b: Person {`~id` : $1}) WHERE a.name = $2 AND b.name = $3 SET r.endDate = $4",
                {"0": "123", "1": "456", "2": "Alice", "3": "Bob", "4": None},
            ),
            (
                "Different reference names",
                Edge(
                    label="ACTED_IN",
                    properties={},
                    node_src=Node(id="123", labels=["Movie"], properties={}),
                    node_dest=Node(id="456", labels=["Person"], properties={}),
                ),
                "movie",
                "role",
                "actor",
                {"movie.title": "The Matrix", "actor.name": "Keanu Reeves"},
                {"role.character": "Neo"},
                " MATCH (movie: Movie {`~id` : $0})-[role: ACTED_IN]->(actor: Person {`~id` : $1}) WHERE movie.title = $2 AND actor.name = $3 SET role.character = $4",
                {
                    "0": "123",
                    "1": "456",
                    "2": "The Matrix",
                    "3": "Keanu Reeves",
                    "4": "Neo",
                },
            ),
        ]

        for (
            desc,
            edge,
            ref_name_src,
            ref_name_edge,
            ref_name_des,
            where_filters,
            properties_set,
            expected_query,
            expected_params,
        ) in test_cases:
            with self.subTest(description=desc):
                query_result = update_edge(
                    ref_name_src,
                    ref_name_edge,
                    edge,
                    ref_name_des,
                    where_filters,
                    properties_set,
                )
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)

    def test_update_node_parameterized(self):
        """
        Parameterized test for updating nodes with various configurations.
        """
        test_cases = [
            # (description, match_labels, ref_name, where_filters, properties_set, expected_query, expected_params)
            (
                "Single property update",
                "Person",
                "a",
                ["Alice"],
                {"a.age": "25"},
                " MATCH (a: Person) WHERE id(a)=$0 SET a.age = $1",
                {"0": "Alice", "1": "25"},
            ),
            (
                "Multiple properties update",
                "Person",
                "a",
                ["12345"],
                {"a.age": "30", "a.status": "Active", "a.updated": "true"},
                " MATCH (a: Person) WHERE id(a)=$0 SET a.age = $1, a.status = $2, a.updated = $3",
                {"0": "12345", "1": "30", "2": "Active", "3": "true"},
            ),
            (
                "Multiple labels",
                ["Person", "Employee"],
                "a",
                ["alice@example.com"],
                {"a.department": "Engineering"},
                " MATCH (a: Person: Employee) WHERE id(a)=$0 SET a.department = $1",
                {"0": "alice@example.com", "1": "Engineering"},
            ),
            (
                "Multiple node ids",
                "Person",
                "a",
                ["Alice", "Bob"],
                {"a.lastLogin": "2023-04-01"},
                " MATCH (a: Person) WHERE id(a)=$0 OR id(a)=$1 SET a.lastLogin = $2",
                {"0": "Alice", "1": "Bob", "2": "2023-04-01"},
            ),
            (
                "Numeric property values",
                "Product",
                "p",
                ["1001"],
                {"p.price": 29.99, "p.stock": 100},
                " MATCH (p: Product) WHERE id(p)=$0 SET p.price = $1, p.stock = $2",
                {"0": "1001", "1": 29.99, "2": 100},
            ),
            (
                "Boolean property values",
                "User",
                "u",
                ["alice123"],
                {"u.verified": True, "u.active": False},
                " MATCH (u: User) WHERE id(u)=$0 SET u.verified = $1, u.active = $2",
                {"0": "alice123", "1": True, "2": False},
            ),
            (
                "Null property value",
                "Person",
                "a",
                ["Alice"],
                {"a.previousJob": None},
                " MATCH (a: Person) WHERE id(a)=$0 SET a.previousJob = $1",
                {"0": "Alice", "1": None},
            ),
            (
                "Different reference name",
                "Movie",
                "movie",
                ["The Matrix"],
                {"movie.rating": 4.8},
                " MATCH (movie: Movie) WHERE id(movie)=$0 SET movie.rating = $1",
                {"0": "The Matrix", "1": 4.8},
            ),
        ]

        for (
            desc,
            match_labels,
            ref_name,
            node_list,
            properties_set,
            expected_query,
            expected_params,
        ) in test_cases:
            with self.subTest(description=desc):
                query_result = update_node(
                    match_labels, ref_name, node_list, properties_set
                )
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)

    def test_delete_node_parameterized(self):
        """
        Parameterized test for deleting nodes with various configurations.
        """
        test_cases = [
            # (description, node, expected_query, expected_params)
            (
                "Single label with properties",
                Node(id="123", labels=["Person"], properties={"name": "Alice"}),
                " MATCH (n: Person {name : $0, `~id` : $1}) DELETE n",
                {"0": "Alice", "1": "123"},
            ),
            (
                "Multiple labels with properties",
                Node(
                    id="456", labels=["Person", "Employee"], properties={"id": "12345"}
                ),
                " MATCH (n: Person: Employee {id : $0, `~id` : $1}) DELETE n",
                {"0": "12345", "1": "456"},
            ),
            (
                "Single label without properties",
                Node(id="789", labels=["Person"]),
                " MATCH (n: Person {`~id` : $0}) DELETE n",
                {"0": "789"},
            ),
            (
                "No labels with properties",
                Node(id="111", labels=[], properties={"name": "Alice"}),
                " MATCH (n {name : $0, `~id` : $1}) DELETE n",
                {"0": "Alice", "1": "111"},
            ),
            (
                "Numeric property values",
                Node(
                    id="222",
                    labels=["Product"],
                    properties={"price": 29.99, "stock": 100},
                ),
                " MATCH (n: Product {price : $0, stock : $1, `~id` : $2}) DELETE n",
                {"0": 29.99, "1": 100, "2": "222"},
            ),
            (
                "Boolean property values",
                Node(
                    id="333",
                    labels=["User"],
                    properties={"active": True, "verified": False},
                ),
                " MATCH (n: User {active : $0, verified : $1, `~id` : $2}) DELETE n",
                {"0": True, "1": False, "2": "333"},
            ),
            (
                "Null property value",
                Node(id="444", labels=["Person"], properties={"previousJob": None}),
                " MATCH (n: Person {previousJob : $0, `~id` : $1}) DELETE n",
                {"0": None, "1": "444"},
            ),
        ]

        for desc, node, expected_query, expected_params in test_cases:
            with self.subTest(description=desc):
                query_result = delete_node(node)
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)

    def test_delete_edge_parameterized(self):
        """
        Parameterized test for deleting edges with various configurations using Edge class with embedded nodes.
        """
        test_cases = [
            # (description, edge, expected_query, expected_params)
            (
                "Basic edge",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"name": "Bob"}
                    ),
                ),
                " MATCH (a: Person {name : $0, `~id` : $1})-[r: FRIEND_WITH]->(b: Person {name : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
            (
                "Edge with properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2020"},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"name": "Bob"}
                    ),
                ),
                " MATCH (a: Person {name : $0, `~id` : $1})-[r: FRIEND_WITH]->(b: Person {name : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
            (
                "Edge between nodes with no properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(id="123", labels=["Person"]),
                    node_dest=Node(id="456", labels=["Person"]),
                ),
                " MATCH (a: Person {`~id` : $0})-[r: FRIEND_WITH]->(b: Person {`~id` : $1}) DELETE r",
                {"0": "123", "1": "456"},
            ),
            (
                "Edge between different node types",
                Edge(
                    label="WORKS_FOR",
                    properties={"role": "Engineer"},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Company"], properties={"name": "ACME Inc"}
                    ),
                ),
                " MATCH (a: Person {name : $0, `~id` : $1})-[r: WORKS_FOR]->(b: Company {name : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "ACME Inc", "3": "456"},
            ),
            (
                "Edge between nodes with multiple labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(
                        id="123",
                        labels=["Person", "Employee"],
                        properties={"name": "Alice"},
                    ),
                    node_dest=Node(
                        id="456",
                        labels=["Person", "Manager"],
                        properties={"name": "Bob"},
                    ),
                ),
                " MATCH (a: Person: Employee {name : $0, `~id` : $1})-[r: REPORTS_TO]->(b: Person: Manager {name : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
            (
                "Edge with numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={"quantity": 5, "price": 29.99},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Product"], properties={"id": "1001"}
                    ),
                ),
                " MATCH (a: Person {name : $0, `~id` : $1})-[r: PURCHASED]->(b: Product {id : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "1001", "3": "456"},
            ),
            (
                "Edge with boolean property values",
                Edge(
                    label="LIKED",
                    properties={"public": True, "notified": False},
                    node_src=Node(
                        id="123", labels=["User"], properties={"username": "alice123"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Content"], properties={"id": "content-456"}
                    ),
                ),
                " MATCH (a: User {username : $0, `~id` : $1})-[r: LIKED]->(b: Content {id : $2, `~id` : $3}) DELETE r",
                {"0": "alice123", "1": "123", "2": "content-456", "3": "456"},
            ),
            (
                "Edge with null property value",
                Edge(
                    label="FRIEND_WITH",
                    properties={"endDate": None},
                    node_src=Node(
                        id="123", labels=["Person"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        id="456", labels=["Person"], properties={"name": "Bob"}
                    ),
                ),
                " MATCH (a: Person {name : $0, `~id` : $1})-[r: FRIEND_WITH]->(b: Person {name : $2, `~id` : $3}) DELETE r",
                {"0": "Alice", "1": "123", "2": "Bob", "3": "456"},
            ),
        ]

        for desc, edge, expected_query, expected_params in test_cases:
            with self.subTest(description=desc):
                query_result = delete_edge(edge)
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)


if __name__ == "__main__":
    unittest.main()


class TestOpencypherBuilderAdditional(unittest.TestCase):
    """Additional test cases for uncovered functions."""

    def test_insert_nodes(self):
        """Test insert_nodes batch operation."""
        from nx_neptune.clients.opencypher_builder import insert_nodes

        nodes = [
            Node(id="1", labels=["Person"], properties={"name": "Alice"}),
            Node(id="2", labels=["Person"], properties={"name": "Bob"}),
        ]
        queries, params = insert_nodes(nodes)
        self.assertEqual(len(queries), 1)
        self.assertEqual(len(params), 1)
        self.assertIn("nodes", params[0])

    def test_insert_edges(self):
        """Test insert_edges batch operation."""
        from nx_neptune.clients.opencypher_builder import insert_edges

        edges = [
            Edge(
                label="KNOWS",
                properties={},
                node_src=Node(id="1", labels=["Person"]),
                node_dest=Node(id="2", labels=["Person"]),
            ),
            Edge(
                label="KNOWS",
                properties={},
                node_src=Node(id="2", labels=["Person"]),
                node_dest=Node(id="3", labels=["Person"]),
            ),
        ]
        queries, params = insert_edges(edges)
        self.assertEqual(len(queries), 1)
        self.assertEqual(len(params), 1)

    def test_pagerank_mutation_query(self):
        """Test pagerank_mutation_query."""
        from nx_neptune.clients.opencypher_builder import pagerank_mutation_query

        query, params = pagerank_mutation_query({"writeProperty": "rank"})
        self.assertIn("pageRank", query)
        self.assertIn("mutate", query)
        self.assertEqual(params, {})

    def test_label_propagation_query(self):
        """Test label_propagation_query."""
        from nx_neptune.clients.opencypher_builder import label_propagation_query

        query, params = label_propagation_query()
        self.assertIn("labelPropagation", query)
        self.assertIn("YIELD", query)
        self.assertEqual(params, {})

    def test_label_propagation_mutation_query(self):
        """Test label_propagation_mutation_query."""
        from nx_neptune.clients.opencypher_builder import (
            label_propagation_mutation_query,
        )

        query, params = label_propagation_mutation_query({"writeProperty": "community"})
        self.assertIn("labelPropagation", query)
        self.assertIn("mutate", query)
        self.assertEqual(params, {})

    def test_louvain_query(self):
        """Test louvain_query."""
        from nx_neptune.clients.opencypher_builder import louvain_query

        query, params = louvain_query()
        self.assertIn("louvain", query)
        self.assertIn("YIELD", query)
        self.assertEqual(params, {})

    def test_louvain_mutation_query(self):
        """Test louvain_mutation_query."""
        from nx_neptune.clients.opencypher_builder import louvain_mutation_query

        query, params = louvain_mutation_query({"writeProperty": "community"})
        self.assertIn("louvain", query)
        self.assertIn("mutate", query)
        self.assertEqual(params, {})

    def test_closeness_centrality_query(self):
        """Test closeness_centrality_query."""
        from nx_neptune.clients.opencypher_builder import closeness_centrality_query

        query, params = closeness_centrality_query()
        self.assertIn("closenessCentrality", query)
        self.assertIn("YIELD", query)
        self.assertEqual(params, {})

    def test_closeness_centrality_mutation_query(self):
        """Test closeness_centrality_mutation_query."""
        from nx_neptune.clients.opencypher_builder import (
            closeness_centrality_mutation_query,
        )

        query, params = closeness_centrality_mutation_query(
            {"writeProperty": "closeness"}
        )
        self.assertIn("closenessCentrality", query)
        self.assertIn("mutate", query)
        self.assertEqual(params, {})

    def test_degree_centrality_query(self):
        """Test degree_centrality_query."""
        from nx_neptune.clients.opencypher_builder import degree_centrality_query

        query, params = degree_centrality_query()
        self.assertIn("degree", query)
        self.assertIn("YIELD", query)
        self.assertEqual(params, {})

    def test_degree_centrality_mutation_query(self):
        """Test degree_centrality_mutation_query."""
        from nx_neptune.clients.opencypher_builder import (
            degree_centrality_mutation_query,
        )

        query, params = degree_centrality_mutation_query({"writeProperty": "degree"})
        self.assertIn("degree", query)
        self.assertIn("mutate", query)
        self.assertEqual(params, {})

    def test_descendants_at_distance_query(self):
        """Test descendants_at_distance_query."""
        from nx_neptune.clients.opencypher_builder import descendants_at_distance_query

        query, params = descendants_at_distance_query(
            "n", {"id(n)": "Alice"}, {"maxDepth": 2}
        )
        self.assertIn("bfs", query)
        self.assertIn("level", query)
        self.assertEqual(params["0"], "Alice")

    def test_bfs_layers_query(self):
        """Test bfs_layers_query."""
        from nx_neptune.clients.opencypher_builder import bfs_layers_query

        query, params = bfs_layers_query("n", {"id(n)": "Alice"}, {"maxDepth": 3})
        self.assertIn("bfs", query)
        self.assertIn("level", query)
        self.assertEqual(params["0"], "Alice")
