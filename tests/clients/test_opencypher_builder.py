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
            " MATCH (n) WHERE id(n) = $0 CALL neptune.algo.bfs.parents(n, {maxDepth:1, traversalDirection:inbound}) YIELD parent AS parent, node AS node RETURN parent, node",
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
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Person"], properties={"name": "Bob"}),
                ),
                " MERGE (a: Person {name : $0}) MERGE (b: Person {name : $1}) MERGE (a)-[r: FRIEND_WITH]->(b)",
                {"0": "Alice", "1": "Bob"},
            ),
            # Edge without properties
            (
                "Edge without properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(labels=["Person"]),
                    node_dest=Node(labels=["Person"]),
                ),
                " MERGE (a: Person) MERGE (b: Person) MERGE (a)-[r: FRIEND_WITH]->(b)",
                {},
            ),
            # Edge with properties
            (
                "Edge with properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2020"},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Person"], properties={"name": "Bob"}),
                ),
                " MERGE (a: Person {name : $0}) MERGE (b: Person {name : $1}) MERGE (a)-[r: FRIEND_WITH {since : $2}]->(b)",
                {"0": "Alice", "1": "Bob", "2": "2020"},
            ),
            # Edge between nodes with multiple labels
            (
                "Edge between nodes with multiple labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(
                        labels=["Person", "Employee"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        labels=["Person", "Manager"], properties={"name": "Bob"}
                    ),
                ),
                " MERGE (a: Person: Employee {name : $0}) MERGE (b: Person: Manager {name : $1}) MERGE (a)-[r: REPORTS_TO]->(b)",
                {"0": "Alice", "1": "Bob"},
            ),
            # Edge with numeric property values
            (
                "Edge with numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={"quantity": 5, "price": 29.99},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Product"], properties={"id": "1001"}),
                ),
                " MERGE (a: Person {name : $0}) MERGE (b: Product {id : $1}) MERGE (a)-[r: PURCHASED {quantity : $2, price : $3}]->(b)",
                {"0": "Alice", "1": "1001", "2": 5, "3": 29.99},
            ),
            # Edge with boolean property values
            (
                "Edge with boolean property values",
                Edge(
                    label="LIKED",
                    properties={"public": True, "notified": False},
                    node_src=Node(labels=["User"], properties={"username": "alice123"}),
                    node_dest=Node(
                        labels=["Content"], properties={"id": "content-456"}
                    ),
                ),
                " MERGE (a: User {username : $0}) MERGE (b: Content {id : $1}) MERGE (a)-[r: LIKED {public : $2, notified : $3}]->(b)",
                {"0": "alice123", "1": "content-456", "2": True, "3": False},
            ),
            # Edge with different node type
            (
                "Edge with different node type",
                Edge(
                    label="WORKS_FOR",
                    properties={"role": "Engineer"},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Company"], properties={"name": "ACME Inc"}),
                ),
                " MERGE (a: Person {name : $0}) MERGE (b: Company {name : $1}) MERGE (a)-[r: WORKS_FOR {role : $2}]->(b)",
                {"0": "Alice", "1": "ACME Inc", "2": "Engineer"},
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
                Node(labels=["Person"], properties={"name": "Alice"}),
                " CREATE (: Person {name : $0})",
                {"0": "Alice"},
            ),
            (
                "Multiple labels with properties",
                Node(labels=["Person", "Employee"], properties={"id": "12345"}),
                " CREATE (: Person: Employee {id : $0})",
                {"0": "12345"},
            ),
            (
                "Single label without properties",
                Node(labels=["Person"]),
                " CREATE (: Person)",
                {},
            ),
            (
                "No labels with properties",
                Node(labels=[], properties={"name": "Alice"}),
                " CREATE ( {name : $0})",
                {"0": "Alice"},
            ),
            (
                "Numeric property values",
                Node(labels=["Product"], properties={"price": 29.99, "stock": 100}),
                " CREATE (: Product {price : $0, stock : $1})",
                {"0": 29.99, "1": 100},
            ),
            (
                "Boolean property values",
                Node(labels=["User"], properties={"active": True, "verified": False}),
                " CREATE (: User {active : $0, verified : $1})",
                {"0": True, "1": False},
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
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.since": "2020"},
                " MATCH (a: Person)-[r: FRIEND_WITH]->(b: Person) WHERE a.name = $0 AND b.name = $1 SET r.since = $2",
                {"0": "Alice", "1": "Bob", "2": "2020"},
            ),
            (
                "Multiple properties update",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.since": "2020", "r.strength": "strong", "r.active": "true"},
                " MATCH (a: Person)-[r: FRIEND_WITH]->(b: Person) WHERE a.name = $0 AND b.name = $1 SET r.since = $2, r.strength = $3, r.active = $4",
                {"0": "Alice", "1": "Bob", "2": "2020", "3": "strong", "4": "true"},
            ),
            (
                "Different node types",
                Edge(
                    label="WORKS_FOR",
                    properties={},
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Company"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "ACME Inc"},
                {"r.role": "Engineer", "r.startDate": "2022-01-15"},
                " MATCH (a: Person)-[r: WORKS_FOR]->(b: Company) WHERE a.name = $0 AND b.name = $1 SET r.role = $2, r.startDate = $3",
                {"0": "Alice", "1": "ACME Inc", "2": "Engineer", "3": "2022-01-15"},
            ),
            (
                "With node properties in MATCH",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2019"},
                    node_src=Node(labels=["Person"], properties={"age": "30"}),
                    node_dest=Node(labels=["Person"], properties={"age": "28"}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.strength": "very strong"},
                " MATCH (a: Person {age : $0})-[r: FRIEND_WITH]->(b: Person {age : $1}) WHERE a.name = $2 AND b.name = $3 SET r.strength = $4",
                {"0": "30", "1": "28", "2": "Alice", "3": "Bob", "4": "very strong"},
            ),
            (
                "Multiple node labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(labels=["Person", "Employee"], properties={}),
                    node_dest=Node(labels=["Person", "Manager"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.department": "Engineering"},
                " MATCH (a: Person: Employee)-[r: REPORTS_TO]->(b: Person: Manager) WHERE a.name = $0 AND b.name = $1 SET r.department = $2",
                {"0": "Alice", "1": "Bob", "2": "Engineering"},
            ),
            (
                "Multiple WHERE conditions",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Person"], properties={}),
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
                " MATCH (a: Person)-[r: FRIEND_WITH]->(b: Person) WHERE a.name = $0 AND a.age = $1 AND b.name = $2 AND b.city = $3 AND r.since = $4 SET r.strength = $5",
                {
                    "0": "Alice",
                    "1": "30",
                    "2": "Bob",
                    "3": "Seattle",
                    "4": "2018",
                    "5": "best friends",
                },
            ),
            (
                "Numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={},
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Product"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.id": "1001"},
                {"r.quantity": 5, "r.price": 29.99},
                " MATCH (a: Person)-[r: PURCHASED]->(b: Product) WHERE a.name = $0 AND b.id = $1 SET r.quantity = $2, r.price = $3",
                {"0": "Alice", "1": "1001", "2": 5, "3": 29.99},
            ),
            (
                "Boolean property values",
                Edge(
                    label="LIKED",
                    properties={},
                    node_src=Node(labels=["User"], properties={}),
                    node_dest=Node(labels=["Content"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.username": "alice123", "b.id": "content-456"},
                {"r.public": True, "r.notified": False},
                " MATCH (a: User)-[r: LIKED]->(b: Content) WHERE a.username = $0 AND b.id = $1 SET r.public = $2, r.notified = $3",
                {"0": "alice123", "1": "content-456", "2": True, "3": False},
            ),
            (
                "Null property value",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(labels=["Person"], properties={}),
                    node_dest=Node(labels=["Person"], properties={}),
                ),
                "a",
                "r",
                "b",
                {"a.name": "Alice", "b.name": "Bob"},
                {"r.endDate": None},
                " MATCH (a: Person)-[r: FRIEND_WITH]->(b: Person) WHERE a.name = $0 AND b.name = $1 SET r.endDate = $2",
                {"0": "Alice", "1": "Bob", "2": None},
            ),
            (
                "Different reference names",
                Edge(
                    label="ACTED_IN",
                    properties={},
                    node_src=Node(labels=["Movie"], properties={}),
                    node_dest=Node(labels=["Person"], properties={}),
                ),
                "movie",
                "role",
                "actor",
                {"movie.title": "The Matrix", "actor.name": "Keanu Reeves"},
                {"role.character": "Neo"},
                " MATCH (movie: Movie)-[role: ACTED_IN]->(actor: Person) WHERE movie.title = $0 AND actor.name = $1 SET role.character = $2",
                {"0": "The Matrix", "1": "Keanu Reeves", "2": "Neo"},
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
                {"a.name": "Alice"},
                {"a.age": "25"},
                " MATCH (a: Person) WHERE a.name = $0 SET a.age = $1",
                {"0": "Alice", "1": "25"},
            ),
            (
                "Multiple properties update",
                "Person",
                "a",
                {"a.id": "12345"},
                {"a.age": "30", "a.status": "Active", "a.updated": "true"},
                " MATCH (a: Person) WHERE a.id = $0 SET a.age = $1, a.status = $2, a.updated = $3",
                {"0": "12345", "1": "30", "2": "Active", "3": "true"},
            ),
            (
                "Multiple labels",
                ["Person", "Employee"],
                "a",
                {"a.email": "alice@example.com"},
                {"a.department": "Engineering"},
                " MATCH (a: Person: Employee) WHERE a.email = $0 SET a.department = $1",
                {"0": "alice@example.com", "1": "Engineering"},
            ),
            (
                "Multiple WHERE conditions",
                "Person",
                "a",
                {"a.name": "Alice", "a.age": "25", "a.active": "true"},
                {"a.lastLogin": "2023-04-01"},
                " MATCH (a: Person) WHERE a.name = $0 AND a.age = $1 AND a.active = $2 SET a.lastLogin = $3",
                {"0": "Alice", "1": "25", "2": "true", "3": "2023-04-01"},
            ),
            (
                "Numeric property values",
                "Product",
                "p",
                {"p.id": "1001"},
                {"p.price": 29.99, "p.stock": 100},
                " MATCH (p: Product) WHERE p.id = $0 SET p.price = $1, p.stock = $2",
                {"0": "1001", "1": 29.99, "2": 100},
            ),
            (
                "Boolean property values",
                "User",
                "u",
                {"u.username": "alice123"},
                {"u.verified": True, "u.active": False},
                " MATCH (u: User) WHERE u.username = $0 SET u.verified = $1, u.active = $2",
                {"0": "alice123", "1": True, "2": False},
            ),
            (
                "Null property value",
                "Person",
                "a",
                {"a.name": "Alice"},
                {"a.previousJob": None},
                " MATCH (a: Person) WHERE a.name = $0 SET a.previousJob = $1",
                {"0": "Alice", "1": None},
            ),
            (
                "Different reference name",
                "Movie",
                "movie",
                {"movie.title": "The Matrix"},
                {"movie.rating": 4.8},
                " MATCH (movie: Movie) WHERE movie.title = $0 SET movie.rating = $1",
                {"0": "The Matrix", "1": 4.8},
            ),
        ]

        for (
            desc,
            match_labels,
            ref_name,
            where_filters,
            properties_set,
            expected_query,
            expected_params,
        ) in test_cases:
            with self.subTest(description=desc):
                query_result = update_node(
                    match_labels, ref_name, where_filters, properties_set
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
                Node(labels=["Person"], properties={"name": "Alice"}),
                " MATCH (n: Person {name : $0}) DELETE n",
                {"0": "Alice"},
            ),
            (
                "Multiple labels with properties",
                Node(labels=["Person", "Employee"], properties={"id": "12345"}),
                " MATCH (n: Person: Employee {id : $0}) DELETE n",
                {"0": "12345"},
            ),
            (
                "Single label without properties",
                Node(labels=["Person"]),
                " MATCH (n: Person) DELETE n",
                {},
            ),
            (
                "No labels with properties",
                Node(labels=[], properties={"name": "Alice"}),
                " MATCH (n {name : $0}) DELETE n",
                {"0": "Alice"},
            ),
            (
                "Numeric property values",
                Node(labels=["Product"], properties={"price": 29.99, "stock": 100}),
                " MATCH (n: Product {price : $0, stock : $1}) DELETE n",
                {"0": 29.99, "1": 100},
            ),
            (
                "Boolean property values",
                Node(labels=["User"], properties={"active": True, "verified": False}),
                " MATCH (n: User {active : $0, verified : $1}) DELETE n",
                {"0": True, "1": False},
            ),
            (
                "Null property value",
                Node(labels=["Person"], properties={"previousJob": None}),
                " MATCH (n: Person {previousJob : $0}) DELETE n",
                {"0": None},
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
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Person"], properties={"name": "Bob"}),
                ),
                " MATCH (a: Person {name : $0})-[r: FRIEND_WITH]->(b: Person {name : $1}) DELETE r",
                {"0": "Alice", "1": "Bob"},
            ),
            (
                "Edge with properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={"since": "2020"},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Person"], properties={"name": "Bob"}),
                ),
                " MATCH (a: Person {name : $0})-[r: FRIEND_WITH]->(b: Person {name : $1}) DELETE r",
                {"0": "Alice", "1": "Bob"},
            ),
            (
                "Edge between nodes with no properties",
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=Node(labels=["Person"]),
                    node_dest=Node(labels=["Person"]),
                ),
                " MATCH (a: Person)-[r: FRIEND_WITH]->(b: Person) DELETE r",
                {},
            ),
            (
                "Edge between different node types",
                Edge(
                    label="WORKS_FOR",
                    properties={"role": "Engineer"},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Company"], properties={"name": "ACME Inc"}),
                ),
                " MATCH (a: Person {name : $0})-[r: WORKS_FOR]->(b: Company {name : $1}) DELETE r",
                {"0": "Alice", "1": "ACME Inc"},
            ),
            (
                "Edge between nodes with multiple labels",
                Edge(
                    label="REPORTS_TO",
                    properties={},
                    node_src=Node(
                        labels=["Person", "Employee"], properties={"name": "Alice"}
                    ),
                    node_dest=Node(
                        labels=["Person", "Manager"], properties={"name": "Bob"}
                    ),
                ),
                " MATCH (a: Person: Employee {name : $0})-[r: REPORTS_TO]->(b: Person: Manager {name : $1}) DELETE r",
                {"0": "Alice", "1": "Bob"},
            ),
            (
                "Edge with numeric property values",
                Edge(
                    label="PURCHASED",
                    properties={"quantity": 5, "price": 29.99},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Product"], properties={"id": "1001"}),
                ),
                " MATCH (a: Person {name : $0})-[r: PURCHASED]->(b: Product {id : $1}) DELETE r",
                {"0": "Alice", "1": "1001"},
            ),
            (
                "Edge with boolean property values",
                Edge(
                    label="LIKED",
                    properties={"public": True, "notified": False},
                    node_src=Node(labels=["User"], properties={"username": "alice123"}),
                    node_dest=Node(
                        labels=["Content"], properties={"id": "content-456"}
                    ),
                ),
                " MATCH (a: User {username : $0})-[r: LIKED]->(b: Content {id : $1}) DELETE r",
                {"0": "alice123", "1": "content-456"},
            ),
            (
                "Edge with null property value",
                Edge(
                    label="FRIEND_WITH",
                    properties={"endDate": None},
                    node_src=Node(labels=["Person"], properties={"name": "Alice"}),
                    node_dest=Node(labels=["Person"], properties={"name": "Bob"}),
                ),
                " MATCH (a: Person {name : $0})-[r: FRIEND_WITH]->(b: Person {name : $1}) DELETE r",
                {"0": "Alice", "1": "Bob"},
            ),
        ]

        for desc, edge, expected_query, expected_params in test_cases:
            with self.subTest(description=desc):
                query_result = delete_edge(edge)
                self.assertEqual(query_result[0], expected_query)
                self.assertEqual(query_result[1], expected_params)


if __name__ == "__main__":
    unittest.main()
