import json
import logging

import boto3


class NeptuneAnalyticsClient:
    """
    The NeptuneAnalyticsClient is the core component of this plugin,
    responsible for interacting with the AWS Neptune Analytics service.
    It facilitates various actions, including CRUD operations on graphs
    and the execution of supported algorithms.
    TODO: To add test on CRUD operation,
    when hard-coded openCypher syntax being replaced by Python lib.
    """

    def __init__(
        self,
        graphId: str,
        logger=None,
        client=None,
    ):
        """
        Constructs a NeptuneAnalyticsClient object for AWS service interaction,
        with optional custom logger and boto client.
        TODO: To have a create_client || connect_to_na_instance
        for networkX backend integration.
        """
        self.graphId = graphId
        self.logger = logger or logging.getLogger(__name__)
        self.client = client or boto3.client("neptune-graph")

    def create_na_instance(self):
        """
        TODO: Connect to Boto3 and create a Neptune Analytic instance,
        then return the graph ID.
        """
        return "exampleGraphID"

    def add_node(self, nodeContent: str):
        """
        Method to add an additional node into the existing graph,
        which this client hold references to.
        """
        return self.__execute_insert_query(nodeContent)

    def update_nodes(
        self, matchClause: str, whereClause: str, valueClause: str
    ):
        """
        Perform an update on node's property for nodes with matching condition,
        which presented within the graph.
        """
        return self.__execute_update_query(
            matchClause, whereClause, valueClause
        )

    def delete_nodes(self, matchClause: str, deleteClause: str):
        """
        To delete note from the graph with provided condition.

        Args:
            matchClause (str): The OpenCypher MATCH clause.
            deleteClause (str): The OpenCypher DELETE clause.

        Returns:
            _type_: Result from boto client in string format.
        """

        return self.__execute_delete_query(matchClause, deleteClause)

    def clear_graph(self):
        """
        To perform truncation to clear all nodes and edges on the graph.
        """
        return self.__execute_generic_query("MATCH (n) DETACH DELETE n")

    def add_edge(self, createClause: str, matchClause: str):
        """
        Perform an insertion to add a relationship between two nodes.

        Args:
            createClause (str): The OpenCypher CREATE clause.
            matchClause (str): The OpenCypher MATCH clause.

        Returns:
            _type_: Result from boto client in string format.
        """
        return self.__execute_insert_query(createClause, matchClause)

    def update_edges(
        self, matchClause: str, whereClause: str, valueClause: str
    ):
        """
        Update existing edge's properties with provided condition and values.

        Args:
            matchClause (str): The OpenCypher MATCH clause.
            whereClause (str): The OpenCypher WHERE clause.
            valueClause (str): The OpenCypher VALUE clause.

        Returns:
            _type_: Result from boto client in string format.
        """
        return self.__execute_update_query(
            matchClause, whereClause, valueClause
        )

    def delete_edges(self, matchClause: str, valueClause: str):
        """
        Delete one or more edges from NA graph,
        with provided conditions and values.

        Args:
            matchClause (str): The OpenCypher MATCH clause.
            valueClause (str): The OpenCypher VALUE clause.

        Returns:
            _type_: N/A
        """
        return self.__execute_delete_query(matchClause, valueClause)

    def get_all_nodes(self):
        """
        Helper method to return all nodes from the graph,
        in Python List object format.

        Returns:
            _type_: Nodes in JSON format.
        """
        return self.__execute_generic_query("MATCH (a) RETURN a")

    def get_all_edges(self):
        """
        Helper method to return all edges from the graph,
        in Python List object format.

        Returns:
            _type_: Edges in JSON format.
        """
        return self.__execute_generic_query("MATCH (a)-[r]->(b) RETURN r")

    def __execute_generic_query(self, queryString: str):
        """
        Wrapper method to execute incoming openCypher query,
        and return the result from Boto client.

        Args:
            queryString (str): OpenCypher query in string format.

        Returns:
            _type_: Result from Boto client.
        """
        self.logger.info(
            "Executing generic query ["
            + queryString
            + "] on graph ["
            + self.graphId
            + "]"
        )
        response = self.client.execute_query(
            graphIdentifier=self.graphId,
            queryString=queryString,
            language="OPEN_CYPHER",
        )

        return json.loads(response["payload"].read())["results"]

    def __execute_insert_query(self, createClause: str, matchClause: str = ""):
        """
        To first composite the insert OpenCypher query,
        then format it to Boto for execution.

        Args:
            createClause (str): Context of the insertion
            matchClause (str, optional): Optional arg to specify the condition,
            when inserting an edge.

        Returns:
            _type_: API result from Boto client.
        """
        if matchClause:
            queryString = (
                "MATCH " + matchClause + " CREATE (" + createClause + ") "
            )
        else:
            queryString = "CREATE (" + createClause + ") "

        return self.__execute_generic_query(queryString)

    def __execute_update_query(
        self, matchClause: str, whereClause: str, setClause: str
    ):
        """
        Composite an OpenCypher update query,
        then forward it to Boto client for execution

        Args:
            matchClause (str): The OpenCypher MATCH clause.
            whereClause (str): The OpenCypher WHERE clause.
            setClause (str): The OpenCypher VALUE clause.

        Returns:
            _type_: API result from Boto client. s
        """
        queryString = (
            "MATCH "
            + matchClause
            + " "
            + "WHERE "
            + whereClause
            + " "
            + "SET "
            + setClause
            + " "
        )
        return self.__execute_generic_query(queryString)

    def __execute_delete_query(self, matchClause: str, deleteClause: str):
        """
        Composite an OpenCypher delete query,
        then forward it to Boto client for execution.

        Args:
            matchClause (str): The OpenCypher MATCH clause.
            deleteClause (str): The OpenCypher DELETE clause.

        Returns:
            _type_: API result from Boto client. s
        """
        queryString = (
            "MATCH (" + matchClause + ") " + "DELETE " + deleteClause + " "
        )
        return self.__execute_generic_query(queryString)

    def execute_algo_bfs(self, variableClause: str, whereClause: str):
        """
        Composite an OpenCypher `CALL` statement for BFS algorithm run,
        then execute over the remote NA cluster.

        Args:
            variableClause (str): The variable which will be used
            under MATCH and CALL clause.
            whereClause (str): The condition statement to be used
            under WHERE clause for filtering.

        Returns:
            _type_: The execution result of BFS algorithm.
        """
        queryString = (
            "MATCH ("
            + variableClause
            + ") where "
            + whereClause
            + " CALL neptune.algo.bfs("
            + variableClause
            + ") YIELD node RETURN node"
        )

        return self.__execute_generic_query(queryString)
