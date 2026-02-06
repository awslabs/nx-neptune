# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
from enum import Enum
from typing import Callable, Optional, Union

import boto3
import networkx as nx
from botocore.config import Config
from botocore.exceptions import ClientError

from . import NeptuneGraph, instance_management
from .clients import IamClient, NeptuneAnalyticsClient
from .clients.neptune_constants import (
    APP_ID_NX,
    SERVICE_ATHENA,
    SERVICE_IAM,
    SERVICE_NA,
    SERVICE_S3,
    SERVICE_STS,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CleanupTask",
    "SessionManager",
]


class CleanupTask(Enum):
    """Enum for cleanup task types."""

    NONE = None
    DESTROY = "destroy"
    RESET = "reset"
    STOP = "stop"


class SessionGraph:
    """Represents a Neptune Analytics graph database instance.

    This class wraps a Neptune Analytics graph instance, providing access to its
    metadata and methods for executing queries against the graph database.

    Attributes:
        id (str): The unique identifier of the Neptune Analytics graph instance.
        name (str): The human-readable name of the graph instance.
        status (str): The current status of the graph (e.g., 'AVAILABLE', 'CREATING', 'DELETING').
        details (dict): Complete response dictionary from Neptune Analytics API containing
                       all graph metadata (endpoint, memory, replicas, etc.).
    """

    def __init__(
        self,
        id: str,
        name: str,
        status: str,
        details: dict,
        neptune_client=None,
    ):
        """Initialize a SessionGraph instance.

        Args:
            id (str): The unique identifier of the Neptune Analytics graph.
            name (str): The name of the graph instance.
            status (str): The current status of the graph.
            details (dict): Complete metadata dictionary for the graph.
            neptune_client (optional): Boto3 Neptune Analytics client. If not provided,
                                      a new client will be created.
        """
        self.id = id
        self.name = name
        self.status = status
        self.details = details
        self._neptune_client = neptune_client or boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )

    @classmethod
    def from_response(cls, response: dict, neptune_client=None):
        """Create a SessionGraph instance from a Neptune Analytics API response.

        Args:
            response (dict): Response dictionary from Neptune Analytics list_graphs or
                           get_graph API calls. Must contain 'id', 'name', and 'status' fields.
            neptune_client (optional): Boto3 Neptune Analytics client to use for this instance.

        Returns:
            SessionGraph: A new SessionGraph instance populated with data from the response.

        Example:
            >>> response = {"id": "g-123", "name": "my-graph", "status": "AVAILABLE"}
            >>> graph = SessionGraph.from_response(response)
        """
        name = response["name"]
        id = response["id"]
        status = response["status"]
        return cls(id, name, status, response, neptune_client)

    def execute_query(self, opencypher: str):
        """Execute an openCypher query against this Neptune Analytics graph instance.

        Args:
            opencypher (str): openCypher query string to execute against the graph.

        Returns:
            dict: Query results from Neptune Analytics containing the result set.

        Example:
            >>> graph = SessionGraph(id="g-123", name="my-graph", status="AVAILABLE", details={})
            >>> result = graph.execute_query("MATCH (n) RETURN n LIMIT 10")
        """
        na_client = NeptuneAnalyticsClient(self.id, self._neptune_client)
        return na_client.execute_generic_query(opencypher)


class SessionManager:
    """Manages Neptune Analytics sessions and graph operations."""

    def __init__(self, session_name=None, cleanup_task=None):
        """Initialize a SessionManager instance.

        Args:
            session_name (str, optional): Name prefix for filtering graphs.
            cleanup_task (CleanupTask, optional): Cleanup task to perform on exit. Defaults to None.
              When set to DESTROY, will destroy all instances in the session on exit.
              When set to RESET, will reset all instances in the session on exit.
              When set to STOP, will stop all instances in the session on exit.
        """
        self.session_name = session_name
        self.cleanup_task = cleanup_task or CleanupTask.NONE
        self._neptune_client = boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )
        self._sts_client = boto3.client(SERVICE_STS)
        self._s3_iam_role = self._sts_client.get_caller_identity()["Arn"]
        self._iam_client = boto3.client(SERVICE_IAM)
        self._s3_client = boto3.client(SERVICE_S3)
        self._athena_client = boto3.client(SERVICE_ATHENA)

    @classmethod
    def session(cls, session_name=None, cleanup_task=None):
        """Create a new SessionManager instance.

        Args:
            session_name (str, optional): Name prefix for filtering graphs.
            cleanup_task (CleanupTask, optional): Cleanup task to perform on exit. Defaults to None.

        Returns:
            SessionManager: A new SessionManager instance.
        """
        return cls(session_name, cleanup_task)

    def __enter__(self):
        """Enter the session manager."""

        # TODO: consider creating graphs here
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and clean up graphs."""

        if self.cleanup_task == CleanupTask.DESTROY:
            self.destroy_all_graphs()

    def execute_query(self, graph_id: str, opencypher: str):
        """Execute an openCypher query against a Neptune Analytics graph.

        Args:
            graph_id (str): ID of the Neptune Analytics graph to query.
            opencypher (str): openCypher query string to execute.

        Returns:
            dict: Query results from Neptune Analytics.
        """
        return self.get_graph(graph_id).execute_query(opencypher)

    def validate_permissions(self):
        """Validate AWS permissions for Neptune Analytics operations.

        Returns:
            bool: True if permissions are valid, False otherwise.
        """
        return instance_management.validate_permissions()

    def list_graphs(self) -> list[SessionGraph]:
        """List available Neptune Analytics graphs.

        If session_name is set, filters graphs to those starting with the session_name prefix.

        Returns:
            list: List of SessionGraph objects containing graph metadata.
        """
        response = self._neptune_client.list_graphs()
        graphs = response.get("graphs", [])

        if self.session_name:
            graphs = filter(
                lambda g: g.get("name", "").startswith(self.session_name), graphs
            )

        return [SessionGraph.from_response(g, self._neptune_client) for g in graphs]

    def _get_existing_graph(
        self, filter_status: Optional[list[str]] = None
    ) -> Optional[SessionGraph]:
        """Get the first existing graph, optionally filtered by status.

        Args:
            filter_status: Optional list of status values to filter by (case-insensitive).
                          If None, returns the first graph regardless of status.

        Returns:
            SessionGraph or None: Graph details if found, None otherwise.
        """
        graphs = self.list_graphs()
        if not graphs:
            return None

        if filter_status is None:
            return graphs[0]

        filter_status_lower = [s.lower() for s in filter_status]
        for graph in graphs:
            if graph.status.lower() in filter_status_lower:
                return graph
        return None

    def get_graph(self, graph_id: str) -> SessionGraph:
        """Get details for a specific graph by ID.

        Args:
            graph_id (str): ID of the graph to retrieve

        Returns:
            SessionGraph: Graph, with details, if found

        Raises:
            Exception: If no graph is found with the given ID
        """
        graphs = self.list_graphs()

        for graph in graphs:
            if graph.id == graph_id:
                return graph

        # Package that as nx object
        raise Exception(f"No graph instance with id {graph_id} found")

    async def get_or_create_graph(self, config: Optional[dict] = None) -> SessionGraph:
        """Get the first available graph or create a new one if none exist.

        Returns:
            SessionGraph: graph object for an available instance
        """
        graph = self._get_existing_graph(filter_status=["AVAILABLE"])
        if graph:
            return graph
        # create a new graph and return
        logger.info(f"Creating new graph named with prefix: {self.session_name}")
        graph_id = await instance_management.create_na_instance(
            config=config,
            na_client=self._neptune_client,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
            graph_name_prefix=self.session_name,
        )
        return self.get_graph(graph_id)

    async def create_from_snapshot(
        self,
        snapshot_id: str,
        config: Optional[dict] = None,
    ) -> SessionGraph:
        """Create a new Neptune Analytics instance from a snapshot.

        Args:
            snapshot_id (str): Name of the snapshot to create instance from.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            SessionGraph: graph with details
        """
        logger.info(
            f"Creating new graph from snapshot {snapshot_id} named with prefix: {self.session_name}"
        )
        graph_id = await instance_management.create_na_instance_from_snapshot(
            snapshot_id,
            config=config,
            graph_name_prefix=self.session_name,
            na_client=self._neptune_client,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
        )
        return self.get_graph(graph_id)

    async def create_from_csv(
        self,
        s3_arn: str,
        config: Optional[dict] = None,
    ) -> SessionGraph:
        """Create a new Neptune Analytics instance from a s3 bucket location with CSV data.

        Args:
            s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            SessionGraph: graph with details
        """
        logger.info(
            f"Creating new graph from csv {s3_arn} named with prefix: {self.session_name}"
        )
        graph_id, task_id = await instance_management.create_na_instance_with_s3_import(
            s3_arn,
            config=config,
            graph_name_prefix=self.session_name,
            na_client=self._neptune_client,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
        )
        return self.get_graph(graph_id)

    async def create_multiple_instances(
        self, count: int, config: Optional[dict] = None
    ) -> list[str]:
        """Create multiple Neptune Analytics instances in parallel.

        Args:
            count (int): Number of instances to create.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            list[str]: List of graph IDs for the created instances.
        """
        tasks = [
            instance_management.create_na_instance(
                config=config,
                na_client=self._neptune_client,
                sts_client=self._sts_client,
                iam_client=self._iam_client,
                graph_name_prefix=self.session_name,
            )
            for _ in range(count)
        ]
        graph_ids = await asyncio.gather(*tasks)
        return graph_ids

    async def export_to_csv(
        self,
        graph: Union[str, SessionGraph],
        s3_location,
        export_filter=None,
    ) -> str:
        """Export Neptune Analytics graph data to CSV files in S3.

        Args:
            graph (Union[str, SessionGraph]): Graph ID string or SessionGraph.
            s3_location (str): S3 location to store exported CSV files.
            export_filter (dict, optional): Filter criteria for the export. Defaults to None.

        Returns:
            str: Task ID of the export operation.
        """
        graph_id = _get_graph_id(graph)

        return await instance_management.export_csv_to_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
                nx.Graph(),
            ),
            s3_location,
            export_filter=export_filter,
        )

    async def import_from_csv(
        self,
        graph: Union[str, SessionGraph],
        s3_location,
        reset_graph_ahead=False,
        max_size: Optional[int] = None,
    ) -> str:
        """Import CSV data from S3 into a Neptune Analytics graph.

        Args:
            graph (Union[str, SessionGraph]): Graph ID string or SessionGraph.
            s3_location (str): S3 location containing CSV data to import.
            reset_graph_ahead (bool, optional): Whether to reset the graph before import. Defaults to False.
            max_size (int, optional): If defined, maximum memory size in GB to scale up to. Defaults to None.

        Returns:
            str: Task ID of the import operation.

        Raises:
            ClientError: If import fails due to insufficient memory and max_size is exceeded, or other AWS client errors.
        """

        graph_id = _get_graph_id(graph)
        skip_snapshot = True
        while True:
            try:
                return await instance_management.import_csv_from_s3(
                    NeptuneGraph(
                        NeptuneAnalyticsClient(graph_id, self._neptune_client),
                        IamClient(self._s3_iam_role, self._iam_client),
                        nx.Graph(),
                    ),
                    s3_location,
                    reset_graph_ahead,
                    skip_snapshot,
                )
            except ClientError as e:
                current_size = self.get_graph(graph_id).details["provisionedMemory"]
                if (
                    max_size is not None
                    and e.response["Error"]["Code"] == "InsufficientMemory"
                    and max_size > current_size
                ):
                    prospect_size = current_size * 2

                    if prospect_size > max_size > current_size:
                        prospect_size = max_size

                    await instance_management.update_na_instance_size(
                        graph_id=graph_id, prospect_size=prospect_size
                    )
                    continue
                else:
                    raise e

    async def import_from_table(
        self,
        graph: Union[str, SessionGraph],
        s3_location,
        sql_queries,
        sql_parameters=None,
        catalog=None,
        database=None,
        remove_buckets=True,
    ) -> str:
        """Import data from Athena table query results into a Neptune Analytics graph.

        Args:
            graph (Union[str, SessionGraph]): Graph ID string or SessionGraph.
            s3_location (str): S3 location to store intermediate CSV data.
            sql_queries (list): List of SQL queries to execute against Athena tables.
            sql_parameters (list[list], optional): 2D List of execution parameters to pass with each SQL query.
            catalog (str, optional): Athena catalog name. Defaults to None.
            database (str, optional): Athena database name. Defaults to None.
            remove_buckets (bool): After a successful import, delete the S3 bucket contents if True.

        Returns:
            str: Graph ID of the target graph.
        """
        graph_id = _get_graph_id(graph)

        logger.info(f"Importing to graph {graph_id}")

        reset_graph_ahead = False
        skip_snapshot = True

        if sql_parameters is None:
            sql_parameters = []

        # export the datalake table to S3 as CSV projection data
        query_execution_ids = await instance_management.export_athena_table_to_s3(
            sql_queries,
            sql_parameters,
            s3_location,
            catalog,
            database,
            athena_client=self._athena_client,
            s3_client=self._s3_client,
            iam_client=self._iam_client,
            sts_client=self._sts_client,
        )
        if not query_execution_ids:
            raise Exception("Projections not created.")

        logger.info(f"Created projection data in {s3_location}")

        # import the S3 CSV files to Neptune
        task_id = await instance_management.import_csv_from_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
                nx.Graph(),
            ),
            s3_location,
            reset_graph_ahead,
            skip_snapshot,
        )

        if remove_buckets:
            for query_execution_id in query_execution_ids:
                delete_location = f"{s3_location}{query_execution_id}.csv"
                logger.info(f"deleting bucket at {delete_location}")
                instance_management.empty_s3_bucket(
                    delete_location,
                    self._s3_client,
                    self._sts_client,
                    self._iam_client,
                )

        logger.info(f"Graph data imported to graph {graph_id} using task {task_id}")
        return task_id

    async def export_to_table(
        self,
        graph: Union[str, SessionGraph],
        s3_location: str,
        csv_table_name: str,
        csv_catalog: str,
        csv_database: str,
        iceberg_vertices_table_name: str,
        iceberg_edges_table_name: str,
        iceberg_catalog: str,
        iceberg_database: str,
        remove_resources=True,
    ) -> str:
        """Export Neptune Analytics graph data to Athena tables via S3.

        Args:
            graph (Union[str, SessionGraph]): Graph ID string or SessionGraph.
            s3_location (str): S3 location to store exported CSV data.
            csv_table_name (str): Name for the intermediate CSV table in Athena.
            csv_catalog (str): Athena catalog for CSV table.
            csv_database (str): Athena database for CSV table.
            iceberg_vertices_table_name (str): Name for the vertices Iceberg table.
            iceberg_edges_table_name (str): Name for the edges Iceberg table.
            iceberg_catalog (str): Athena catalog for Iceberg tables.
            iceberg_database (str): Athena database for Iceberg tables.
            remove_resources (bool): After a successful import, delete all temporary resources if True.

        Returns:
            str: Query execution ID from the final Athena operation.
        """
        graph_id = _get_graph_id(graph)

        logger.info(f"Exporting graph: {graph_id}")

        task_id = await instance_management.export_csv_to_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
                nx.Graph(),
            ),
            s3_location,
        )

        s3_export_location = f"{s3_location}/{task_id}"
        logger.info(f"Graph exported to S3 at location: {s3_export_location}")

        ###
        logger.info(f"Creating CSV export table; SQL logs output to {s3_location}")

        # Create table - blocking
        await instance_management.create_csv_table_from_s3(
            s3_export_location,
            s3_location,  # logs directory
            csv_table_name,
            catalog=csv_catalog,
            database=csv_database,
            athena_client=self._athena_client,
            s3_client=self._s3_client,
            iam_client=self._iam_client,
            sts_client=self._sts_client,
        )
        logger.info(f"Table created {csv_catalog}/{csv_database}/{csv_table_name}")

        ###
        logger.info(
            f"Creating iceberg table for vertices: {iceberg_catalog}/{iceberg_database}/{iceberg_vertices_table_name}"
        )
        logger.info(f"SQL logs output to {s3_location}")

        csv_vertices_table_name = (
            f"{csv_catalog}.{csv_database}.{csv_table_name}_vertices"
        )
        query_id = await instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_vertices_table_name,
            csv_vertices_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database,
            athena_client=self._athena_client,
            iam_client=self._iam_client,
            sts_client=self._sts_client,
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_vertices_table_name} with query ID: {query_id}"
        )

        ###
        logger.info(
            f"Creating iceberg table for edges: {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}"
        )
        logger.info(f"SQL logs output to {s3_location}")

        csv_edges_table_name = f"{csv_catalog}.{csv_database}.{csv_table_name}_edges"
        query_id = await instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_edges_table_name,
            csv_edges_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database,
            athena_client=self._athena_client,
            iam_client=self._iam_client,
            sts_client=self._sts_client,
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name} with query ID: {query_id}"
        )

        if remove_resources:
            # remove export bucket
            instance_management.empty_s3_bucket(
                s3_export_location, self._s3_client, self._sts_client, self._iam_client
            )

            # drop CSV table
            await instance_management.drop_athena_table(
                csv_table_name,
                s3_location,
                catalog=csv_catalog,
                database=csv_database,
                athena_client=self._athena_client,
                sts_client=self._sts_client,
                iam_client=self._iam_client,
            )

        return query_id

    async def create_snapshot(
        self, graph: Union[str, SessionGraph], snapshot_name: str
    ):
        """Create a Neptune Analytics graph snapshot.

        Args:
            graph: Either a graph ID string or a SessionGraph
            snapshot_name (str): Name of the snapshot to create instances from.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            snapshot_id that was created
        """
        graph_id = _get_graph_id(graph)

        logger.info(f"Creating snapshot: {snapshot_name}")
        snapshot_id = await instance_management.create_graph_snapshot(
            graph_id,
            snapshot_name,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
            na_client=self._neptune_client,
        )

        logger.info(f"Snapshot [{snapshot_id}] create complete")
        return snapshot_id

    async def delete_snapshot(self, snapshot_id: str) -> str:
        """Delete a Neptune Analytics graph snapshot.

        Args:
            snapshot_id (str): ID of the snapshot to delete.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            snapshot_id that was deleted
        """
        logger.info(f"Deleting snapshot: {snapshot_id}")
        deleted_snapshot_id = await instance_management.delete_graph_snapshot(
            snapshot_id,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
            na_client=self._neptune_client,
        )

        logger.info(f"Snapshot [{deleted_snapshot_id}] delete complete")
        return deleted_snapshot_id

    def destroy_graph(self, graph_name: Union[str, list[str]]):
        """Destroy one or more Neptune Analytics graphs.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to stop

        Returns:
            asyncio.Future: A future that resolves when the graphs have been stopped.
        """
        return self._destroy_graphs(graph_name)

    def destroy_all_graphs(self):
        """Delete all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and permanently deletes each graph instance.
        This operation cannot be undone.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been deleted.
        """
        return self._destroy_graphs([])

    def start_graph(self, graph_name: Union[str, list[str]]):
        """Start one or more Neptune Analytics graphs.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to stop

        Returns:
            asyncio.Future: A future that resolves when the graphs have been stopped.
        """
        return self._start_graphs(graph_name)

    def start_all_graphs(self):
        """Start all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._start_graphs([])

    def stop_graph(self, graph_name: Union[str, list[str]]):
        """Stop one or more Neptune Analytics graphs.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to stop

        Returns:
            asyncio.Future: A future that resolves when the graphs have been stopped.
        """
        return self._stop_graphs(graph_name)

    def stop_all_graphs(self):
        """Stop all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._stop_graphs([])

    def reset_graph(self, graph_name: Union[str, list[str]]):
        """Reset one or more Neptune Analytics graphs.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to stop

        Returns:
            asyncio.Future: A future that resolves when the graphs have been stopped.
        """
        return self._reset_graphs(graph_name)

    def reset_all_graphs(self):
        """Stop all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._reset_graphs([])

    def _destroy_graphs(self, graph_name: Union[str, list[str]]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.delete_na_instance,
            status_to_check="AVAILABLE",
            graph_names=graph_name,
        )

    def _stop_graphs(self, graph_name: Union[str, list[str]]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.stop_na_instance,
            status_to_check="AVAILABLE",
            graph_names=graph_name,
        )

    def _start_graphs(self, graph_name: Union[str, list[str]]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.start_na_instance,
            status_to_check="STOPPED",
            graph_names=graph_name,
        )

    def _reset_graphs(self, graph_name: Union[str, list[str]]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.reset_graph,
            status_to_check="AVAILABLE",
            graph_names=graph_name,
        )

    def _graph_bulk_operation(
        self, operation: Callable, status_to_check: str, graph_names: list[str]
    ):
        # Get all graphs matching name filter if specified
        graphs = [
            graph
            for graph in self.list_graphs()
            if len(graph_names) == 0 or graph.name in graph_names
        ]
        if graph_names and len(graphs) == 0:
            logger.warning(
                f"No graphs found matching name: {graph_names} and status: {status_to_check}"
            )

        # Filter for graphs in correct status, log warning for others
        graph_ids = []
        for graph in graphs:
            if graph.status == status_to_check:
                graph_ids.append(graph.id)
            else:
                logger.warning(
                    f"Skipping graph {graph.id} - status is {graph.status}, expected {status_to_check}"
                )

        future_list = []
        for graph_id in graph_ids:
            future_list.append(operation(graph_id))
        return asyncio.gather(*future_list)


def _get_graph_id(graph: Union[str, SessionGraph]) -> str:
    """Extract graph ID from string or dictionary.

    Args:
        graph: Either a graph ID string or a dictionary containing an 'id' field

    Returns:
        str: The graph ID

    Raises:
        Exception: If graph is not a string and doesn't contain an 'id' field
    """
    if isinstance(graph, str):
        return graph
    if isinstance(graph, SessionGraph):
        return graph.id
    raise Exception(
        "No graph id provided - 'graph' must a graph id string, or contain an `id` field"
    )
