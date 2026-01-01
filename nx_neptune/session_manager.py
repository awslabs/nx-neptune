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
from .clients.neptune_constants import APP_ID_NX, SERVICE_IAM, SERVICE_NA, SERVICE_STS

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


def _format_output_graph(graph_details: dict[str, str], with_details=False):
    """Format graph details for output.

    Args:
        graph_details: Dictionary containing graph information
        with_details: If True, return full details; if False, return only name, id, and status

    Returns:
        Dictionary with formatted graph information
    """
    if with_details:
        return graph_details
    return {
        "name": graph_details["name"],
        "id": graph_details["id"],
        "status": graph_details["status"],
    }


def _get_graph_id(graph: Union[str, dict[str, str]]) -> str:
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
    if isinstance(graph, dict) and graph["id"]:
        return graph["id"]
    raise Exception(
        "No graph id provided - 'graph' must a graph id string, or contain an `id` field"
    )


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

        na_client = NeptuneAnalyticsClient(graph_id, self._neptune_client)
        return na_client.execute_generic_query(opencypher)

    def validate_permissions(self):
        """Validate AWS permissions for Neptune Analytics operations.

        Returns:
            bool: True if permissions are valid, False otherwise.
        """
        return instance_management.validate_permissions()

    def list_graphs(self, with_details=False):
        """List available Neptune Analytics graphs.

        If session_name is set, filters graphs to those starting with the session_name prefix.

        Returns:
            list: List of graph dictionaries containing graph metadata.
        """
        response = self._neptune_client.list_graphs()
        graphs = response.get("graphs", [])

        if self.session_name:
            graphs = [
                _format_output_graph(g, with_details)
                for g in graphs
                if g.get("name", "").startswith(self.session_name)
            ]

        return graphs

    def _get_existing_graph(self, filter_status: Optional[list[str]] = None):
        """Get the first existing graph, optionally filtered by status.

        Args:
            filter_status: Optional list of status values to filter by (case-insensitive).
                          If None, returns the first graph regardless of status.

        Returns:
            dict or None: Graph details if found, None otherwise.
        """
        graphs = self.list_graphs()
        if not graphs:
            return None

        if filter_status is None:
            return graphs[0]

        filter_status_lower = [s.lower() for s in filter_status]
        for graph in graphs:
            if graph.get("status", "").lower() in filter_status_lower:
                return graph
        return None

    def get_graph(self, graph_id: str):
        """Get details for a specific graph by ID.

        Args:
            graph_id (str): ID of the graph to retrieve

        Returns:
            dict: Graph details if found

        Raises:
            Exception: If no graph is found with the given ID
        """
        graphs = self.list_graphs()

        for graph in graphs:
            if graph["id"] == graph_id:
                return graph

        # Package that as nx object
        raise Exception(f"No graph instance with id {graph_id} found")

    async def get_or_create_graph(self, config: Optional[dict] = None) -> dict:
        """Get the first available graph or create a new one if none exist.

        Returns:
            dict with the graph status
        """
        graph = self._get_existing_graph(filter_status=["AVAILABLE"])
        if graph:
            return _format_output_graph(graph)
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
    ) -> dict:
        """Create a new Neptune Analytics instance from a snapshot.

        Args:
            snapshot_id (str): Name of the snapshot to create instance from.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            dict with the graph status
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
    ) -> dict:
        """Create a new Neptune Analytics instance from a s3 bucket location with CSV data.

        Args:
            s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            dict with the graph status
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
        graph: Union[str, dict[str, str]],
        s3_location,
        export_filter=None,
    ) -> str:
        """Export Neptune Analytics graph data to CSV files in S3.

        Args:
            graph (Union[str, dict[str, str]]): Graph ID string or graph metadata dict.
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
        graph: Union[str, dict[str, str]],
        s3_location,
        reset_graph_ahead=False,
        max_size=None
    ) -> str:
        """Import CSV data from S3 into a Neptune Analytics graph.

        Args:
            graph (Union[str, dict[str, str]]): Graph ID string or graph metadata dict.
            s3_location (str): S3 location containing CSV data to import.
            reset_graph_ahead (bool, optional): Whether to reset the graph before import. Defaults to False.
            max_size (int, optional): Maximum memory size in GB to scale up to if import fails due to insufficient memory. Defaults to None.

        Returns:
            str: Task ID of the import operation.

        Raises:
            ClientError: If import fails due to insufficient memory and max_size is exceeded, or other AWS client errors.
        """

        graph_id = _get_graph_id(graph)
        skip_snapshot = True
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
            if e.response["Error"]["Code"] == "InsufficientMemory":
                current_size = self.get_graph(graph_id)["provisionedMemory"]

                while max_size and max_size >= current_size*2:
                    await instance_management.update_na_instance_size(
                        graph_id=graph_id, prospect_size=current_size * 2)
                    current_size = current_size * 2

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
                        if e.response["Error"]["Code"] == "InsufficientMemory":
                            continue
                        else:
                            raise e

    async def import_from_table(
        self,
        graph: Union[str, dict[str, str]],
        s3_location,
        sql_queries,
        catalog=None,
        database=None,
    ) -> str:
        """Import data from Athena table query results into a Neptune Analytics graph.

        Args:
            graph (Union[str, dict[str, str]]): Graph ID string or graph metadata dict.
            s3_location (str): S3 location to store intermediate CSV data.
            sql_queries (list): List of SQL queries to execute against Athena tables.
            catalog (str, optional): Athena catalog name. Defaults to None.
            database (str, optional): Athena database name. Defaults to None.

        Returns:
            str: Graph ID of the target graph.
        """
        graph_id = _get_graph_id(graph)

        logger.info(f"Importing to graph {graph_id}")

        reset_graph_ahead = False
        skip_snapshot = True

        # export the datalake table to S3 as CSV projection data
        projection_created = await instance_management.export_athena_table_to_s3(
            sql_queries,
            s3_location,
            catalog,
            database,
        )
        if not projection_created:
            raise Exception("Projection not created.")

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

        # TODO Cleanup resources

        logger.info(f"Graph data imported to graph {graph_id} using task {task_id}")
        return task_id

    async def export_to_table(
        self,
        graph: Union[str, dict[str, str]],
        s3_location: str,
        csv_table_name: str,
        csv_catalog: str,
        csv_database: str,
        iceberg_vertices_table_name: str,
        iceberg_edges_table_name: str,
        iceberg_catalog: str,
        iceberg_database: str,
    ) -> str:
        """Export Neptune Analytics graph data to Athena tables via S3.

        Args:
            graph (Union[str, dict[str, str]]): Graph ID string or graph metadata dict.
            s3_location (str): S3 location to store exported CSV data.
            csv_table_name (str): Name for the intermediate CSV table in Athena.
            csv_catalog (str): Athena catalog for CSV table.
            csv_database (str): Athena database for CSV table.
            iceberg_vertices_table_name (str): Name for the vertices Iceberg table.
            iceberg_edges_table_name (str): Name for the edges Iceberg table.
            iceberg_catalog (str): Athena catalog for Iceberg tables.
            iceberg_database (str): Athena database for Iceberg tables.

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
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name} with query ID: {query_id}"
        )

        return query_id

    async def create_snapshot(
        self, graph: Union[str, dict[str, str]], snapshot_name: str
    ):
        """Create a Neptune Analytics graph snapshot.

        Args:
            graph: Either a graph ID string or a dictionary containing an 'id' field
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
            if len(graph_names) == 0 or graph["name"] in graph_names
        ]
        if graph_names and len(graphs) == 0:
            logger.warning(
                f"No graphs found matching name: {graph_names} and status: {status_to_check}"
            )

        # Filter for graphs in correct status, log warning for others
        graph_ids = []
        for graph in graphs:
            if graph["status"] == status_to_check:
                graph_ids.append(graph["id"])
            else:
                logger.warning(
                    f"Skipping graph {graph['id']} - status is {graph['status']}, expected {status_to_check}"
                )

        future_list = []
        for graph_id in graph_ids:
            future_list.append(operation(graph_id))
        return asyncio.gather(*future_list)
