# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
from asyncio import Future
from typing import Optional, Callable

import boto3
import networkx as nx
from botocore.config import Config

from . import NeptuneGraph, instance_management
from .clients import IamClient, NeptuneAnalyticsClient
from .clients.neptune_constants import APP_ID_NX, SERVICE_IAM, SERVICE_NA, SERVICE_STS

logger = logging.getLogger(__name__)


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


def _get_graph_id(graph: str | dict[str, str]) -> str:
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

    def __init__(self, session_name=None):
        """Initialize a SessionManager instance.

        Args:
            session_name (str, optional): Name prefix for filtering graphs.
        """
        self.session_name = session_name
        self._neptune_client = boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )
        self._sts_client = boto3.client(SERVICE_STS)

        self._s3_iam_role = self._sts_client.get_caller_identity()["Arn"]

        self._iam_client = boto3.client(SERVICE_IAM)

    @classmethod
    def session(cls, session_name=None):
        """Create a new SessionManager instance.

        Args:
            session_name (str, optional): Name prefix for filtering graphs.

        Returns:
            SessionManager: A new SessionManager instance.
        """
        return cls(session_name)

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

    def _get_existing_graph(self, filter_status: list[str] | None = None):
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

    async def get_or_create_graph(self, config: Optional[dict] = None):
        """Get the first available graph or create a new one if none exist.

        Returns:
            dict or asyncio.Future: Graph metadata dict if a graph exists,
                                   or Future that resolves when new graph is created.
        """
        graph = self._get_existing_graph()
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
        return {"id": graph_id, "status": "CREATING"}

    async def import_from_csv(
        self,
        graph: str | dict[str, str],
        s3_location,
    ):
        graph_id = _get_graph_id(graph)

        reset_graph_ahead = False
        skip_snapshot = True

        # TODO Cleanup resources

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

    async def import_from_table(
        self,
        graph: str | dict[str, str],
        s3_location,
        sql_queries,
        catalog=None,
        database=None,
    ):
        graph_id = _get_graph_id(graph)

        logger.info(f"Importing to graph {graph_id}")

        reset_graph_ahead = False
        skip_snapshot = True

        # export the datalake table to S3 as CSV projection data
        projection_created = instance_management.export_athena_table_to_s3(
            sql_queries,
            s3_location,
            catalog,
            database,
        )
        if not projection_created:
            raise Exception("Projection not created.")

        logger.info(f"Created projection data in {s3_location}")

        # import the S3 CSV files to Neptune
        future = instance_management.import_csv_from_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
                nx.Graph(),
            ),
            s3_location,
            reset_graph_ahead,
            skip_snapshot,
        )
        import_blocking_status = await future
        print("Import completed with status: " + import_blocking_status)

        # TODO Cleanup resources

        logger.info(f"Graph data imported to graph {graph_id}")

    async def export_to_table(
        self,
        graph: str | dict[str, str],
        s3_location: str,
        csv_table_name: str,
        csv_catalog: str,
        csv_database: str,
        iceberg_vertices_table_name: str,
        iceberg_edges_table_name: str,
        iceberg_catalog: str,
        iceberg_database: str,
    ):
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
        instance_management.create_csv_table_from_s3(
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
        instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_vertices_table_name,
            csv_vertices_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database,
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_vertices_table_name}"
        )

        ###
        logger.info(
            f"Creating iceberg table for edges: {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}"
        )
        logger.info(f"SQL logs output to {s3_location}")

        csv_edges_table_name = f"{csv_catalog}.{csv_database}.{csv_table_name}_edges"
        instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_edges_table_name,
            csv_edges_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database,
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}"
        )
        logger.info(
            f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}"
        )

        return True

    def destroy_graph(self, graph_name: str | list[str]):
        """Destroy one or more Neptune Analytics graphs.

        Args:
            graph_name (str | list[str]): Name or list of names of graphs to stop

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

    def start_graph(self, graph_name: str | list[str]):
        """Start one or more Neptune Analytics graphs.

        Args:
            graph_name (str | list[str]): Name or list of names of graphs to stop

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

    def stop_graph(self, graph_name: str | list[str]):
        """Stop one or more Neptune Analytics graphs.

        Args:
            graph_name (str | list[str]): Name or list of names of graphs to stop

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

    def reset_graph(self, graph_name: str | list[str]):
        """Reset one or more Neptune Analytics graphs.

        Args:
            graph_name (str | list[str]): Name or list of names of graphs to stop

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

    def _destroy_graphs(self, graph_name: str | list[str]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.delete_na_instance,
            status_to_check="AVAILABLE",
            graph_names=graph_name,
        )

    def _stop_graphs(self, graph_name: str | list[str]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.stop_na_instance,
            status_to_check="AVAILABLE",
            graph_names=graph_name,
        )

    def _start_graphs(self, graph_name: str | list[str]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.start_na_instance,
            status_to_check="STOPPED",
            graph_names=graph_name,
        )

    def _reset_graphs(self, graph_name: str | list[str]):
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
