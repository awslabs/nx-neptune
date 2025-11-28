# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
from asyncio import Future
from typing import Optional
import boto3

from . import instance_management
from .clients import NeptuneAnalyticsClient, IamClient
from .clients.neptune_constants import SERVICE_IAM, SERVICE_NA, SERVICE_STS

logger = logging.getLogger(__name__)


class SessionManager:
    """Manages Neptune Analytics sessions and graph operations."""

    def __init__(self, session_name=None):
        """Initialize a SessionManager instance.

        Args:
            session_name (str, optional): Name prefix for filtering graphs.
        """
        self.session_name = session_name
        self._neptune_client = boto3.client(SERVICE_NA)
        self._sts_client = boto3.client(SERVICE_STS)

        self._s3_iam_role = self._sts_client.get_caller_identity()["Arn"]

        self._iam_client = boto3.client(SERVICE_IAM)

    @classmethod
    def Session(cls, session_name=None):
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

    def _format_output_graph(self, graph_details: dict[str, str], with_details=False):
        if with_details:
            return graph_details
        return {
            "name": graph_details["name"],
            "id": graph_details["id"],
            "status": graph_details["status"]
        }

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
                self._format_output_graph(g, with_details) for g in graphs if
                g.get("name", "").startswith(self.session_name)
            ]

        return graphs

    def _get_existing_graph(self):
        graphs = self.list_graphs()
        if graphs:
            return graphs[0]
        return None

    async def get_or_create_graph(self, config: Optional[dict] = None):
        """Get the first available graph or create a new one if none exist.

        Returns:
            dict or asyncio.Future: Graph metadata dict if a graph exists,
                                   or Future that resolves when new graph is created.
        """
        graph = self._get_existing_graph()
        if graph:
            return self._format_output_graph(graph)
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

    def import_from_csv(
            self,
            graph: str | dict[str, str],
            s3_location,
    ) -> Future:
        if isinstance(graph, str):
            graph_id = graph
        elif isinstance(graph, dict) and graph.id:
            graph_id = graph.id
        else:
            raise Exception("No graph id provided - 'graph' must a graph id string, or contain an `id` field")

        reset_graph_ahead = False
        skip_snapshot = True

        # TODO Cleanup resources

        return instance_management.import_csv_from_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
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
        if isinstance(graph, str):
            graph_id = graph
        elif isinstance(graph, dict) and graph["id"]:
            graph_id = graph["id"]
        else:
            raise Exception("No graph id provided - 'graph' must a graph id string, or contain an `id` field")

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
    ) -> Future:

        if isinstance(graph, str):
            graph_id = graph
        elif isinstance(graph, dict) and graph["id"]:
            graph_id = graph["id"]
        else:
            raise Exception("No graph id provided - 'graph' must a graph id string, or contain an `id` field")

        logger.info(f"Exporting graph: {graph_id}")

        task_id = await instance_management.export_csv_to_s3(
            NeptuneGraph(
                NeptuneAnalyticsClient(graph_id, self._neptune_client),
                IamClient(self._s3_iam_role, self._iam_client),
            ),
            s3_location
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
            database=csv_database
        )
        logger.info(f"Table created {csv_catalog}/{csv_database}/{csv_table_name}")

        ###
        logger.info(
            f"Creating iceberg table for vertices: {iceberg_catalog}/{iceberg_database}/{iceberg_vertices_table_name}")
        logger.info(f"SQL logs output to {s3_location}")

        csv_vertices_table_name = f"{csv_catalog}.{csv_database}.{csv_table_name}_vertices"
        instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_vertices_table_name,
            csv_vertices_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database
        )
        logger.info(f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_vertices_table_name}")

        ###
        logger.info(
            f"Creating iceberg table for edges: {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}")
        logger.info(f"SQL logs output to {s3_location}")

        csv_edges_table_name = f"{csv_catalog}.{csv_database}.{csv_table_name}_edges"
        instance_management.create_iceberg_table_from_table(
            s3_location,
            iceberg_edges_table_name,
            csv_edges_table_name,
            catalog=iceberg_catalog,
            database=iceberg_database
        )
        logger.info(f"Table created {iceberg_catalog}/{iceberg_database}/{iceberg_edges_table_name}")

        return True

    def destroy_graph(self, graph_name):
        """Destroy a specific Neptune Analytics graph.

        Args:
            graph_name (str): Name of the graph to stop

        Returns:
            asyncio.Future: A future that resolves when the graph has been stopped.
        """
        return self._destroy_graphs(graph_name)

    def destroy_all_graphs(self):
        """Delete all Neptune Analytics graphs associated with this session.

            Fetches all graph IDs for the current session and permanently deletes each graph instance.
            This operation cannot be undone.

            Returns:
                asyncio.Future: A future that resolves when all graphs have been deleted.
            """
        return self._destroy_graphs()

    def start_graph(self, graph_name):
        """Start a specific Neptune Analytics graph.

        Args:
            graph_name (str): Name of the graph to stop

        Returns:
            asyncio.Future: A future that resolves when the graph has been stopped.
        """
        return self._start_graphs(graph_name)

    def reset_graph(self, graph_name):
        """Start a specific Neptune Analytics graph.

        Args:
            graph_name (str): Name of the graph to stop

        Returns:
            asyncio.Future: A future that resolves when the graph has been stopped.
        """
        return self._reset_graphs(graph_name)

    def start_all_graphs(self):
        """Start all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._start_graphs()

    def stop_graph(self, graph_name):
        """Stop a specific Neptune Analytics graph.

        Args:
            graph_name (str): Name of the graph to stop

        Returns:
            asyncio.Future: A future that resolves when the graph has been stopped.
        """
        return self._stop_graphs(graph_name)

    def stop_all_graphs(self):
        """Stop all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._stop_graphs()

    def reset_all_graphs(self):
        """Stop all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and stops each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been stopped.
        """
        return self._reset_graphs()

    def _destroy_graphs(self, graph_name: str = None):
        return self._graph_bulk_operation(
            operation=instance_management.delete_na_instance,
            status_to_check='AVAILABLE',
            graph_name=graph_name
        )

    def _stop_graphs(self, graph_name: str = None):
        return self._graph_bulk_operation(
            operation=instance_management.stop_na_instance,
            status_to_check='AVAILABLE',
            graph_name=graph_name
        )

    def _start_graphs(self, graph_name: str = None):
        return self._graph_bulk_operation(
            operation=instance_management.start_na_instance,
            status_to_check='STOPPED',
            graph_name=graph_name
        )

    def _reset_graphs(self, graph_name: str = None):
        return self._graph_bulk_operation(
            operation=instance_management.reset_graph,
            status_to_check='AVAILABLE',
            graph_name=graph_name
        )

    def _graph_bulk_operation(self, operation: callable, status_to_check: str, graph_name: str = None):
        # Get all graphs matching name filter if specified
        graphs = [graph for graph in self.list_graphs()
                 if graph_name is None or graph['name'] == graph_name]

        if graph_name and len(graphs) == 0:
            logger.warning(f"No graphs found matching name: {graph_name} and status: {status_to_check}")

        # Filter for graphs in correct status, log warning for others
        graph_ids = []
        for graph in graphs:
            if graph['status'] == status_to_check:
                graph_ids.append(graph['id'])
            else:
                logger.warning(f"Skipping graph {graph['id']} - status is {graph['status']}, expected {status_to_check}")

        future_list = []
        for graph_id in graph_ids:
            future_list.append(operation(graph_id))
        return asyncio.gather(*future_list)
