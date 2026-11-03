# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Callable, Optional, Sequence, Union

import boto3
import networkx as nx
from botocore.config import Config
from botocore.exceptions import ClientError

from . import NeptuneGraph, instance_management
from .clients import IamClientWrapper, NeptuneAnalyticsClient
from .clients.client_factory import ClientFactory
from .property_graph import (
    AthenaTableMetadata,
    PropertyGraph,
    property_graph_to_sql,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CleanupTask",
    "SessionManager",
]


# The uuid4 suffix instance_management._create_random_graph_name appends to a
# session prefix: `str(uuid.uuid4())`, i.e. lowercase hex in 8-4-4-4-12 groups.
_UUID4_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _has_running_loop() -> bool:
    """Whether the calling thread is inside a running event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _caller_is_being_cancelled() -> bool:
    """Whether the running task itself has been asked to cancel.

    This separates the two `CancelledError`s that can reach an ``__aexit__``.
    One is a cancellation *of the caller* - ``asyncio.timeout``, a
    ``TaskGroup`` unwinding, a plain ``task.cancel()`` - which must propagate,
    because ``timeout`` and ``TaskGroup`` rely on seeing it to convert it into
    ``TimeoutError`` or to finish unwinding. The other is the `CancelledError`
    that ``gather(return_exceptions=True)`` hands back *as a value* for a
    cancelled child graph operation; that one is a cleanup failure and is
    reported as such, and it must not be mistaken for the caller going away.

    ``Task.cancelling()`` counts outstanding cancellation requests against the
    current task, which is non-zero only in the first case.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _run_sync(coro):
    """Run ``coro`` to completion from synchronous code.

    ``asyncio.run`` raises when a loop is already running, so in that case the
    coroutine runs on its own loop in a worker thread. That bridge blocks the
    caller's loop until cleanup finishes, which also holds Ctrl-C, so async
    callers should use ``async with`` and let ``__aexit__`` await instead -
    ``__exit__`` warns when it has to come through here.
    """
    if not _has_running_loop():
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _belongs_to_session(graph_name, session_name: str) -> bool:
    """Whether ``graph_name`` is a graph of the session called ``session_name``.

    Generated names are exactly ``'{prefix}-{uuid4}'`` (see
    ``instance_management._create_random_graph_name``), so the whole name is
    matched rather than just the prefix. A prefix test is not enough: session
    names in this repo are routinely hyphenated (``reset-demo``,
    ``snapshot-demo``), so ``startswith('demo-')`` would let session ``'demo'``
    claim ``'demo-reset-<uuid>'``, which belongs to session ``'demo-reset'``.
    Cleanup decides whose data gets reset or deleted, so the match has to be
    exact.

    A consequence worth knowing: a graph this session did not create through
    ``instance_management`` - say one hand-named ``'demo-scratch'`` - no longer
    belongs to any session, so cleanup leaves it alone.
    """
    return (
        re.fullmatch(
            rf"{re.escape(session_name)}-{_UUID4_PATTERN}", str(graph_name or "")
        )
        is not None
    )


class CleanupTask(Enum):
    """Enum for cleanup task types."""

    NONE = None
    DESTROY = "destroy"
    RESET = "reset"
    STOP = "stop"


# Graph statuses each operation acts on; any other status is skipped with a warning.
#
# Every task stays AVAILABLE-only, DESTROY included. A STOPPED graph is deletable
# and still bills for storage, so widening DESTROY to it is tempting, but session
# membership is a name-prefix match rather than a record of what this session
# created: a graph someone else stopped *to keep its data* can match the prefix,
# and delete_na_instance clears deletion protection and skips the snapshot. Any
# widening needs an ownership check or an explicit opt-in first.
_ACTIVE_STATUSES = ("AVAILABLE",)

# cleanup task -> (instance_management function to run, `aws neptune-graph` verb,
#                  graph statuses the task acts on)
_CLEANUP_OPERATIONS = {
    CleanupTask.STOP: ("stop_na_instance", "stop-graph", _ACTIVE_STATUSES),
    CleanupTask.RESET: ("reset_graph", "reset-graph", _ACTIVE_STATUSES),
    CleanupTask.DESTROY: ("delete_na_instance", "delete-graph", _ACTIVE_STATUSES),
}


class SessionManager:
    """Manages Neptune Analytics sessions and graph operations.

    Usable as either a synchronous or an asynchronous context manager. Async
    callers should prefer ``async with``, which awaits the exit-time cleanup on
    their own event loop instead of blocking it on a worker thread::

        async with SessionManager.session("demo", CleanupTask.STOP) as session:
            graph = await session.get_or_create_graph()
    """

    def __init__(self, session_name=None, cleanup_task=None):
        """Initialize a SessionManager instance.

        Args:
            session_name (str, optional): Session name. The session uses graphs named
                session_name or '{session_name}-{uuid}'.
            cleanup_task (CleanupTask, optional): Cleanup task to perform on exit. Defaults to None.
              When set to DESTROY, will destroy all instances in the session on exit.
              When set to RESET, will reset all instances in the session on exit.
              When set to STOP, will stop all instances in the session on exit.
              STOP, RESET and DESTROY each require a session_name and raise
              ValueError without one.
        """
        self.session_name = session_name
        self.cleanup_task = cleanup_task or CleanupTask.NONE
        if self.cleanup_task in _CLEANUP_OPERATIONS and not session_name:
            raise ValueError(
                f"{self.cleanup_task} requires a session_name: without one the cleanup "
                "would target every Neptune Analytics graph in the account/region."
            )
        self._clients = ClientFactory()
        self._neptune_client = self._clients.neptune()
        self._sts_client = self._clients.sts()
        self._s3_iam_role = self._sts_client.get_caller_identity()["Arn"]
        self._iam_client = self._clients.iam()
        self._s3_client = self._clients.s3()
        self._athena_client = self._clients.athena()

    @classmethod
    def session(cls, session_name=None, cleanup_task=None):
        """Create a new SessionManager instance.

        Args:
            session_name (str, optional): Session name. The session uses graphs named
                session_name or '{session_name}-{uuid}'.
            cleanup_task (CleanupTask, optional): Cleanup task to perform on exit. Defaults to None.

        Returns:
            SessionManager: A new SessionManager instance.
        """
        return cls(session_name, cleanup_task)

    def __enter__(self):
        """Enter the session manager.

        In async code prefer ``async with``, which cleans up without blocking
        the caller's event loop. See :meth:`__exit__`.
        """

        # TODO: consider creating graphs here
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and clean up graphs.

        A ``with`` block is the only cleanup hook: a ``SessionManager`` used
        without one never cleans up its graphs automatically.

        Exit runs the configured cleanup task (STOP, RESET or DESTROY) over every
        ``AVAILABLE`` graph in the session and waits for each to reach its
        terminal state, so exiting the block can take minutes. Graphs in any
        other status, including ``STOPPED``, are skipped with a warning.

        ``async with`` is the supported path in async code. Cleanup is async, so
        reaching it from this synchronous hook inside a running loop means
        running it on a worker thread and blocking the caller's loop until it
        finishes - which also delays Ctrl-C for the whole cleanup. That case is
        warned about rather than refused, because refusing it would leak the
        running, billed graphs the cleanup exists to release.

        A cleanup failure is never allowed to replace an exception raised by the
        ``with`` body: that error is the one the caller needs to see, so a failed
        cleanup is logged instead and the original exception propagates.
        """

        if self.cleanup_task not in _CLEANUP_OPERATIONS:
            return
        if _has_running_loop():
            logger.warning(
                f"Session cleanup ({self.cleanup_task.value}) is running through "
                f"the synchronous `with` path inside a running event loop. It will "
                f"block this loop until every graph finishes, and Ctrl-C will not "
                f"be delivered until then. Use `async with SessionManager.session("
                f"...)` in async code, including notebook cells."
            )
        try:
            _run_sync(self._cleanup_coro())
        except BaseException as e:  # noqa: BLE001 - see _handle_cleanup_failure
            self._handle_cleanup_failure(e, exc_type)

    async def __aenter__(self):
        """Enter the session manager from async code.

        The async counterpart of :meth:`__enter__`, and the supported path for
        async callers: every ``SessionManager`` create method is ``async``, so
        this is the usual case.
        """

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager from async code and clean up graphs.

        Same cleanup as :meth:`__exit__`, awaited directly on the caller's loop
        rather than bridged to a worker thread, so a Ctrl-C during a
        minutes-long cleanup is delivered when it arrives instead of when the
        cleanup ends, and a cancellation of the calling task reaches the caller.

        This removes the thread bridge, not every stall: the underlying
        ``boto3`` calls are synchronous, so ``list_graphs``, the per-graph
        status checks, the IAM permission checks and each stop/reset/delete
        still occupy the loop for as long as the AWS call takes. Other tasks
        make progress between those calls, not during them. Moving them to
        ``asyncio.to_thread`` is a separate change.
        """

        if self.cleanup_task not in _CLEANUP_OPERATIONS:
            return
        try:
            await self._cleanup_coro()
        except BaseException as e:  # noqa: BLE001 - see _handle_cleanup_failure
            if isinstance(e, asyncio.CancelledError) and _caller_is_being_cancelled():
                raise
            self._handle_cleanup_failure(e, exc_type)

    def _cleanup_coro(self):
        """The configured cleanup task, as an un-awaited coroutine."""
        operation_name, _, statuses = _CLEANUP_OPERATIONS[self.cleanup_task]
        return self._await_cleanup_all_graphs(operation_name, statuses)

    def _handle_cleanup_failure(self, error: BaseException, exc_type):
        """Report a failed cleanup, and decide whether it may propagate.

        ``BaseException`` is deliberate at the call sites: it pairs with the
        ``BaseException`` filter in ``_await_cleanup_all_graphs``, since
        ``gather(return_exceptions=True)`` returns e.g. ``CancelledError`` as a
        value that is then re-raised. Catching only ``Exception`` would let it
        replace the body's exception.

        ``KeyboardInterrupt`` and ``SystemExit`` are carved out: they are
        control-flow signals (e.g. Ctrl-C during a minutes-long cleanup), not
        cleanup failures, so they propagate even when the body already raised.
        """
        _, verb, _ = _CLEANUP_OPERATIONS[self.cleanup_task]
        logger.warning(
            f"Session cleanup ({self.cleanup_task.value}) failed: {error!r}. Affected "
            f"graphs may still be running and billed; list them with "
            f"`aws neptune-graph list-graphs` and retry each with "
            f"`aws neptune-graph {verb} --graph-identifier <id>`"
        )
        if exc_type is None or isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error

    async def _await_cleanup_all_graphs(
        self, operation_name: str, statuses: Sequence[str]
    ):
        """Run an instance_management operation on every graph, to completion.

        The gather is built *and* awaited inside the running loop. Every graph
        is attempted even if another fails (``return_exceptions=True``, kept on
        this private path so the public bulk methods still raise); failures are
        then logged per graph and the first one is raised.
        """
        operation = getattr(instance_management, operation_name)
        graph_ids = self._select_graph_ids(statuses, [])
        results = await asyncio.gather(
            *[operation(graph_id) for graph_id in graph_ids], return_exceptions=True
        )
        failures = [
            (graph_id, result)
            for graph_id, result in zip(graph_ids, results)
            if isinstance(result, BaseException)
        ]
        for graph_id, error in failures:
            # repr, not str: str(CancelledError()) is empty, which rendered as
            # "failed for graph g-1: " and named no cause at all.
            logger.warning(f"Session cleanup failed for graph {graph_id}: {error!r}")
        if failures:
            raise failures[0][1]

    def validate_permissions(self):
        """Validate AWS permissions for Neptune Analytics operations.

        Returns:
            bool: True if permissions are valid, False otherwise.
        """
        return instance_management.validate_permissions()

    def list_graphs(self) -> list[NeptuneAnalyticsClient]:
        """List available Neptune Analytics graphs.

        If session_name is set, filters graphs to this session's graphs, i.e.
        those named ``'{session_name}-...'`` - see :func:`_belongs_to_session`.

        Returns:
            list: List of NeptuneAnalyticsClient objects containing graph metadata.
        """
        response = self._neptune_client.list_graphs()
        graphs = response.get("graphs", [])

        if self.session_name:
            graphs = [
                NeptuneAnalyticsClient.from_response(g, self._neptune_client)
                for g in graphs
                if _belongs_to_session(g.get("name"), self.session_name)
            ]
        else:
            graphs = [
                NeptuneAnalyticsClient.from_response(g, self._neptune_client)
                for g in graphs
            ]

        return graphs

    def _get_existing_graph(
        self, filter_status: Optional[list[str]] = None
    ) -> Optional[NeptuneAnalyticsClient]:
        """Get the first existing graph, optionally filtered by status.

        Args:
            filter_status: Optional list of status values to filter by (case-insensitive).
                          If None, returns the first graph regardless of status.

        Returns:
            NeptuneAnalyticsClient or None: Graph details if found, None otherwise.
        """
        graphs = self.list_graphs()
        if not graphs:
            return None

        if filter_status is None:
            return graphs[0]

        filter_status_lower = [s.lower() for s in filter_status]
        for graph in graphs:
            if graph.status is not None and graph.status.lower() in filter_status_lower:
                return graph
        return None

    def get_graph(self, graph_id: str) -> NeptuneAnalyticsClient:
        """Get details for a specific graph by ID.

        Args:
            graph_id (str): ID of the graph to retrieve

        Returns:
            NeptuneAnalyticsClient: Graph, with details, if found

        Raises:
            Exception: If no graph is found with the given ID
        """
        graphs = self.list_graphs()

        for graph in graphs:
            if graph.graph_id == graph_id:
                return graph

        # Package that as nx object
        raise Exception(f"No graph instance with id {graph_id} found")

    async def get_or_create_graph(
        self, config: Optional[dict] = None
    ) -> NeptuneAnalyticsClient:
        """Get the first available graph or create a new one if none exist.

        Returns:
            NeptuneAnalyticsClient: graph object for an available instance
        """
        graph = self._get_existing_graph(filter_status=["AVAILABLE"])
        if graph:
            return graph
        return await self.create_graph(config)

    async def create_graph(
        self,
        config: Optional[dict] = None,
        on_created: Optional[Callable[[str], None]] = None,
    ) -> NeptuneAnalyticsClient:
        """Create a new graph named with the session prefix.

        Unlike get_or_create_graph, this never reuses an existing graph.

        Args:
            config (Optional[dict]): Graph creation settings, e.g. provisionedMemory.
            on_created (Optional[Callable[[str], None]]): Called with the graph ID as
                soon as creation is accepted, before waiting for the graph to become
                available.

        Returns:
            NeptuneAnalyticsClient: graph object for the new instance
        """
        logger.info(f"Creating new graph named with prefix: {self.session_name}")
        graph_id = await instance_management.create_na_instance(
            config=config,
            na_client=self._neptune_client,
            sts_client=self._sts_client,
            iam_client=self._iam_client,
            graph_name_prefix=self.session_name,
            on_created=on_created,
        )
        return self.get_graph(graph_id)

    async def create_from_snapshot(
        self,
        snapshot_id: str,
        config: Optional[dict] = None,
    ) -> NeptuneAnalyticsClient:
        """Create a new Neptune Analytics instance from a snapshot.

        Args:
            snapshot_id (str): Name of the snapshot to create instance from.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            NeptuneAnalyticsClient: graph with details
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
    ) -> NeptuneAnalyticsClient:
        """Create a new Neptune Analytics instance from a s3 bucket location with CSV data.

        Args:
            s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            NeptuneAnalyticsClient: graph with details
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
        graph: NeptuneAnalyticsClient,
        s3_location,
        export_filter=None,
    ) -> str:
        """Export Neptune Analytics graph data to CSV files in S3.

        Args:
            graph (NeptuneAnalyticsClient): Graph to export
            s3_location (str): S3 location to store exported CSV files.
            export_filter (dict, optional): Filter criteria for the export. Defaults to None.

        Returns:
            str: Task ID of the export operation.
        """

        return await instance_management.export_csv_to_s3(
            NeptuneGraph(
                graph,
                IamClientWrapper(self._s3_iam_role, self._iam_client),
                nx.Graph(),
            ),
            s3_location,
            export_filter=export_filter,
        )

    async def import_from_csv(
        self,
        graph: NeptuneAnalyticsClient,
        s3_location,
        reset_graph_ahead=False,
        max_size: Optional[int] = None,
    ) -> str:
        """Import CSV data from S3 into a Neptune Analytics graph.

        Args:
            graph (NeptuneAnalyticsClient): Graph to import
            s3_location (str): S3 location containing CSV data to import.
            reset_graph_ahead (bool, optional): Whether to reset the graph before import. Defaults to False.
            max_size (int, optional): If defined, maximum memory size in GB to scale up to. Defaults to None.

        Returns:
            str: Task ID of the import operation.

        Raises:
            ClientError: If import fails due to insufficient memory and max_size is exceeded, or other AWS client errors.
        """

        skip_snapshot = True
        while True:
            try:
                return await instance_management.import_csv_from_s3(
                    NeptuneGraph(
                        graph,
                        IamClientWrapper(self._s3_iam_role, self._iam_client),
                        nx.Graph(),
                    ),
                    s3_location,
                    reset_graph_ahead,
                    skip_snapshot,
                )
            except ClientError as e:
                current_size = self.get_graph(graph.graph_id).details[
                    "provisionedMemory"
                ]
                if (
                    max_size is not None
                    and e.response["Error"]["Code"] == "InsufficientMemory"
                    and max_size > current_size
                ):
                    prospect_size = current_size * 2

                    if prospect_size > max_size > current_size:
                        prospect_size = max_size

                    await instance_management.update_na_instance_size(
                        graph_id=graph.graph_id, prospect_size=prospect_size
                    )
                    continue
                else:
                    raise e

    async def import_from_table(
        self,
        graph: NeptuneAnalyticsClient,
        s3_location,
        sql_queries,
        sql_parameters=None,
        catalog=None,
        database=None,
        remove_buckets=True,
    ) -> str:
        """Import data from Athena table query results into a Neptune Analytics graph.

        Args:
            graph (NeptuneAnalyticsClient): Graph to import
            s3_location (str): S3 location to store intermediate CSV data.
            sql_queries (list): List of SQL queries to execute against Athena tables.
            sql_parameters (list[list], optional): 2D List of execution parameters to pass with each SQL query.
            catalog (str, optional): Athena catalog name. Defaults to None.
            database (str, optional): Athena database name. Defaults to None.
            remove_buckets (bool): After a successful import, delete the S3 bucket contents if True.

        Returns:
            str: Graph ID of the target graph.
        """
        logger.info(f"Importing to graph {graph.graph_id}")

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
                graph,
                IamClientWrapper(self._s3_iam_role, self._iam_client),
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

        logger.info(
            f"Graph data imported to graph {graph.graph_id} using task {task_id}"
        )
        return task_id

    async def import_from_graph_schema(
        self,
        graph: NeptuneAnalyticsClient,
        s3_location: str,
        property_graph: Union[str, PropertyGraph],
        catalog=None,
        database=None,
        remove_buckets=True,
    ) -> str:
        """Import data described by a ``CREATE PROPERTY GRAPH`` statement.

        A schema-first front-end over :meth:`import_from_table`: instead of
        hand-writing the Athena ``SELECT`` projections that alias source columns
        into Neptune's ``~id``/``~label``/``~from``/``~to`` load format, declare
        the mapping with the standard SQL/PGQ (SQL:2023) ``CREATE PROPERTY
        GRAPH`` DDL. Each source table's columns and types are read from the
        Athena catalog (``athena:GetTableMetadata``, plus ``glue:GetTable`` for
        Glue catalogs) to resolve the DDL into projection queries, which are
        imported through the unchanged Athena -> S3 -> Neptune path.

        Args:
            graph (NeptuneAnalyticsClient): Graph to import into.
            s3_location (str): S3 location to store intermediate CSV data.
            property_graph: A ``CREATE PROPERTY GRAPH`` statement, or its parsed
                :class:`PropertyGraph`.
            catalog (str, optional): Athena catalog for unqualified tables.
                Defaults to AwsDataCatalog.
            database (str, optional): Athena database for unqualified tables.
            remove_buckets (bool): Delete intermediate S3 CSV data after a
                successful import if True.

        Returns:
            str: Task ID of the import operation.

        Raises:
            PropertyGraphSyntaxError: If the DDL cannot be parsed.
            PropertyGraphSchemaError: If the DDL does not match the source tables.
        """
        metadata = AthenaTableMetadata(
            self._athena_client, catalog=catalog, database=database
        )
        sql_queries = property_graph_to_sql(property_graph, metadata)
        logger.info(
            f"Translated property graph into {len(sql_queries)} projection queries"
        )
        return await self.import_from_table(
            graph,
            s3_location,
            sql_queries,
            catalog=catalog,
            database=database,
            remove_buckets=remove_buckets,
        )

    async def export_to_table(
        self,
        graph: NeptuneAnalyticsClient,
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
            graph (NeptuneAnalyticsClient): Graph to export.
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
        logger.info(f"Exporting graph: {graph.graph_id}")

        task_id = await instance_management.export_csv_to_s3(
            NeptuneGraph(
                graph,
                IamClientWrapper(self._s3_iam_role, self._iam_client),
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

    async def create_snapshot(self, graph: NeptuneAnalyticsClient, snapshot_name: str):
        """Create a Neptune Analytics graph snapshot.

        Args:
            graph (NeptuneAnalyticsClient): Graph to create snapshot
            snapshot_name (str): Name of the snapshot to create instances from.
            config (Optional[dict]): Optional configuration to pass to each instance creation.

        Returns:
            snapshot_id that was created
        """
        logger.info(f"Creating snapshot: {snapshot_name}")
        snapshot_id = await instance_management.create_graph_snapshot(
            graph.graph_id,
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

        Deletes graphs in ``AVAILABLE`` status; graphs in any other status,
        ``STOPPED`` included, are skipped with a warning. This operation cannot
        be undone.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to destroy

        Returns:
            asyncio.Future: A future that resolves when the graphs have been deleted.
        """
        return self._destroy_graphs(graph_name)

    def destroy_all_graphs(self):
        """Delete all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and permanently deletes each graph instance.
        This operation cannot be undone. Only graphs in ``AVAILABLE`` status are
        deleted; graphs in any other status, ``STOPPED`` included, are skipped
        with a warning.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been deleted.
        """
        return self._destroy_graphs([])

    def start_graph(self, graph_name: Union[str, list[str]]):
        """Start one or more Neptune Analytics graphs.

        Args:
            graph_name (Union[str, list[str]]): Name or list of names of graphs to start

        Returns:
            asyncio.Future: A future that resolves when the graphs have been started.
        """
        return self._start_graphs(graph_name)

    def start_all_graphs(self):
        """Start all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and starts each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been started.
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
            graph_name (Union[str, list[str]]): Name or list of names of graphs to reset

        Returns:
            asyncio.Future: A future that resolves when the graphs have been reset.
        """
        return self._reset_graphs(graph_name)

    def reset_all_graphs(self):
        """Reset all Neptune Analytics graphs associated with this session.

        Fetches all graph IDs for the current session and resets each graph instance.

        Returns:
            asyncio.Future: A future that resolves when all graphs have been reset.
        """
        return self._reset_graphs([])

    def _destroy_graphs(self, graph_name: Union[str, list[str]]):
        if isinstance(graph_name, str):
            graph_name = [graph_name]
        return self._graph_bulk_operation(
            operation=instance_management.delete_na_instance,
            status_to_check=_ACTIVE_STATUSES,
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
        self,
        operation: Callable,
        status_to_check: Union[str, Sequence[str]],
        graph_names: list[str],
    ):
        graph_ids = self._select_graph_ids(status_to_check, graph_names)
        return asyncio.gather(*[operation(graph_id) for graph_id in graph_ids])

    def _select_graph_ids(
        self, status_to_check: Union[str, Sequence[str]], graph_names: list[str]
    ):
        if isinstance(status_to_check, str):
            status_to_check = (status_to_check,)
        expected = " or ".join(status_to_check)
        # Guard: an empty graph_names means "all graphs in this session", which
        # relies on session_name to scope list_graphs(). With no session_name,
        # list_graphs() returns every graph in the account/region, so a bulk
        # "*_all_graphs" call would operate on unrelated graphs. Refuse it.
        if not graph_names and not self.session_name:
            raise ValueError(
                "Refusing a bulk operation over all graphs: this session has no "
                "session_name, so an empty selection would match every Neptune "
                "Analytics graph in the account/region. Create the session with a "
                "session_name to scope it, or pass explicit graph names."
            )

        # Get all graphs matching name filter if specified
        graphs = [
            graph
            for graph in self.list_graphs()
            if len(graph_names) == 0 or graph.name in graph_names
        ]
        if graph_names and len(graphs) == 0:
            logger.warning(
                f"No graphs found matching name: {graph_names} and status: {expected}"
            )

        # Filter for graphs in correct status, log warning for others
        graph_ids = []
        for graph in graphs:
            if graph.status in status_to_check:
                graph_ids.append(graph.graph_id)
            else:
                logger.warning(
                    f"Skipping graph {graph.graph_id} - status is {graph.status}, expected {expected}"
                )

        return graph_ids
