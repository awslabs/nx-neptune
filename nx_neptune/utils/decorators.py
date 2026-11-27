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
import asyncio
import concurrent.futures
from collections.abc import Callable
import logging
import os
from functools import wraps

import networkx

__all__ = ["configure_if_nx_active"]

from nx_plugin import NeptuneConfig

from ..clients import Edge, Node
from ..instance_management import (
    create_na_instance,
    delete_na_instance,
    export_csv_to_s3,
    import_csv_from_s3,
)
from ..na_graph import NeptuneGraph, get_config, set_config_graph_id

logger = logging.getLogger(__name__)


def configure_if_nx_active():
    """
    Decorator to set the configuration for the connection to Neptune Analytics within nx_neptune.
    Calls any setup or teardown routines assigned in the configuration.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            if "NX_ALGORITHM_TEST" in os.environ:
                return func(*args, **kwargs)

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            logger.debug(f"configure_if_nx_active: {func.__name__}")
            graph = args[0]
            # The graph *this* call created, or None if it reused one. Held as an
            # id rather than a bool: the config is a single process-global object
            # (`networkx.config.backends.neptune`) that `set_config_graph_id`
            # overwrites, so by the time a failure lands the global may name a
            # graph belonging to another call. Deleting whatever it holds then
            # destroys someone else's graph and leaks this one. A bool can say
            # "this call created something" but not *which* something.
            created_graph_id: str | None = None

            neptune_config = get_config()
            neptune_config.validate_config()

            try:
                # Execute setup instructions
                if neptune_config.graph_id is not None:
                    na_graph = NeptuneGraph.from_config(
                        config=neptune_config, graph=graph, logger=logger
                    )
                    neptune_config = _run_coroutine(
                        _execute_setup_routines_on_graph(na_graph, neptune_config),
                        loop,
                    )

                elif neptune_config.create_new_instance is True or isinstance(
                    neptune_config.create_new_instance, dict
                ):

                    def _record_created(graph_id: str) -> None:
                        # Captured the instant the graph exists, so a setup step
                        # that fails after creation (the S3 import, say) still
                        # leaves the id in hand for the teardown below.
                        nonlocal created_graph_id
                        created_graph_id = graph_id

                    neptune_config = _run_coroutine(
                        _execute_setup_new_graph(
                            neptune_config, graph, on_created=_record_created
                        ),
                        loop,
                    )

                    na_graph = NeptuneGraph.from_config(
                        config=neptune_config, graph=graph, logger=logger
                    )

                _sync_data_to_neptune(graph, na_graph, neptune_config)

                converted_args = (na_graph,) + args[1:]

                # Call algorithm
                rv = func(*converted_args, **kwargs)

                # Inside the `try`, deliberately, not in an `else`. The success
                # teardown can itself raise — `export_csv_to_s3` can fail, and
                # `save_snapshot` or `reset_graph` raise "Not implemented yet"
                # *before* the destroy step is reached. From an `else` that
                # exception escaped the handler below, leaving the instance this
                # call created running and billed: the very leak this path exists
                # to close.
                if neptune_config.graph_id is not None:
                    _run_coroutine(
                        _execute_teardown_routines_on_graph(na_graph, neptune_config),
                        loop,
                    )
                return rv
            except (KeyboardInterrupt, SystemExit):
                # Control flow, not a failure: run no teardown, so a Ctrl-C is
                # not turned into a delete. But name the graph on the way out —
                # interrupting a long call is how a notebook user abandons one,
                # and while the id survives in the global config for the rest of
                # the session, restarting the kernel is the usual next step and
                # loses it. After that nothing left running can delete the graph,
                # so this log line is the only record the user has.
                if created_graph_id is not None:
                    logger.warning(
                        "Interrupted after creating graph %s; it was left running "
                        "and is billed. Delete it by hand if it is not wanted.",
                        created_graph_id,
                    )
                raise
            except BaseException:
                _teardown_after_failure(created_graph_id, loop)
                raise

        return wrapper

    return decorator


def _run_coroutine(coro, loop):
    """Run a coroutine to completion, from inside a running loop or not."""
    if loop and loop.is_running():
        # asyncio.run cannot nest in a running loop, so use a worker thread.
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            return pool.submit(asyncio.run, coro).result()
        finally:
            pool.shutdown()
    return asyncio.run(coro)


def _teardown_after_failure(created_graph_id: str | None, loop) -> None:
    """Run the failure teardown, never letting it replace the caller's error.

    Extracted from the exception handler so the policy — swallow, log, and let
    the original exception propagate — is stated in one place rather than nested
    two levels inside the wrapper, where a later edit could mask the error the
    caller actually needs.
    """
    try:
        _run_coroutine(_execute_failure_teardown(created_graph_id, get_config()), loop)
    except (KeyboardInterrupt, SystemExit):
        # An interrupt is the user's instruction, not a teardown failure. It was
        # previously swallowed here, which contradicted the wrapper's own policy
        # of never turning a Ctrl-C into a delete: the user pressed it and
        # nothing happened.
        raise
    except Exception:
        logger.error(
            "Teardown after a failed call (graph %s) may not have completed; "
            "the instance may still be running and billed",
            created_graph_id,
            exc_info=True,
        )


async def _execute_failure_teardown(
    created_graph_id: str | None, neptune_config: NeptuneConfig
) -> None:
    """Release what this call created, after the algorithm raised.

    Deliberately narrower than the success teardown, so that a failure cannot
    destroy data:

    - the S3 export is skipped, so a partial export cannot overwrite the last
      good one;
    - only ``created_graph_id`` is destroyed — the graph this call created, and
      never the one the config happens to name now. A caller-supplied
      ``graph_id`` is never deleted because an algorithm raised, and neither is
      a concurrent call's graph.

    ``neptune_config`` is read for ``destroy_instance`` only, which is a setting
    rather than an identity, so the global is the right source for it.
    """
    if created_graph_id is None:
        return
    if not neptune_config.destroy_instance:
        return

    logger.debug(f"Destroy instance created by this call: {created_graph_id}")
    await _destroy_instance(created_graph_id)


async def _destroy_instance(graph_id: str) -> NeptuneConfig:
    """Delete the instance and clear the stored graph id."""
    await delete_na_instance(graph_id)
    config = set_config_graph_id(None)
    logger.debug("Instance destroyed")
    return config


async def _execute_setup_routines_on_graph(
    na_graph: NeptuneGraph, neptune_config: NeptuneConfig, *args, **kwargs
) -> NeptuneConfig:
    # Restore graph data from S3
    if neptune_config.import_s3_bucket is not None:
        logger.debug(f"Restore graph data from S3: {neptune_config.import_s3_bucket}")
        await import_csv_from_s3(
            na_graph,
            neptune_config.import_s3_bucket,
            neptune_config.skip_graph_reset,
        )

    # Restore graph data from a snapshot
    if neptune_config.restore_snapshot is not None:
        # TODO
        logger.debug("Restore graph data from snapshot")
        raise Exception("Not implemented yet (workflow: restore_snapshot)")

    return neptune_config


async def _execute_setup_new_graph(
    neptune_config: NeptuneConfig,
    graph: networkx.Graph,
    *args,
    on_created: Callable[[str], None] | None = None,
    **kwargs,
) -> NeptuneConfig:
    """Create a graph and load it.

    ``on_created`` is called with the new graph id the moment creation returns,
    before anything that could fail afterwards. The caller needs the id even
    when this function raises — an import that fails after the graph exists
    still leaves it running and billed — and it must not have to recover the id
    from the process-global config, which another call may have overwritten.
    """
    if neptune_config.import_s3_bucket is not None:
        # TODO: update this to do everything in one shot

        logger.debug("Create empty instance")
        config = neptune_config.create_new_instance
        # Passed down, not just applied to the return value: create_na_instance
        # reports the id as soon as creation is *accepted* and then waits for the
        # graph to become available. If that wait throws, the graph exists in AWS
        # but this call never returns, so an id captured only here would miss the
        # one case that leaves a graph nobody can name.
        graph_id = await create_na_instance(
            config if isinstance(config, dict) else None,
            on_created=on_created,
        )

        # once done: save the graph id and update the config
        neptune_config = set_config_graph_id(graph_id)
        logger.debug(f"Instance created: {graph_id}")

        na_graph = NeptuneGraph.from_config(
            config=neptune_config, graph=graph, logger=logger
        )

        logger.debug(f"Restore graph data from S3: {neptune_config.import_s3_bucket}")
        await import_csv_from_s3(
            na_graph,
            neptune_config.import_s3_bucket,
        )

    elif neptune_config.restore_snapshot:
        # TODO
        graph_id = "g-restore_snapshot"
        logger.debug("Create graph from snapshot: " + neptune_config.restore_snapshot)
        raise Exception(
            "Not implemented yet (workflow: create_new_instance w/ restore_snapshot)"
        )
    else:
        logger.debug("Create empty instance")
        config = neptune_config.create_new_instance
        # Passed down, not just applied to the return value: create_na_instance
        # reports the id as soon as creation is *accepted* and then waits for the
        # graph to become available. If that wait throws, the graph exists in AWS
        # but this call never returns, so an id captured only here would miss the
        # one case that leaves a graph nobody can name.
        graph_id = await create_na_instance(
            config if isinstance(config, dict) else None,
            on_created=on_created,
        )

        # once done: save the graph id and update the config
        neptune_config = set_config_graph_id(graph_id)
        logger.debug(f"Instance created: {graph_id}")

    return neptune_config


def _sync_data_to_neptune(
    graph: networkx.Graph, neptune_graph: NeptuneGraph, neptune_config: NeptuneConfig
):
    if not graph.nodes and not graph.edges:
        logger.debug("Graph is empty")
        return neptune_graph

    logger.debug(
        f"Sync data to instance: nodes:{len(graph.nodes())}, edges:{len(graph.edges())}"
    )

    batch_size_node = neptune_config.batch_update_node_size
    batch_size_edge = neptune_config.batch_update_edge_size

    if not neptune_config.skip_graph_reset:
        neptune_graph.clear_graph()

    """
    Push all Nodes from NetworkX into Neptune Analytics
    """
    nodes = [Node.convert_from_nx((n, d)) for n, d in graph.nodes(data=True)]
    for i in range(0, len(nodes), batch_size_node):
        last_node_pos = (
            len(nodes) if (i + batch_size_node) > len(nodes) else i + batch_size_node
        )
        logger.debug(f"Adding nodes[{i} - {last_node_pos}]")
        batch = nodes[i:last_node_pos]
        neptune_graph.add_nodes(batch)

    """
    Push all Edges from NetworkX into Neptune Analytics
    """
    edges = [
        Edge.convert_from_nx(edge=edge, is_directed=graph.is_directed())
        for edge in graph.edges(data=True)
    ]
    for i in range(0, len(edges), batch_size_edge):
        last_edge_pos = (
            len(edges) if (i + batch_size_edge) > len(edges) else i + batch_size_edge
        )
        logger.debug(f"Adding edges[{i} - {last_edge_pos}]")
        batch = edges[i:last_edge_pos]
        neptune_graph.add_edges(batch)

    return neptune_graph


async def _execute_teardown_routines_on_graph(
    na_graph: NeptuneGraph, neptune_config: NeptuneConfig, *args, **kwargs
) -> NeptuneConfig:
    if neptune_config.graph_id is not None:
        if neptune_config.export_s3_bucket is not None:
            logger.debug("Export graph data to S3: " + neptune_config.export_s3_bucket)
            await export_csv_to_s3(na_graph, neptune_config.export_s3_bucket)

        if neptune_config.save_snapshot:
            logger.debug("Export graph to snapshot")
            raise Exception("Not implemented yet (workflow: save_snapshot)")

        if neptune_config.reset_graph:
            logger.debug("Reset graph")
            raise Exception("Not implemented yet (workflow: reset_graph)")

        if neptune_config.destroy_instance:
            logger.debug(f"Destroy instance {neptune_config.graph_id}")
            neptune_config = await _destroy_instance(neptune_config.graph_id)

    return neptune_config
