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

            neptune_config = get_config()
            neptune_config.validate_config()

            # Execute setup instructions
            if neptune_config.graph_id is not None:
                na_graph = NeptuneGraph.from_config(
                    config=neptune_config, graph=graph, logger=logger
                )
                if loop and loop.is_running():
                    pool = concurrent.futures.ThreadPoolExecutor()
                    neptune_config = pool.submit(
                        asyncio.run,
                        _execute_setup_routines_on_graph(na_graph, neptune_config),
                    ).result()
                else:
                    neptune_config = asyncio.run(
                        _execute_setup_routines_on_graph(na_graph, neptune_config)
                    )

            elif neptune_config.create_new_instance is True or isinstance(
                neptune_config.create_new_instance, dict
            ):
                if loop and loop.is_running():
                    pool = concurrent.futures.ThreadPoolExecutor()
                    neptune_config = pool.submit(
                        asyncio.run, _execute_setup_new_graph(neptune_config, graph)
                    ).result()
                else:
                    neptune_config = asyncio.run(
                        _execute_setup_new_graph(neptune_config, graph)
                    )

                na_graph = NeptuneGraph.from_config(
                    config=neptune_config, graph=graph, logger=logger
                )

            _sync_data_to_neptune(graph, na_graph, neptune_config)

            converted_args = (na_graph,) + args[1:]

            # Call algorithm
            rv = func(*converted_args, **kwargs)

            # Execute teardown instructions
            if neptune_config.graph_id is not None:
                if loop and loop.is_running():
                    pool = concurrent.futures.ThreadPoolExecutor()
                    pool.submit(
                        asyncio.run,
                        _execute_teardown_routines_on_graph(na_graph, neptune_config),
                    ).result()
                else:
                    asyncio.run(
                        _execute_teardown_routines_on_graph(na_graph, neptune_config)
                    )

            return rv

        return wrapper

    return decorator


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
    neptune_config: NeptuneConfig, graph: networkx.Graph, *args, **kwargs
) -> NeptuneConfig:
    if neptune_config.import_s3_bucket is not None:
        # TODO: update this to do everything in one shot

        logger.debug("Create empty instance")
        config = neptune_config.create_new_instance
        graph_id = await create_na_instance(
            config if isinstance(config, dict) else None
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
        graph_id = await create_na_instance(
            config if isinstance(config, dict) else None
        )

        # once done: save the graph id and update the config
        neptune_config = set_config_graph_id(graph_id)
        logger.debug(f"Instance created: {graph_id}")

    return neptune_config


def _sync_data_to_neptune(
    graph: networkx.Graph, neptune_graph: NeptuneGraph, neptune_config: NeptuneConfig
):
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
            await delete_na_instance(neptune_config.graph_id)
            # clear the graph id
            neptune_config = set_config_graph_id(None)
            logger.debug("Instance destroyed")

    return neptune_config
