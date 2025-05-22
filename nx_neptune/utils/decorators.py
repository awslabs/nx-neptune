import asyncio
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

            logger.debug(f"configure_if_nx_active: {func.__name__}")
            graph = args[0]

            neptune_config = get_config()
            neptune_config.validate_config()

            # Execute setup instructions
            if neptune_config.graph_id is not None:
                na_graph = NeptuneGraph.from_config(
                    config=neptune_config, graph=graph, logger=logger
                )
                neptune_config = asyncio.run(
                    _execute_setup_routines_on_graph(na_graph, neptune_config)
                )

            elif neptune_config.create_new_instance:

                neptune_config = asyncio.run(
                    _execute_setup_new_graph(neptune_config, graph)
                )
                na_graph = NeptuneGraph.from_config(
                    config=neptune_config, graph=graph, logger=logger
                )

            _sync_data_to_neptune(graph, na_graph)

            converted_args = (na_graph,) + args[1:]

            # Call algorithm
            rv = func(*converted_args, **kwargs)

            # Execute teardown instructions
            if neptune_config.graph_id is not None:
                asyncio.run(
                    _execute_teardown_routines_on_graph(na_graph, neptune_config)
                )
            return rv

        return wrapper

    return decorator


async def _execute_setup_routines_on_graph(
    na_graph: NeptuneGraph, neptune_config: NeptuneConfig, *args, **kwargs
) -> NeptuneGraph:
    # Restore graph data from S3
    if neptune_config.import_s3_bucket is not None:
        logger.debug(f"Restore graph data from S3: {neptune_config.import_s3_bucket}")
        await import_csv_from_s3(
            na_graph,
            neptune_config.import_s3_bucket,
            (
                neptune_config.skip_graph_reset
                if hasattr(neptune_config, "skip_graph_reset")
                else True
            ),
        )

    # Restore graph data from a snapshot
    if neptune_config.restore_snapshot is not None:
        # TODO
        logger.debug("Restore graph data from snapshot")
        raise Exception("Not implemented yet (workflow: restore_snapshot)")

    return neptune_config


async def _execute_setup_new_graph(
    neptune_config: NeptuneConfig, graph: networkx.Graph, *args, **kwargs
) -> NeptuneGraph:
    if neptune_config.import_s3_bucket is not None:
        # TODO: update this to do everything in one shot

        logger.debug("Create empty instance")
        graph_id = await create_na_instance()

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
        graph_id = await create_na_instance()

        # once done: save the graph id and update the config
        neptune_config = set_config_graph_id(graph_id)
        logger.debug(f"Instance created: {graph_id}")

    return neptune_config


def _sync_data_to_neptune(graph: networkx.Graph, neptune_graph: NeptuneGraph):
    logger.debug(
        f"Sync data to instance: nodes:{len(graph.nodes())}, edges:{len(graph.edges())}"
    )

    """
    Push all Nodes from NetworkX into Neptune Analytics
    """
    for node in graph.nodes().data():
        na_node = Node.convert_from_nx(node)
        logger.debug(f"add_node={na_node}")
        neptune_graph.add_node(na_node)

    """
    Push all Edges from NetworkX into Neptune Analytics
    """
    for edge in graph.edges().data():
        na_edge = Edge.convert_from_nx(edge)
        logger.debug(f"add_edge={na_edge}")
        neptune_graph.add_edge(na_edge)

        # Push the reverse direction edge if the graph is undirected
        if not graph.is_directed():
            na_reverse_edge = Edge.convert_from_nx(edge).to_reverse_edge()
            logger.debug(f"add_edge={na_reverse_edge}")
            neptune_graph.add_edge(na_reverse_edge)

    return neptune_graph


async def _execute_teardown_routines_on_graph(
    na_graph: NeptuneGraph, neptune_config: NeptuneConfig, *args, **kwargs
) -> NeptuneGraph:
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
            logger.debug(f"Instance destroyed: {neptune_config.graph_id}")

    return neptune_config
