import asyncio
import logging
import os
import sys

import networkx as nx

from nx_neptune.instance_management import create_na_instance, import_csv_from_s3

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of a remote Neptune Analytics resources with create.
TODO: destroy instance 
"""


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout  # Explicitly set output to stdout
    )
    for logger_name in ['nx_neptune.instance_management', 'nx_neptune.na_graph']:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
    logger = logging.getLogger(__name__)
    BACKEND = "neptune"
    s3_location_import = os.getenv('ARN_IMPORT_BUCKET')
    role_arn = os.getenv('ARN_IAM_ROLE')

    # ---------------------- Create ---------------------------

    graph_id = await create_na_instance("", True)
    logger.info(f"A new instance is created with graph-id: {graph_id.result()}")

    # # ---------------------- Import and Execute ---------------------------
    # future = import_csv_from_s3(
    #     na_graph, s3_location_import)
    # import_blocking_status = await future
    # os.environ['GRAPH_ID'] = graph_id
    # # Initialize a directed graph
    # # BFS on Air route
    # r = list(nx.bfs_edges(nx.DiGraph(), source="48", backend=BACKEND))
    # print('BFS search on Neptune Analytics with source=48 (Vancouver international airport): ')
    # print(f"Total size of the result: {len(r)}")


if __name__ == "__main__":
    asyncio.run(main())
