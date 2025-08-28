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
import logging
import os
import sys

import boto3
import networkx as nx
from nx_neptune import NeptuneGraph
from nx_neptune.instance_management import create_na_instance, import_csv_from_s3, delete_na_instance

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of a remote Neptune Analytics resources with create.
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
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')
    role_arn = os.getenv('NETWORKX_ARN_IAM_ROLE')

    # ---------------------- Create ---------------------------
    graph_id = await create_na_instance()
    logger.info(f"A new instance is created with graph-id: {graph_id}")

    # ---------------------- Import ---------------------------
    os.environ['NETWORKX_GRAPH_ID'] = graph_id
    print(s3_location_import)
    na_graph = NeptuneGraph.from_config()
    await import_csv_from_s3(na_graph, s3_location_import)

    # Initialize a directed graph
    # ---------------------- Execute ---------------------------
    # BFS on Air route
    # r = list(nx.bfs_edges(nx.DiGraph(), source="48", backend=BACKEND))
    # print('BFS search on Neptune Analytics with source=48 (Vancouver international airport): ')
    # print(f"Total size of the result: {len(r)}")

    # ------------------------- Delete --------------------------
    fut = await delete_na_instance(graph_id)
    logger.info(f"Instance delete completed with status: {fut}")


if __name__ == "__main__":
    asyncio.run(main())
