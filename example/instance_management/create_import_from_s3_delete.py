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

from dotenv import load_dotenv
load_dotenv()

from nx_neptune import NeptuneGraph, set_config_graph_id, export_csv_to_s3, empty_s3_bucket, Node, Edge
from resources_management.instance_management import create_na_instance, import_csv_from_s3, delete_na_instance

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of a remote Neptune Analytics resources.  We create a new instance, import a CSV into the instance, 
and destroy it.
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout  # Explicitly set output to stdout
)
for logger_name in ['nx_neptune.instance_management', 'nx_neptune.na_graph']:
    logging.getLogger(logger_name).setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

graph_id = os.getenv('GRAPH_ID')
task_id = os.getenv('TASK_ID')

async def do_create_instance():
    global graph_id, task_id

    # ---------------------- Create ---------------------------
    logger.info("Creating new graph")

    graph_id = await create_na_instance()
    logger.info(f"A new instance is created with graph-id: {graph_id}")

    # Populate the dataset by inserting nodes into the graph
    na_graph = NeptuneGraph.from_config(config=set_config_graph_id(graph_id))
    alice = Node(id='Alice', labels=['Person'], properties={'age': 24})
    na_graph.add_node(alice)

    bob = Node(id='Bob', labels=['Person'], properties={'hair': 'brown'})
    na_graph.add_node(bob)

    edge = Edge(label='FRIEND_WITH', properties={}, node_src=alice, node_dest=bob)
    na_graph.add_edge(edge)

async def do_export_to_s3():
    global graph_id, task_id

    # ---------------------- SETUP ----------------------------
    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')

    # ---------------------- Export ---------------------------
    print(f"Exporting CSV to {s3_location_export} from {graph_id}")
    na_graph = NeptuneGraph.from_config(config=set_config_graph_id(graph_id))
    task_id = await export_csv_to_s3(na_graph, s3_location_export)
    logger.info(f"Export completed. Task id: {task_id}")

async def do_import_from_s3():
    global graph_id, task_id

    # ---------------------- SETUP ----------------------------
    if task_id:
        s3_location_import = f"{os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')}{task_id}/"
    else:
        s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    # ---------------------- Import ---------------------------
    print(f"Importing CSV from {s3_location_import} into {graph_id}")
    na_graph = NeptuneGraph.from_config(config=set_config_graph_id(graph_id))
    import_task_id = await import_csv_from_s3(na_graph, s3_location_import)
    logger.info(f"Import completed: task id {import_task_id}")

async def do_delete():
    global graph_id, task_id

    # ------------------------- DELETE --------------------------
    if task_id:
        s3_location_import = f"{os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')}{task_id}/"
        print(f"Deleting S3 bucket {s3_location_import}")
        empty_s3_bucket(s3_location_import)

    status = await delete_na_instance(graph_id)
    logger.info(f"Instance delete completed with status: {status}")

if __name__ == "__main__":
    asyncio.run(do_create_instance())
    asyncio.run(do_export_to_s3())
    asyncio.run(do_import_from_s3())
    asyncio.run(do_delete())
