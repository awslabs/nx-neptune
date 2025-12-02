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

from nx_neptune import NeptuneGraph, set_config_graph_id
from nx_neptune.instance_management import create_na_instance, import_csv_from_s3, delete_na_instance

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

graph_id = os.getenv('NETWORKX_GRAPH_ID')

async def do_create_instance():
    # ---------------------- Create ---------------------------
    logger.info("Creating new graph")
    graph_id = await create_na_instance()
    logger.info(f"A new instance is created with graph-id: {graph_id}")

async def do_import_from_s3():
    # ---------------------- SETUP ----------------------------
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    # ---------------------- Import ---------------------------
    print(f"Importing CSV from {s3_location_import} into {graph_id}")
    na_graph = NeptuneGraph.from_config(config=set_config_graph_id(graph_id))
    status = await import_csv_from_s3(na_graph, s3_location_import)
    logger.info(f"Import completed with status: {status}")

async def do_delete():
    # ------------------------- Delete --------------------------
    status = await delete_na_instance(graph_id)
    logger.info(f"Instance delete completed with status: {status}")


if __name__ == "__main__":
    # asyncio.run(do_create_instance())
    asyncio.run(do_import_from_s3())
    # asyncio.run(do_delete())
