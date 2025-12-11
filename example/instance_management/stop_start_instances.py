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
from nx_neptune.instance_management import (
    create_na_instance, 
    create_graph_snapshot, 
    create_na_instance_from_snapshot,
    stop_na_instance,
    start_na_instance,
    delete_graph_snapshot,
    delete_na_instance
)

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of Neptune Analytics snapshots with stop/start operations. 
We create an instance, create a snapshot, import the snapshot into a new instance,
stop the instance, start it again, delete the snapshot, and delete both instances.
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
for logger_name in ['nx_neptune.instance_management', 'nx_neptune.na_graph']:
    logging.getLogger(logger_name).setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

graph_id = None
snapshot_id = None

async def do_create_source_instance():
    global graph_id
    
    logger.info("Creating graph instance")
    graph_id = await create_na_instance()
    logger.info(f"Instance created with graph-id: {graph_id}")

async def do_stop_instance():
    global graph_id
    
    logger.info(f"Stopping target instance: {graph_id}")
    graph_id = await stop_na_instance(graph_id)
    logger.info(f"Target instance stopped")

async def do_start_instance():
    global graph_id
    
    logger.info(f"Starting target instance: {graph_id}")
    graph_id = await start_na_instance(graph_id)
    logger.info(f"Target instance started")

async def do_delete_instances():
    global graph_id
    
    logger.info(f"Deleting instance: {graph_id}")
    await delete_na_instance(graph_id)
    logger.info(f"Instance deletion completed")

if __name__ == "__main__":
    asyncio.run(do_create_source_instance())
    asyncio.run(do_stop_instance())
    asyncio.run(do_start_instance())
    asyncio.run(do_delete_instances())
