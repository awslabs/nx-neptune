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

from resources_management.instance_management import (
    create_na_instance, 
    create_graph_snapshot, 
    create_na_instance_from_snapshot,
    delete_graph_snapshot,
    delete_na_instance
)

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of Neptune Analytics snapshots. We create an instance, create a snapshot, 
import the snapshot into a new instance, delete the snapshot, and delete both instances.
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

source_graph_id = None
snapshot_id = None
target_graph_id = None

async def do_create_source_instance():
    global source_graph_id
    
    logger.info("Creating source graph instance")
    source_graph_id = await create_na_instance()
    logger.info(f"Source instance created with graph-id: {source_graph_id}")

async def do_create_snapshot():
    global source_graph_id, snapshot_id
    
    logger.info(f"Creating snapshot from graph: {source_graph_id}")
    snapshot_id = await create_graph_snapshot(source_graph_id)
    logger.info(f"Snapshot created with id: {snapshot_id}")

async def do_create_instance_from_snapshot():
    global snapshot_id, target_graph_id
    
    logger.info(f"Creating new instance from snapshot: {snapshot_id}")
    target_graph_id = await create_na_instance_from_snapshot(snapshot_id)
    logger.info(f"Target instance created with graph-id: {target_graph_id}")

async def do_delete_snapshot():
    global snapshot_id
    
    logger.info(f"Deleting snapshot: {snapshot_id}")
    status = await delete_graph_snapshot(snapshot_id)
    logger.info(f"Snapshot deletion completed with status: {status}")

async def do_delete_instances():
    global source_graph_id, target_graph_id
    
    logger.info(f"Deleting both instances in parallel: {source_graph_id}, {target_graph_id}")
    source_status, target_status = await asyncio.gather(
        delete_na_instance(source_graph_id),
        delete_na_instance(target_graph_id)
    )
    logger.info(f"Source instance deletion completed with status: {source_status}")
    logger.info(f"Target instance deletion completed with status: {target_status}")

if __name__ == "__main__":
    asyncio.run(do_create_source_instance())
    asyncio.run(do_create_snapshot())
    asyncio.run(do_create_instance_from_snapshot())
    asyncio.run(do_delete_snapshot())
    asyncio.run(do_delete_instances())
