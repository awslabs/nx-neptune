#!/usr/bin/env python3
import asyncio
import logging
import os
import sys

import networkx as nx

from nx_neptune import NeptuneGraph, import_csv_from_s3, export_csv_to_s3

"""
Reset and Import Data for Neptune Analytics Graph.

This script provides functionality to:
1. Import data from an S3 bucket into the graph
2. Export graph data to an S3 bucket 

The script uses boto3 to interact with the Neptune Analytics API and includes
functions to wait for operations to complete before proceeding.
"""


async def main():
    logging.basicConfig(
        level=logging.WARNING,
        format='%(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout  # Explicitly set output to stdout
    )
    for logger_name in ['IAMClient', 'nx_neptune.clients.instance_management']:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
    logger = logging.getLogger(__name__)


    # Note: User will need to update the below variable ahead of running the example:
    # Role arn which authorise Neptune Analytics to perform S3 import and export on user behalf,
    # in the format of: arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
    # S3 bucket path for import and export location, which in the format of:
    # s3://BUCKET_NAME/FOLDER_NAME
    s3_location_import = os.getenv('ARN_IMPORT_BUCKET')
    s3_location_export = os.getenv('ARN_EXPORT_BUCKET')

    # Clean up remote graph and populate test data.
    g = nx.DiGraph()
    na_graph = NeptuneGraph(graph=g)

    # Import blocking
    future = import_csv_from_s3(
        na_graph, s3_location_import)
    import_blocking_status = await future
    print("Import completed with status: " + import_blocking_status)


    # Export - blocking
    future = export_csv_to_s3(
        na_graph, s3_location_export)
    await future
    print("Export completed with export location: " + s3_location_export)


if __name__ == "__main__":
    asyncio.run(main())
