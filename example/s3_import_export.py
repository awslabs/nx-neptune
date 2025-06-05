#!/usr/bin/env python3
import asyncio
import os

import networkx as nx

from nx_neptune import NeptuneGraph, import_csv_from_s3, export_csv_to_s3
from nx_neptune.utils.utils import get_stdout_logger

"""
Reset and Import Data for Neptune Analytics Graph.

This script provides functionality to:
1. Import data from an S3 bucket into the graph
2. Export graph data to an S3 bucket 

The script uses boto3 to interact with the Neptune Analytics API and includes
functions to wait for operations to complete before proceeding.
"""


async def main():
    logger = get_stdout_logger(__name__, [
        'IAMClient',
        'nx_neptune.instance_management',
        'nx_neptune.utils.decorators',
        'nx_neptune.interface',
        'nx_neptune.na_graph',
        'nx_neptune.clients.instance_management', __name__])

    # Note: User will need to update the below variable ahead of running the example:
    # Role arn which authorise Neptune Analytics to perform S3 import and export on user behalf,
    # in the format of: arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
    # S3 bucket path for import and export location, which in the format of:
    # s3://BUCKET_NAME/FOLDER_NAME
    s3_location_import = os.getenv('ARN_IMPORT_BUCKET')
    s3_location_export = os.getenv('ARN_EXPORT_BUCKET')

    # Clean up remote graph and populate test data.
    g = nx.DiGraph()
    na_graph = NeptuneGraph.from_config(graph=g)

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
