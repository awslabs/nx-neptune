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
#!/usr/bin/env python3
import asyncio
import os

from dotenv import load_dotenv
load_dotenv()

from nx_neptune import NeptuneGraph, import_csv_from_s3, export_csv_to_s3, export_athena_table_to_s3
from nx_neptune.utils.utils import get_stdout_logger

"""
TODO update description

The script uses boto3 to interact with the Neptune Analytics API and includes
functions to wait for operations to complete before proceeding.
"""

logger = get_stdout_logger(__name__, [
    'IAMClient',
    'nx_neptune.instance_management',
    'nx_neptune.utils.decorators',
    'nx_neptune.interface',
    'nx_neptune.na_graph',
    'nx_neptune.clients.instance_management', __name__])

SOURCE_AND_DESTINATION_AIRPORT_IDS = """
SELECT DISTINCT "~id", airport_name, 'airline' AS "~label" FROM (
    SELECT source_airport_id as "~id", source_airport as "airport_name" 
    FROM air_routes_db.air_routes_table
    WHERE source_airport_id IS NOT NULL
    UNION ALL
    SELECT dest_airport_id as "~id", dest_airport as "airport_name" 
    FROM air_routes_db.air_routes_table
    WHERE dest_airport_id IS NOT NULL
);
"""

FLIGHT_RELATIONSHIPS = """
SELECT "source_airport_id" as "~from", "dest_airport_id" as "~to", 'flight' AS "~label", 
FROM air_routes_db.air_routes_table
"""

SOURCE_AIRPORTS_WITH_MORE_STOPS = """
SELECT DISTINCT source_airport_id AS "~id", 'airline' AS "~label" 
FROM air_routes_db.air_routes_table 
WHERE stops > 0
"""

ALL_NODES = "MATCH (n) RETURN n"
ALL_EDGES = "MATCH (m)-[r]-(n) RETURN m, r, n"

async def do_import_from_table():

    # Note: User will need to update the below variable ahead of running the example:
    # Role arn which authorise Neptune Analytics to perform S3 import and export on user behalf,
    # in the format of: arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
    # S3 bucket path for import and export location, which in the format of:
    # s3://BUCKET_NAME/FOLDER_NAME
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    sql_queries = [
        SOURCE_AND_DESTINATION_AIRPORT_IDS,
        FLIGHT_RELATIONSHIPS,
    ]
    export_projection_status = export_athena_table_to_s3(sql_queries, s3_location_import)

async def do_import_from_s3():

    # Clean up remote graph and populate test data.
    na_graph = NeptuneGraph.from_config()
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    # Import blocking
    import_blocking_status = await import_csv_from_s3(na_graph, s3_location_import)
    print("Import completed with status: " + import_blocking_status)

async def do_export_to_table():

    na_graph = NeptuneGraph.from_config()

    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    print("Skip export")

    # Export - blocking
    # await export_csv_to_s3(na_graph, s3_location_export)
    # print("Export completed with export location: " + s3_location_export)

async def do_execute_opencypher():

    na_graph = NeptuneGraph.from_config()
    all_nodes = na_graph.execute_call(ALL_NODES)
    logger.info(f"all_nodes: {all_nodes}")
    all_edges = na_graph.execute_call(ALL_EDGES)
    logger.info(f"all_edges: {all_edges}")

if __name__ == "__main__":
    asyncio.run(do_import_from_table())
    # asyncio.run(do_import_from_s3())
    # asyncio.run(do_export_to_table())
    # asyncio.run(do_execute_opencypher())
