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

from nx_neptune import (
    NeptuneGraph,
    import_csv_from_s3,
    export_csv_to_s3,
    export_athena_table_to_s3,
    create_table_from_s3
)
from nx_neptune.utils.utils import get_stdout_logger

task_id = "t-6v0b09ug64"

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
SELECT DISTINCT "~id", airport_name AS "airport_name:string", 'airline' AS "~label" FROM (
    SELECT source_airport_id as "~id", source_airport as "airport_name" 
    FROM air_routes_db.air_routes_table
    WHERE source_airport_id IS NOT NULL
    UNION ALL
    SELECT dest_airport_id as "~id", dest_airport as "airport_name" 
    FROM air_routes_db.air_routes_table
    WHERE dest_airport_id IS NOT NULL
);
"""

# TODO add properties: airline, airline_id, codeshare, stops
# TODO add list property: equipment
FLIGHT_RELATIONSHIPS = """
SELECT source_airport_id as "~from", dest_airport_id as "~to", 'flight' AS "~label" 
FROM air_routes_db.air_routes_table
WHERE source_airport_id IS NOT NULL AND dest_airport_id IS NOT NULL
"""

CREATE_AIRLINES_TABLE = """
CREATE EXTERNAL TABLE air_routes_db.new_air_routes_table
    airline string
    airline_id string
    ...
STORED AS TEXTFILE
LOCATION 's3://your-neptune-export-bucket/path/'
TBLPROPERTIES ('skip.header.line.count'='1')
"""

SOURCE_AND_DESTINATION_BANK_CUSTOMERS = """
SELECT DISTINCT "~id", 'customer' AS "~label" 
FROM (
    SELECT "nameOrig" as "~id"
    FROM bank_fraud.transactions
    WHERE "nameOrig" IS NOT NULL AND "step"=1
    UNION ALL
    SELECT "nameDest" as "~id"
    FROM bank_fraud.transactions
    WHERE "nameDest" IS NOT NULL AND "step"=1
);
"""

BANK_TRANSACTIONS = """
SELECT 
    "nameOrig" as "~from", 
    "nameDest" as "~to", 
    "type" AS "~label", 
    "step" AS "step:Int", 
    "amount" AS "amount:Float", 
    "oldbalanceOrg" AS "oldbalanceOrg:Float", 
    "newbalanceOrig" AS "newbalanceOrig:Float", 
    "oldbalanceDest" AS "oldbalanceDest:Float", 
    "newbalanceDest" AS "newbalanceDest:Float", 
    "isFraud" AS "isFraud:Int"
FROM bank_fraud.transactions
WHERE "nameOrig" IS NOT NULL AND "nameDest" IS NOT NULL AND "step"=1
"""

CREATE_NEW_BANK_TRANSACTIONS_TABLE = """
CREATE EXTERNAL TABLE IF NOT EXISTS bank_fraud.new_transactions (
    `~id` string,
    `~from` string,
    `~to` string,
    `~label` string,
    `step` int,
    `isFraud` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nx-fraud-detection/t-6v0b09ug64'
TBLPROPERTIES ('classification' = 'csv', 'skip.header.line.count'='1');
"""

ALL_NODES = "MATCH (n) RETURN n LIMIT 10"
ALL_EDGES = "MATCH ()-[r]-() RETURN r LIMIT 10"

async def do_import_from_table():

    # Note: User will need to update the below variable ahead of running the example:
    # Role arn which authorise Neptune Analytics to perform S3 import and export on user behalf,
    # in the format of: arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
    # S3 bucket path for import and export location, which in the format of:
    # s3://BUCKET_NAME/FOLDER_NAME
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')
    print(f"create projection to s3 bucket: {s3_location_import}")

    sql_queries = [
        # SOURCE_AND_DESTINATION_AIRPORT_IDS,
        # FLIGHT_RELATIONSHIPS,
        SOURCE_AND_DESTINATION_BANK_CUSTOMERS,
        BANK_TRANSACTIONS,
    ]
    print(f"running sql queries:{'\n'.join(sql_queries)}")
    export_projection_status = export_athena_table_to_s3(sql_queries, s3_location_import)

async def do_import_from_s3():

    # Clean up remote graph and populate test data.
    na_graph = NeptuneGraph.from_config()
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    # Import blocking
    import_blocking_status = await import_csv_from_s3(na_graph, s3_location_import)
    print("Import completed with status: " + import_blocking_status)

async def do_export_to_s3():
    na_graph = NeptuneGraph.from_config()

    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')

    # Export - blocking
    task_id = await export_csv_to_s3(na_graph, s3_location_export)
    print(f"Export completed with export location: {s3_location_export}/{task_id}")

async def do_export_to_table():

    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')

    # Create table - blocking
    await create_table_from_s3(s3_location_export, CREATE_NEW_BANK_TRANSACTIONS_TABLE)
    # create_table_from_s3(f"{s3_location_export}/{task_id}", s3_location_export, 'bank_fraud.new_transactions_typed')

async def do_execute_opencypher():
    na_graph = NeptuneGraph.from_config()
    all_nodes = na_graph.execute_call(ALL_NODES)
    logger.info(f"all_nodes: {all_nodes}")
    all_edges = na_graph.execute_call(ALL_EDGES)
    logger.info(f"all_edges: {all_edges}")

async def do_execute_sql_query():
    pass

if __name__ == "__main__":
    asyncio.run(do_import_from_table())
    asyncio.run(do_import_from_s3())
    asyncio.run(do_export_to_s3())
    asyncio.run(do_export_to_table())
    # asyncio.run(do_execute_opencypher())
