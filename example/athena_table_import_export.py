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

import networkx as nx

from nx_neptune import (
    NeptuneGraph,
    import_csv_from_s3,
    export_csv_to_s3,
    export_athena_table_to_s3,
    create_csv_table_from_s3,
    create_iceberg_table_from_table, louvain_communities
)
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
    __name__
])

"""
This data comes from kaggle.com:
https://www.kaggle.com/code/kartik2112/fraud-detection-on-paysim-dataset/input?select=PS_20174392719_1491204439457_log.csv
"""
SOURCE_AND_DESTINATION_BANK_CUSTOMERS = """
SELECT DISTINCT "~id", 'customer' AS "~label" 
FROM (
    SELECT "nameOrig" as "~id"
    FROM transactions
    WHERE "nameOrig" IS NOT NULL
    UNION ALL
    SELECT "nameDest" as "~id"
    FROM transactions
    WHERE "nameDest" IS NOT NULL
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
FROM transactions
WHERE "nameOrig" IS NOT NULL AND "nameDest" IS NOT NULL
"""

CREATE_NEW_BANK_TRANSACTIONS_TABLE = """
CREATE EXTERNAL TABLE IF NOT EXISTS bank_fraud_full.transactions (
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
        SOURCE_AND_DESTINATION_BANK_CUSTOMERS,
        BANK_TRANSACTIONS,
    ]
    print(f"running sql queries:{'\n'.join(sql_queries)}")
    export_projection_status = export_athena_table_to_s3(
        sql_queries,
        s3_location_import,
        catalog='s3tablescatalog/nx-fraud-detection-data',
        database='bank_fraud_full',
    )

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

async def do_export_to_csv_table():

    task_id = os.getenv('TASK_ID')
    s3_sql_output = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    s3_export_location = f"{os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')}/{task_id}"
    catalog = 'AwsDataCatalog'
    database = 'bank_fraud_full'
    csv_table_name = 'transactions_csv'

    # Create table - blocking
    create_csv_table_from_s3(s3_export_location, s3_sql_output, csv_table_name, catalog=catalog, database=database)

async def do_export_to_iceberg_table():

    s3_sql_output = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    catalog = 's3tablescatalog/nx-fraud-detection-data'
    database = 'bank_fraud_full'

    iceberg_table_name = 'transactions_updated'
    csv_table_name = 'AwsDataCatalog.bank_fraud_full.transactions_csv_edges'
    create_iceberg_table_from_table(s3_sql_output, iceberg_table_name, csv_table_name, catalog=catalog, database=database)

    iceberg_table_name = 'customers_updated'
    csv_table_name = 'AwsDataCatalog.bank_fraud_full.transactions_csv_vertices'
    create_iceberg_table_from_table(s3_sql_output, iceberg_table_name, csv_table_name, catalog=catalog, database=database)

async def do_execute_opencypher():
    nx.config.backends.neptune.graph_id = os.getenv('NETWORKX_GRAPH_ID')
    nx.config.backends.neptune.skip_graph_reset = True

    result = nx.community.louvain_communities(nx.Graph(), backend="neptune")
    print(f"louvain result: \n{result}")
    for community in result:
        if len(community) > 30:
            print(f"possible fraud (size:{len(community)})? {community}")

    result = nx.pagerank(
        nx.Graph(),
        edge_weight_property="amount",
        edge_weight_type="double",
        write_property="amount_rank",
        backend="neptune"
    )
    print(f"pagerank result: \n{result}")

def do_execute_dump_graph():
    na_graph = NeptuneGraph.from_config()
    all_nodes = na_graph.execute_call(ALL_NODES)
    print(f"all nodes: {all_nodes}")

    all_edges = na_graph.execute_call(ALL_EDGES)
    print(f"all edges: {all_edges}")

if __name__ == "__main__":
    asyncio.run(do_import_from_table())
    asyncio.run(do_import_from_s3())
    asyncio.run(do_export_to_s3())
    asyncio.run(do_export_to_csv_table())
    asyncio.run(do_export_to_iceberg_table())
    asyncio.run(do_execute_opencypher())
    do_execute_dump_graph()

