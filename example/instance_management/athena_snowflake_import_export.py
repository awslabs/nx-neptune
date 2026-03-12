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
    create_iceberg_table_from_table
)
from nx_neptune.utils.utils import get_stdout_logger

"""
This script demonstrates importing PaySim fraud detection data from Snowflake
into Neptune Analytics using Athena Federated Query, running graph algorithms,
and exporting enriched results back to S3.

Prerequisites:
- Snowflake account with PaySim data loaded
- AWS Secrets Manager secret with Snowflake credentials
- Athena data source configured for Snowflake (creates Lambda connector automatically)
- Required environment variables set (see below)
"""

logger = get_stdout_logger(__name__, [
    'IAMClient',
    'nx_neptune.instance_management',
    'nx_neptune.utils.decorators',
    'nx_neptune.interface',
    'nx_neptune.na_graph',
    __name__
])

# Environment variables
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'FRAUD_DETECTION')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
ATHENA_CATALOG_NAME = os.getenv('ATHENA_CATALOG_NAME', 'snowflake_catalog')

# SQL queries using Snowflake passthrough syntax
TEST_QUERY = f"""
SELECT *
FROM TABLE(
    "{ATHENA_CATALOG_NAME}".system.query(
        query => '
            USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
            USE DATABASE {SNOWFLAKE_DATABASE};
            USE SCHEMA {SNOWFLAKE_SCHEMA};
            DESCRIBE TABLE TRANSACTIONS
        '
    ))
"""

SOURCE_AND_DESTINATION_CUSTOMERS = f"""
SELECT *
FROM TABLE(
    "{ATHENA_CATALOG_NAME}".system.query(
        query => '
            USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
            USE DATABASE {SNOWFLAKE_DATABASE};
            USE SCHEMA {SNOWFLAKE_SCHEMA};
            SELECT DISTINCT "~id", ''customer'' AS "~label"
            FROM (
                SELECT NAMEORIG as "~id" FROM TRANSACTIONS WHERE NAMEORIG IS NOT NULL
                UNION ALL
                SELECT NAMEDEST as "~id" FROM TRANSACTIONS WHERE NAMEDEST IS NOT NULL
            )
        '
    ))
"""

BANK_TRANSACTIONS = f"""
SELECT *
FROM TABLE(
    "{ATHENA_CATALOG_NAME}".system.query(
        query => '
            USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
            USE DATABASE {SNOWFLAKE_DATABASE};
            USE SCHEMA {SNOWFLAKE_SCHEMA};
            SELECT
                NAMEORIG as "~from",
                NAMEDEST as "~to",
                TYPE AS "~label",
                STEP AS "step:Int",
                AMOUNT AS "amount:Float",
                OLDBALANCEORG AS "oldbalanceOrg:Float",
                NEWBALANCEORIG AS "newbalanceOrig:Float",
                OLDBALANCEDEST AS "oldbalanceDest:Float",
                NEWBALANCEDEST AS "newbalanceDest:Float",
                ISFRAUD AS "isFraud:Int",
                ISFLAGGEDFRAUD AS "isFlaggedFraud:Int"
            FROM TRANSACTIONS WHERE NAMEORIG IS NOT NULL AND NAMEDEST IS NOT NULL
            '
    ))
"""

ALL_NODES = "MATCH (n) RETURN n LIMIT 10"
ALL_EDGES = "MATCH ()-[r]-() RETURN r LIMIT 10"

async def do_import_from_snowflake():
    """Export Snowflake data to S3 via Athena Federated Query"""
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')
    print(f"Creating projection to S3 bucket: {s3_location_import}")

    sql_queries = [
        TEST_QUERY,
        #### uncomment this
        # SOURCE_AND_DESTINATION_CUSTOMERS,
        # BANK_TRANSACTIONS,
    ]
    sql_queries_str = '\n'.join(sql_queries)
    print(f"Running SQL queries via Athena Federated Query:\n{sql_queries_str}")
    
    export_projection_status = await export_athena_table_to_s3(
        sql_queries,
        [],  # No parameters needed for Snowflake passthrough queries
        s3_location_import,
        catalog=ATHENA_CATALOG_NAME,
        database=SNOWFLAKE_DATABASE,
    )
    print(f"Export projection status: {export_projection_status}")

async def do_import_from_s3():
    """Import CSV data from S3 into Neptune Analytics"""
    na_graph = NeptuneGraph.from_config()
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    import_blocking_status = await import_csv_from_s3(na_graph, s3_location_import)
    print("Import completed with status: " + import_blocking_status)

async def do_export_to_s3():
    """Export Neptune Analytics graph to S3"""
    na_graph = NeptuneGraph.from_config()
    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')

    task_id = await export_csv_to_s3(na_graph, s3_location_export)
    print(f"Export completed with export location: {s3_location_export}/{task_id}")

async def do_export_to_csv_table():
    """Create CSV table in Athena from exported Neptune data"""
    task_id = os.getenv('TASK_ID')
    s3_sql_output = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    s3_export_location = f"{os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')}/{task_id}"
    catalog = 'AwsDataCatalog'
    database = 'fraud_detection_results'
    csv_table_name = 'transactions_csv'

    await create_csv_table_from_s3(s3_export_location, s3_sql_output, csv_table_name, catalog=catalog, database=database)
    print(f"Created CSV table: {catalog}.{database}.{csv_table_name}")

async def do_export_to_iceberg_table():
    """Convert CSV tables to Iceberg format"""
    s3_sql_output = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    catalog = 's3tablescatalog/fraud-detection-results'
    database = 'fraud_detection_results'

    iceberg_transactions_table_name = 'transactions_enriched'
    csv_transactions_table_name = 'AwsDataCatalog.fraud_detection_results.transactions_csv_edges'
    transactions_query_id = await create_iceberg_table_from_table(
        s3_sql_output, iceberg_transactions_table_name, csv_transactions_table_name, 
        catalog=catalog, database=database
    )
    print(f"Created iceberg table {iceberg_transactions_table_name} with query ID: {transactions_query_id}")

    iceberg_customers_table_name = 'customers_with_communities'
    csv_customers_table_name = 'AwsDataCatalog.fraud_detection_results.transactions_csv_vertices'
    customers_query_id = await create_iceberg_table_from_table(
        s3_sql_output, iceberg_customers_table_name, csv_customers_table_name, 
        catalog=catalog, database=database
    )
    print(f"Created iceberg table {iceberg_customers_table_name} with query ID: {customers_query_id}")

async def do_execute_opencypher():
    """Execute graph algorithms on Neptune Analytics"""
    nx.config.backends.neptune.graph_id = os.getenv('NETWORKX_GRAPH_ID')
    nx.config.backends.neptune.skip_graph_reset = True

    result = nx.community.louvain_communities(nx.Graph(), backend="neptune", write_property="community")
    print(f"Louvain result: \n{result}")
    for community in result:
        if len(community) > 30:
            print(f"Possible fraud community (size:{len(community)}): {community}")

    result = nx.pagerank(
        nx.Graph(),
        edge_weight_property="amount",
        edge_weight_type="double",
        write_property="amount_rank",
        backend="neptune"
    )
    print(f"PageRank result: \n{result}")

def do_execute_dump_graph():
    """Dump sample graph data"""
    na_graph = NeptuneGraph.from_config()
    all_nodes = na_graph.execute_call(ALL_NODES)
    print(f"Sample nodes: {all_nodes}")

    all_edges = na_graph.execute_call(ALL_EDGES)
    print(f"Sample edges: {all_edges}")

if __name__ == "__main__":
    asyncio.run(do_import_from_snowflake())
    asyncio.run(do_import_from_s3())
    asyncio.run(do_export_to_s3())
    asyncio.run(do_export_to_csv_table())
    asyncio.run(do_export_to_iceberg_table())
    asyncio.run(do_execute_opencypher())
    do_execute_dump_graph()
