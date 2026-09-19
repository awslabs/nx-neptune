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
"""Import a data-lake table into Neptune Analytics from a declarative schema.

Instead of hand-writing the Athena projection SELECTs (see
``athena_table_import_export.py``), declare the vertex/edge mapping with a
SQL/PGQ ``CREATE PROPERTY GRAPH`` statement and let ``import_from_graph_schema``
translate it into those queries. This runs the same Athena -> S3 -> Neptune
import path.
"""
import asyncio
import os

from dotenv import load_dotenv
load_dotenv()

from nx_neptune import property_graph_to_sql
from nx_neptune.session_manager import CleanupTask, SessionManager
from nx_neptune.utils.utils import get_stdout_logger

logger = get_stdout_logger(__name__, [
    'nx_neptune.session_manager',
    'nx_neptune.instance_management',
    'nx_neptune.property_graph',
    __name__
])

"""
This data comes from kaggle.com:
https://www.kaggle.com/code/kartik2112/fraud-detection-on-paysim-dataset/input?select=PS_20174392719_1491204439457_log.csv

The same paysim projection expressed as a property-graph schema. Customers are
projected from the transactions table's origin/destination account columns, and
each transaction becomes a labelled edge between them.
"""
FINANCIAL_GRAPH_DDL = """
CREATE PROPERTY GRAPH financial
  VERTEX TABLES (
    transactions AS customer
      KEY (nameOrig)
      LABEL customer
  )
  EDGE TABLES (
    transactions
      SOURCE KEY (nameOrig) REFERENCES customer
      DESTINATION KEY (nameDest) REFERENCES customer
      LABEL transfer
      PROPERTIES (
        type AS type,
        step:Int,
        amount:Float,
        oldbalanceOrg:Float,
        newbalanceOrig:Float,
        oldbalanceDest:Float,
        newbalanceDest:Float,
        isFraud:Int
      )
  )
"""


def preview_generated_sql():
    """Print the Athena queries the DDL translates into, without importing."""
    for query in property_graph_to_sql(FINANCIAL_GRAPH_DDL):
        print(query)
        print("---")


async def do_import_from_graph_schema():
    # Note: update these ahead of running the example.
    # S3 bucket path for the intermediate CSV projection, in the format:
    # s3://BUCKET_NAME/FOLDER_NAME
    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')

    with SessionManager("property-graph-demo", cleanup_task=CleanupTask.NONE) as session:
        graph = await session.get_or_create_graph()
        graph_id = await session.import_from_graph_schema(
            graph,
            s3_location_import,
            FINANCIAL_GRAPH_DDL,
            catalog='s3tablescatalog/nx-fraud-detection-data',
            database='bank_fraud_full',
        )
        print(f"Imported data into graph {graph_id}")


if __name__ == "__main__":
    preview_generated_sql()
    asyncio.run(do_import_from_graph_schema())
