import asyncio
import logging
import os
import sys

import boto3
import networkx as nx
from nx_neptune import NeptuneGraph
from nx_neptune.clients import NeptuneAnalyticsClient
from nx_neptune.instance_management import create_na_instance, import_csv_from_s3, delete_na_instance, \
    delete_status_check_wrapper

""" 
This is a sample script to demonstrate how nx-neptune can be used to handle 
the lifecycle of a remote Neptune Analytics resources with create.
"""


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout  # Explicitly set output to stdout
    )
    for logger_name in ['nx_neptune.instance_management', 'nx_neptune.na_graph']:
        logging.getLogger(logger_name).setLevel(logging.DEBUG)

    # ---------------------- CONFIG SETUP ---------------------------
    # Setup the neptune backend configuration so that we have the following workflow:
    # 1. Create a new neptune-graph instance on AWS
    # 2. Import the cit-Patents dataset - if can be downloaded from
    #    https://data.rapids.ai/cugraph/datasets/cit-Patents.csv
    # 3. Run the pagerank algorithm on AWS Neptune Analytics graph on the imported
    #    dataset and return the top 10 results
    # 4. Export the dataset to S3 - to capture any mutations
    # 5. Destroy the neptune-graph instance
    #
    nx.config.backends.neptune.create_new_instance = True
    nx.config.backends.neptune.s3_iam_role = "<your-role>"
    nx.config.backends.neptune.import_s3_bucket = "<your-s3-bucket>/cit-Patents"
    nx.config.backends.neptune.export_s3_bucket = "<your-s3-bucket>/export"
    nx.config.backends.neptune.destroy_instance = True

    # ---------------------- RUN WORKFLOW ---------------------------
    r_neptune = nx.pagerank(g, backend="neptune")
    print("PageRank results using Neptune Analytics:")
    sorted_results = sorted(r_neptune.items(), key=lambda x: (x[1], x[0]), reverse=True)
    for key, value in sorted_results[:10]:
        print(f"{key}: {value:.6f}")

if __name__ == "__main__":
    asyncio.run(main())
