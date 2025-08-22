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
import os

import networkx as nx
import pandas as pd
import requests

from nx_neptune import NETWORKX_GRAPH_ID, NeptuneGraph, Node
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how Closeness Centrality computation can delegated to run on a remote AWS Neptune Analytics instance.  
"""


"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.centrality.closeness_centrality',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators', __name__])

# Download routes data
routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
routes_file = "resources/notebook_test_data_routes.dat"
# Ensure the directory exists
os.makedirs(os.path.dirname(routes_file), exist_ok=True)
# Download only if file doesn't exist
if not os.path.isfile(routes_file):
    with open(routes_file, "wb") as f:
        f.write(requests.get(routes_url).content)
cols = [
    "airline", "airline_id", "source_airport", "source_airport_id",
    "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment"
]
routes_df = pd.read_csv("resources/notebook_test_data_routes.dat", names=cols, header=None)
air_route_graph = nx.Graph()  # use Graph for un-directed air routes

for _, row in routes_df.iterrows():
    src = row["source_airport"]
    dst = row["dest_airport"]
    if pd.notnull(src) and pd.notnull(dst):
        air_route_graph.add_edge(src, dst)
logger.info(f'Populated test dataset with nodes:{air_route_graph.number_of_nodes()} and edges:{air_route_graph.number_of_edges()}')


logger.info("\n---------scenario: AWS - closeness_centrality----------\n")
result = nx.closeness_centrality(air_route_graph, backend="neptune")

for key, value in sorted(result.items(), key=lambda x: (x[1], x[0]), reverse=True)[:5]:
    logger.info(f"{key}: {value:.6f}")


logger.info("\n---------scenario: AWS - closeness_centrality - Selected node----------\n")
result = nx.closeness_centrality(air_route_graph, backend="neptune", u="YVR")

logger.info(result)

logger.info("\n---------scenario: AWS - closeness_centrality - Mutation----------\n")
nx.closeness_centrality(air_route_graph, backend="neptune", write_property="ccScore")
na_graph = NeptuneGraph.from_config()
logger.info("Algorithm execution - Neptune Analytics: ")
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))

