import os

import networkx as nx
import pandas as pd
import requests

from nx_neptune import NETWORKX_GRAPH_ID, Node, NeptuneGraph
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how Label propagation algorithm (LPA) computation can be offloaded into remote AWS Neptune Analytics instance.  
"""


"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.communities.label_propagation',
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


logger.info("\n---------scenario: AWS - label_propagation_communities----------\n")
# scenario: AWS
logger.info("Algorithm execution - Neptune Analytics:")
result = nx.community.label_propagation_communities(air_route_graph, backend="neptune")
# Print the first group
logger.info(list(result)[:1])


logger.info("\n---------scenario: AWS - fast_label_propagation_communities----------\n")
# scenario: AWS
logger.info("Algorithm execution - Neptune Analytics:")
result = nx.community.fast_label_propagation_communities(air_route_graph, backend="neptune")
# Print the first group
logger.info(list(result)[:1])


logger.info("\n---------scenario: AWS - asyn_lpa_communities----------\n")
# scenario: AWS
logger.info("Algorithm execution - Neptune Analytics:")
result = nx.community.asyn_lpa_communities(air_route_graph, backend="neptune")
# Print the first group
logger.info(list(result)[:1])


na_graph = NeptuneGraph.from_config()

logger.info("\n---------scenario: AWS - label_propagation_communities - Mutation----------\n")
# scenario: AWS
result = nx.community.label_propagation_communities(air_route_graph, backend="neptune", write_property="communities")
logger.info("Algorithm execution - Neptune Analytics: ")
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))


logger.info("\n---------scenario: AWS - fast_label_propagation_communities - Mutation----------\n")
# scenario: AWS
result = nx.community.fast_label_propagation_communities(air_route_graph, backend="neptune", write_property="communities")
logger.info("Algorithm execution - Neptune Analytics: ")
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))


logger.info("\n---------scenario: AWS - asyn_lpa_communities - Mutation----------\n")
# scenario: AWS
result = nx.community.asyn_lpa_communities(air_route_graph, backend="neptune", write_property="communities")
logger.info("Algorithm execution - Neptune Analytics: ")
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))
