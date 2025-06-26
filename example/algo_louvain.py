import os

import networkx as nx
import pandas as pd
import requests

from nx_neptune import NETWORKX_GRAPH_ID, Node, NeptuneGraph
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how Louvain algorithm computation can be offloaded into remote AWS Neptune Analytics instance.  
"""
def get_air_route_graph():
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
            air_route_graph.add_edge(src, dst, custom_weight=1)
    logger.info(
        f'Populated test dataset with nodes:{air_route_graph.number_of_nodes()} and edges:{air_route_graph.number_of_edges()}')

    return air_route_graph

"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.communities.louvain',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators', __name__])

# Populate air-route data
g = get_air_route_graph()

logger.info("\n---------scenario: AWS - louvain_communities----------\n")
result = nx.community.louvain_communities(g, backend="neptune")
# Print the first group
logger.info(result[1])


logger.info("\n---------scenario: AWS - louvain_communities - weight ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", weight="custom_weight")
# Print the first group
logger.info(result[:1])

logger.info("\n---------scenario: AWS - louvain_communities - max_level ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", max_level=100)
# Print the first group
logger.info(result[:1])


logger.info("\n---------scenario: AWS - louvain_communities - threshold ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", threshold=0.5)
# Print the first group
logger.info(result[:1])


logger.info("\n---------scenario: AWS - louvain_communities - levelTolerance ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", level_tolerance=0.5)
# Print the first group
logger.info(result[:1])


logger.info("\n---------scenario: AWS - louvain_communities - maxIterations ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", max_iterations=100)
# Print the first group
logger.info(result[:1])


logger.info("\n---------scenario: AWS - louvain_communities - concurrency ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", concurrency=1)
# Print the first group
logger.info(result[:1])

logger.info("\n---------scenario: AWS - louvain_communities - edge_labels ----------\n")
result = nx.community.louvain_communities(g, backend="neptune", edge_labels=["RELATES_TO"])
# Print the first group
logger.info(result[:1])


logger.info("\n---------scenario: AWS - louvain_communities - Mutation----------\n")
result = nx.community.louvain_communities(g, backend="neptune", write_property="communities")
# Print 10 nodes
na_graph = NeptuneGraph.from_config()
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))

