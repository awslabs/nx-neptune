import networkx as nx
from nx_neptune import NeptuneGraph
import logging
import os

""" 
This sample script demonstrates how to use AWS Neptune Analytics 
to offload graph algorithm computations from a local cluster.
In this example, a single graph is imported into Neptune Analytics. 
Once imported, a Breadth-First Search (BFS) algorithm is executed on Neptune Analytics 
to determine which nodes (people) are connected as friends to Alice.
"""
logger = logging.getLogger(__name__)
logging.basicConfig(filename="out.log", level=os.getenv("LOGLEVEL", "DEBUG").upper())
nx.config.warnings_to_ignore.add("cache")

"""Read and load graphId from environment variable. """ 
graph_id = os.getenv('GRAPH_ID')
if not graph_id:
    raise Exception('Environment Variable GRAPH_ID is not defined')

"""Clear the Neptune Analytics graph"""
g = nx.Graph()
na_graph = NeptuneGraph(graph=g)
na_graph.clear_graph()

"""
Create a graph with 3 nodes, and edges in between
0 --> 1 --> 2
"""
print("Create NX Graph with 3 nodes: nx.path_graph(3)")
G = nx.path_graph(3)

print('Edges from BFS search from source=0: ')
r = nx.bfs_edges(G, "0", backend="neptune")
print(r)
# [1, 2, 0]

print('Edges from BFS search from source=0; depth_limit=1: ')
r = nx.bfs_edges(G, source="0", depth_limit=1, backend="neptune")
print(r)
# [(0, 1)]

print('Edges from BFS search from source=1: ')
r = nx.bfs_edges(G, source="1", backend="neptune")
print(r)
# [(1, 2)]

print('Edges from BFS search from source=1; reversed=True: ')
r = nx.bfs_edges(G, source="1", reverse=True, backend="neptune")
print(r)
# [(1, 0)]

"""
Create a graph with 12 nodes, and edges in between
0 --> 1 --> 2 --> ... --> 11
"""
print("\nCreate NX Graph with 12 nodes: nx.path_graph(12)")
G = nx.path_graph(12)

r = nx.bfs_edges(G, source="6", backend="neptune")
print('Edges from BFS search from source=6: ')
print(r)
# ['6', ..., '11']

r = nx.bfs_edges(G, source="6", reverse=True, backend="neptune")
print('Edges from BFS search from source=6, reverse=True: ')
print(r)
# ['0', ..., '6']

