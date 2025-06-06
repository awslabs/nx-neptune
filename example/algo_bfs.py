import networkx as nx
from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID
from nx_neptune.utils.utils import get_stdout_logger

""" 
This sample script demonstrates how to use AWS Neptune Analytics 
to offload graph algorithm computations from a local cluster.
In this example, a single graph is imported into Neptune Analytics. 
Once imported, a Breadth-First Search (BFS) algorithm is executed on Neptune Analytics 
to determine which nodes (people) are connected as friends to Alice.
"""
logger = get_stdout_logger(__name__,  ['nx_neptune.algorithms.traversal.bfs',
                                       'nx_neptune.na_graph',
                                       'nx_neptune.utils.decorators'])

nx.config.warnings_to_ignore.add("cache")

"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

BACKEND = "neptune"
print(f"Using backend={BACKEND}")

"""Clear the Neptune Analytics graph"""
g = nx.Graph()
na_graph = NeptuneGraph.from_config(graph=g)
na_graph.clear_graph()

"""
Using a Directed Graph: Add named nodes with strings for the "name" property
"""
G = nx.DiGraph()
G.add_node("Alice")
G.add_node("Bob")
G.add_node("Carl")
G.add_edge("Alice", "Bob")
G.add_edge("Alice", "Carl")

print('Edges from BFS search from source="Alice": ')
r = list(nx.bfs_edges(G, "Alice", backend=BACKEND))
print(r) # [("Alice", "Bob"), ("Alice", "Carl")]
assert isinstance(r, list)
assert len(r) == 2
assert ["Alice", "Bob"] in r
assert ["Alice", "Carl"] in r

print('Edges from BFS search from source="Bob"; reverse=True; depth_limit=1: ')
r = list(nx.bfs_edges(G, "Bob", backend=BACKEND, reverse=True, depth_limit=1))
print(r) # [("Bob", "Alice")]
assert isinstance(r, list)
assert len(r) == 1
assert ["Bob", "Alice"] in r

na_graph.clear_graph()

"""
Create a graph with 3 nodes, and edges in between
0 --> 1 --> 2
"""
G = nx.path_graph(3)

print('Edges from BFS search from source=0: ')
r = list(nx.bfs_edges(G, "0", backend=BACKEND))
print(r)
assert isinstance(r, list)
assert len(r) == 2
assert ["0", "1"] in r
assert ["1", "2"] in r

print('Edges from BFS search from source=0; depth_limit=1: ')
r = list(nx.bfs_edges(G, source="0", depth_limit=1, backend=BACKEND))
print(r)
assert r == [["0", "1"]]

print('Edges from BFS search from source=1: ')
r = list(nx.bfs_edges(G, source="1", backend=BACKEND))
print(r)
assert len(r) == 2
assert ["1", "0"] in r
assert ["1", "2"] in r

print('Edges from BFS search from source=1; reversed=True: ')
r = list(nx.bfs_edges(G, source="1", reverse=True, backend=BACKEND))
print(r)
assert isinstance(r, list)
assert len(r) == 2
assert ["1", "0"] in r
assert ["1", "2"] in r

print('Edges from BFS search from source=1; reversed=False: ')
r = list(nx.bfs_edges(G, source="1", reverse=False, backend=BACKEND))
print(r)
assert isinstance(r, list)
assert len(r) == 2
assert ["1", "0"] in r
assert ["1", "2"] in r

na_graph.clear_graph()

"""
Create a graph with 12 nodes, and edges in between
0 --> 1 --> 2 --> ... --> 11
"""
print("\nCreate NX Graph with 12 nodes: nx.path_graph(12)")
G = nx.path_graph(12)

r = list(nx.bfs_edges(G, source="6", backend=BACKEND))
print('Edges from BFS search from source=6: ')
print(r)
# [(6, 7) ..., (10, 11)]
assert isinstance(r, list)
assert len(r) == 11
for i in [6, 7, 8, 9, 10]:
    assert[str(i), str(i+1)] in r
for i in [6, 5, 4, 3, 2, 1]:
    assert[str(i), str(i-1)] in r

na_graph.clear_graph()
G = nx.Graph()
G.add_node("Alice")
G.add_node("Bob")
G.add_node("Carl")
G.add_edge("Alice", "Bob")
G.add_edge("Alice", "Carl")

logger.info("\n-------------------\n")
# scenario: AWS - vertex_label, edge_labels and concurrency
r = list(nx.bfs_edges(G, source="Alice", backend=BACKEND, vertex_label="Node" ,edge_labels=["RELATES_TO"], concurrency=0))
print('Edges from BFS search from source=Alice: ')
for value in sorted(r, reverse=True):
    logger.info(value)
