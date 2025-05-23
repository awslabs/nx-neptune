import networkx as nx

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID
from nx_neptune.clients import Node, Edge

""" 
This is an sample script which demonstrate how class NeptuneGraph 
can be used to perform basic CRUD operation against an existing Neptune Analytics graph.
"""

"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable NETWORKX_GRAPH_ID is not defined')

"""Clear the Neptune Analytics graph"""
nx_graph = nx.Graph()
g = NeptuneGraph.from_config(graph=nx_graph)
g.clear_graph()

"""Populate the dataset by inserting nodes into the graph"""
alice = Node(id='Alice', labels=['Person'], properties={'age': 24})
bob = Node(id='Bob', labels=['Person'], properties={'hair': 'brown'})
edge = Edge(label='FRIEND_WITH', properties={}, node_src=alice, node_dest=bob)

g.add_node(alice)
g.add_node(bob)

g.add_edge(edge)

g.update_node(
    match_labels='Person',
    ref_name='a',
    node=alice,
    properties_set={'a.hair': 'black'})

g.update_nodes(
    match_labels='Person',
    ref_name='a',
    nodes=[alice, bob],
    properties_set={'a.career': 'Flight Instructor'})

"""Update an edge"""
g.update_edges('a', 'r', 'b',
               edge,
               {'a.name': 'Alice', 'b.name': 'Bob'},
               {'r.since': 1997})

""" To demonstrate how to print existing nodes and edges which exist on the graph """

"""List all nodes"""
for item in g.get_all_nodes():
    print(Node.from_neptune_response(item))

"""List all edges"""
for item in g.get_all_edges():
    print(item)
