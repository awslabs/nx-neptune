import networkx as nx
from nx_neptune import NeptuneGraph
import logging
import os

""" 
This is an sample script which demonstrate how class NeptuneGraph 
can be used to perform basic CRUD operation against an existing Neptune Analytics graph.
"""

"""Read and load graphId from environment variable. """
graph_id = os.getenv('GRAPH_ID')
if not graph_id:
    raise Exception('Environment Variable GRAPH_ID is not defined')

"""Clear the Neptune Analytics graph"""
nx_graph = nx.Graph()
g = NeptuneGraph(graph=nx_graph)
g.clear_graph()

"""Populate the dataset by inserting nodes into the graph"""
g.add_node('a:Person {name: \'Alice\'}')
g.add_node('a:Person {name: \'Bob\'}')
g.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Alice\'}), (b:Person {name: \'Bob\'})')

g.update_nodes('(a:Person)', 'a.name = \'Alice\'', 'a.age=\'25\'')

"""Update an edge"""
g.update_edges('(a:Person)-[r:FRIEND_WITH]->(b:Person)',
               'a.name = \'Alice\' AND b.name = \'Bob\'', 
               'r.since = 1997')

""" To demonstrate how to print existing nodes and edges which exist on the graph """

"""List all nodes"""
for item in g.get_all_nodes():
    print(item)

"""List all edges"""
for item in g.get_all_edges():
    print(item)
