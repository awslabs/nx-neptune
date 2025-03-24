from nx_neptune_analytics import NeptuneAnalyticsClient
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
logging.basicConfig(filename="stdout.log", level=os.getenv("LOGLEVEL", "INFO").upper())

"""Provide the graph ID as constructor argument.""" 
client = NeptuneAnalyticsClient(graphId="g-r4g1koz7v9")
client.clear_graph()

"""
This dataset represents a simple directed graph where Alice is indirectly connected to Ken through Bob and Kathy, 
while Ben is an isolated node with no connections. 

Dataset:
Alice --> Bob --> Kathy --> Ken     Ben
"""
client.add_node('a:Person {name: \'Alice\'}')
client.add_node('a:Person {name: \'Bob\'}')
client.add_node('a:Person {name: \'Kathy\'}')
client.add_node('a:Person {name: \'Ken\'}')
client.add_node('a:Person {name: \'Ben\'}')

client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Alice\'}), (b:Person {name: \'Bob\'})')
client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Bob\'}), (b:Person {name: \'Kathy\'})')
client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Kathy\'}), (b:Person {name: \'Ken\'})')

"""
Execute BFS algorithm starting from the node where name = "Alice". 
It traverses the graph outward from Alice, identifying all reachable nodes. 
The result contains the connected nodes and their traversal paths.
"""
result = client.execute_algo_bfs('n', 'n.name=\"Alice\"')

""" 
Print the result, which expect to have Alice, Bob, Kathy and Ken on the result set,
but not Ben, as there is NO direct or indirect edge connection between Alice and Ben (Isolated node). 

{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Kathy'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Bob'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Ken'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Alice'}}}
"""
for item in result:
    print(item)
