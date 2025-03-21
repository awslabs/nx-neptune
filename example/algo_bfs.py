from nx_neptune_analytics import NeptuneAnalyticsClient
import logging
import os

""" 
This is an sample script to demonstrate how AWS Neptune Analytic service 
can be leveraged to offload graph algorithm computation from local cluster.
"""
logger = logging.getLogger(__name__)
logging.basicConfig(filename="stdout.log", level=os.getenv("LOGLEVEL", "INFO").upper())

"""Provide the graph ID as constructor argument.""" 
client = NeptuneAnalyticsClient(graphId="g-r4g1koz7v9")
client.clear_graph()

"""
Adding nodes and edges to composite a simple graph on AWS NA graph for test dataset,
which Alice direct or indirectly connects to Bob, Kathy and Ken, but not Ben. 

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

""" Trigger the remote execution of BFS algorithm with provided dataset."""
result = client.execute_algo_bfs('n', 'n.name=\"Alice\"')

""" 
Print the result, which expect to have Alice, Bob, Kathy and Ken on the result set,
but not Ben, as there is NO direct or indirect edge connection between Alice and Ben. 

{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Kathy'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Bob'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Ken'}}}
{'node': {'~id': 'xxx', '~entityType': 'node', '~labels': ['Person'], '~properties': {'name': 'Alice'}}}
"""
for item in result:
    print(item)
