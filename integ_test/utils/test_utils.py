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
import pytest
import pandas as pd
import requests
import networkx as nx
from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID

__all__ = [
    "BACKEND",
    "air_route_graph",
    "neptune_graph"
]

BACKEND = os.environ.get("BACKEND") or "neptune"
if BACKEND == "False" or BACKEND == "None":
    BACKEND=None

@pytest.fixture(scope="module", autouse=True)
def neptune_graph():
    """Setup Neptune graph for testing"""
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

    print(f"\nRun tests using BACKEND={BACKEND}")
    print(f"graph_identifier={NETWORKX_GRAPH_ID}")

    g = nx.Graph()
    na_graph = NeptuneGraph.from_config(graph=g)
    return na_graph

@pytest.fixture(scope="module")
def air_route_graph():
    """Create airline routes graph from resources data"""
    routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
    routes_file = "integ_test/resources/test_data_routes.dat"
    
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
    
    routes_df = pd.read_csv(routes_file, names=cols, header=None)
    air_route_graph = nx.Graph()  # use Graph for un-directed air routes

    for _, row in routes_df.iterrows():
        src = row["source_airport"]
        dst = row["dest_airport"]
        if pd.notnull(src) and pd.notnull(dst):
            air_route_graph.add_edge(src, dst)
    
    return air_route_graph