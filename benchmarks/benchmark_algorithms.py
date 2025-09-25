#!/usr/bin/env python3
"""
Benchmarking script for nx-neptune algorithms.
Runs all supported algorithms 10 times in parallel and measures execution time.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import time
import json
import logging
import pandas as pd
import networkx as nx
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from types import GeneratorType

from nx_neptune.interface import ALGORITHMS

# For debugging
ALGORITHMS = [
    # "bfs_edges",
    # "bfs_layers",
    # "descendants_at_distance",
    # "pagerank",
    # "degree_centrality",
    # "in_degree_centrality",
    # "out_degree_centrality",
    # "closeness_centrality",
    "label_propagation_communities",
    # "asyn_lpa_communities",
    # "fast_label_propagation_communities",
    # "louvain_communities",
]

alg_params = {
    "bfs_edges": {
        "source": "BOS" # BOS airport
    },
    "bfs_layers": {
        "sources": ["JFK", "BOS", "DFW", "LAX", "ORD"] # various airports by ID
    },
    "descendants_at_distance": {
        "source": "BOS", # BOS airport
        "distance": 2,
    },
}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RUN_COUNT = 10
MAX_WORKERS = 10
BACKEND = "neptune"
# BACKEND = False

def setup_test_data():
    """Download and prepare test data."""
    routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
    routes_file = "resources/notebook_test_data_routes.dat"
    
    os.makedirs(os.path.dirname(routes_file), exist_ok=True)
    
    if not os.path.isfile(routes_file):
        logger.info("Downloading test data...")
        with open(routes_file, "wb") as f:
            f.write(requests.get(routes_url).content)
    
    cols = [
        "airline", "airline_id", "source_airport", "source_airport_id",
        "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment"
    ]
    
    routes_df = pd.read_csv("resources/notebook_test_data_routes.dat", names=cols, header=None)
    
    G = nx.DiGraph()
    for _, row in routes_df.iterrows():
        src = row["source_airport"]
        dst = row["dest_airport"]
        if pd.notnull(src) and pd.notnull(dst):
            G.add_edge(src, dst)
    
    logger.info(f"Graph created with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    return G

def run_algorithm(algorithm_name, graph, run_id):
    """Run a single algorithm and measure execution time."""

    # Start stopwatch
    start_time = time.time()

    try:
        if hasattr(nx, algorithm_name):
            func = getattr(nx, algorithm_name)
        elif hasattr(nx.community, algorithm_name):
            func = getattr(nx.community, algorithm_name)
        elif hasattr(nx.centrality, algorithm_name):
            func = getattr(nx.centrality, algorithm_name)
        elif hasattr(nx.traversal, algorithm_name):
            func = getattr(nx.traversal, algorithm_name)
        else:
            # Try to find the function in nx namespace
            parts = algorithm_name.split('_')
            for i in range(len(parts)):
                try:
                    func = getattr(nx, algorithm_name)
                    break
                except AttributeError:
                    continue
            else:
                raise AttributeError(f"Algorithm {algorithm_name} not found")

        params = alg_params.get(algorithm_name, {})

        if BACKEND:
            result = func(graph, backend=BACKEND, **params)
        else:
            result = func(graph, **params)
        if isinstance(result, GeneratorType):
            result = list(result)

        # Stop stopwatch
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        logger.debug(f"Algorithm func={algorithm_name}; run_id={run_id}; time={execution_time_ms} ms")
        logger.debug(f"result: {result}")
        
        return {
            "algorithm": algorithm_name,
            "run_id": run_id,
            "execution_time_ms": execution_time_ms,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        
        return {
            "algorithm": algorithm_name,
            "run_id": run_id,
            "execution_time_ms": execution_time_ms,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

def main():
    """Main benchmarking function."""
    if BACKEND == "neptune" and not os.getenv("NETWORKX_GRAPH_ID"):
        raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
    
    logger.info("Setting up test data...")
    graph = setup_test_data()

    if BACKEND == "neptune":
        logger.info(f"load data into graph {os.getenv("NETWORKX_GRAPH_ID")}")

        # run bfs_edges once to sync-data to backend
        nx.bfs_edges(graph, "Dummy", backend=BACKEND)

        # and now empty the graph to ensure that nothing else gets synced
        graph=nx.DiGraph()
    
    logger.info(f"Starting benchmark for {len(ALGORITHMS)} algorithms with 10 runs each...")
    
    results = []
    
    # Run algorithms sequentially, one at a time
    for algorithm in ALGORITHMS:
        logger.info(f"Running {algorithm}...")
        
        # Run each algorithm 10 times in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(run_algorithm, algorithm, graph, run_id) for run_id in range(RUN_COUNT)]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Completed {algorithm} run {result['run_id']}: {result['execution_time_ms']:.4f}ms")
                except Exception as e:
                    logger.error(f"Error in {algorithm}: {e}")
    
    # Save results in the "logs" folder
    output_dir = "logs"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Benchmark completed. Results saved to {output_file}")
    
    # Print summary statistics
    print("\n=== BENCHMARK SUMMARY ===")
    for algorithm in ALGORITHMS:
        algo_results = [r for r in results if r['algorithm'] == algorithm and r['status'] == 'success']
        if algo_results:
            times = [r['execution_time_ms'] for r in algo_results]
            print(f"{algorithm}: avg={sum(times)/len(times):.4f}ms, min={min(times):.4f}ms, max={max(times):.4f}ms")
        else:
            print(f"{algorithm}: No successful runs")

if __name__ == "__main__":
    main()
