#!/usr/bin/env python3
import json
import sys
import time
import os
import shutil
from typing import List, Set
from config import CLUSTER_ROOT, ORCHESTRATOR_NODE, USER_NODES, COMPUTE_NODES, LEDGER_DIR
from models.operation_node import OperationNode
from utils.network import Network, Message
from utils.nodes import UserNode, ComputeNode, OrchestratorNode
from utils.storage_manager import StorageManager

def log(message):
    print(f"[main.py] {message}")

def setup_environment():
    if os.path.exists(CLUSTER_ROOT):
        shutil.rmtree(CLUSTER_ROOT)
    os.makedirs(CLUSTER_ROOT)
    if os.path.exists(LEDGER_DIR):
        shutil.rmtree(LEDGER_DIR)
    os.makedirs(LEDGER_DIR)

def load_transactions(file_path: str) -> List[OperationNode]:
    with open(file_path, 'r') as f:
        data = json.load(f)
    return [OperationNode.from_dict(item) for item in data]

def identify_initial_sources(transactions: List[OperationNode]) -> Set[str]:
    produced = set()
    for tx in transactions:
        for dest in tx.destination:
            produced.add(dest)
    
    initial_sources = set()
    for tx in transactions:
        for src in tx.sources:
            if src not in produced:
                initial_sources.add(src)
    return initial_sources

def main():
    setup_environment()
    
    json_file = "ma_simulation.json"
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        
    log(f"Loading transactions from {json_file}...")
    transactions = load_transactions(json_file)
    
    # Identify initial sources
    initial_sources = identify_initial_sources(transactions)
    log(f"Identified {len(initial_sources)} initial source files.")
    
    # Initialize Network
    network = Network()
    
    # Initialize Nodes
    # Orchestrator
    orchestrator = OrchestratorNode(ORCHESTRATOR_NODE, network, COMPUTE_NODES)
    
    # Workers
    workers = [ComputeNode(node_id, network) for node_id in COMPUTE_NODES]
    
    # User (assuming U01 for now)
    user_node_id = USER_NODES[0]
    user = UserNode(user_node_id, network)
    
    # Start all processes
    orchestrator.start()
    for w in workers:
        w.start()
    user.start()
    
    # Wait for startup
    time.sleep(1)
    
    # Pre-populate User storage with initial sources
    log(f"Populating {user_node_id} storage with initial sources...")
    for rid in initial_sources:
        # Create dummy content
        content = f"Initial content for {rid}"
        user.storage.save_file(rid, content)
        
    # Submit Job
    log("Submitting job...")
    user.submit_job(transactions)
    
    total_tx = len(transactions)
    log(f"Waiting for {total_tx} transactions to complete...")
    
    # Monitor ledger directory
    completed_count = 0
    start_time = time.time()
    
    while completed_count < total_tx:
        # Count transaction files that contain "task_executed" event
        # This is inefficient but simple
        count = 0
        if os.path.exists(LEDGER_DIR):
            for filename in os.listdir(LEDGER_DIR):
                if filename.endswith(".json") and not filename.startswith("transfer_"):
                    # Check if it has task_executed
                    try:
                        with open(os.path.join(LEDGER_DIR, filename), 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                for entry in data:
                                    if entry.get("event") == "task_executed":
                                        count += 1
                                        break
                            elif isinstance(data, dict):
                                if data.get("event") == "task_executed":
                                    count += 1
                    except:
                        pass
        
        if count > completed_count:
            completed_count = count
            log(f"Progress: {completed_count}/{total_tx}")
            
        if completed_count >= total_tx:
            break
            
        if time.time() - start_time > 300: # 5 min timeout
            log("Timeout waiting for completion.")
            break
            
        time.sleep(2)

    log("All transactions completed (or timeout). Stopping cluster...")
    
    # Stop all nodes
    # We can send STOP message
    network.send(Message("MAIN", ORCHESTRATOR_NODE, "STOP", None))
    for w in workers:
        network.send(Message("MAIN", w.node_id, "STOP", None))
    network.send(Message("MAIN", user_node_id, "STOP", None))
    
    orchestrator.join()
    for w in workers:
        w.join()
    user.join()
    
    log("Simulation finished.")

if __name__ == '__main__':
    main()
