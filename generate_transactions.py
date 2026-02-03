#!/usr/bin/env python3
import json
import random
import argparse
import os
import shutil
from typing import List, Set
from models.operation_node import FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager
from config import CLUSTER_ROOT, USER_NODES, LEDGER_DIR


def setup_environment():
    if os.path.exists(CLUSTER_ROOT):
        shutil.rmtree(CLUSTER_ROOT)
    os.makedirs(CLUSTER_ROOT)
    if os.path.exists(LEDGER_DIR):
        shutil.rmtree(LEDGER_DIR)
    os.makedirs(LEDGER_DIR)

def generate_transactions(num_transactions: int, output_file: str):
    transactions = []
    all_sources = set()
    all_destinations = set()
    
    # Pool of RIDs that are outputs of previous transactions, available to be used as inputs
    available_intermediate_results = []

    print(f"Generating {num_transactions} transactions...")

    for _ in range(num_transactions):
        # 1. Determine function
        func_type = random.choice(list(FunctionType))
        
        # 2. Determine sources
        # We want some sources to be previous destinations.
        num_sources = random.randint(1, 3)
        current_sources = []
        
        for _ in range(num_sources):
            use_intermediate = False
            # 50% chance to use intermediate if available
            if available_intermediate_results and random.random() > 0.5:
                use_intermediate = True
            
            if use_intermediate:
                rid = random.choice(available_intermediate_results)
                current_sources.append(rid)
            else:
                # New primary source
                rid = RIDManager.generate()
                current_sources.append(rid)
        
        all_sources.update(current_sources)
        
        # 3. Determine destinations
        num_dests = random.randint(1, 2)
        current_dests = []
        for _ in range(num_dests):
            rid = RIDManager.generate()
            current_dests.append(rid)
        
        all_destinations.update(current_dests)
        available_intermediate_results.extend(current_dests)
        
        # 4. Create transaction
        tx = {
            "id": RIDManager.generate(),
            "function": func_type.value,
            "sources": current_sources,
            "destination": current_dests
        }
        transactions.append(tx)

    # 5. Identify primary sources and create files
    primary_sources = all_sources - all_destinations
    print(f"Identified {len(primary_sources)} primary sources. Generating files in User Node storage...")
    
    # Setup storage for U01
    user_node_id = USER_NODES[0]
    user_root = os.path.join(CLUSTER_ROOT, user_node_id)
    storage = StorageManager(user_root)
    
    for rid in primary_sources:
        # Generate random integer
        val = random.randint(1, 100)
        storage.save_file(rid, str(val))
        # print(f"Created primary source {rid} with value {val}")

    # 6. Save transactions
    with open(output_file, 'w') as f:
        json.dump(transactions, f, indent=4)
    
    print(f"Successfully generated {num_transactions} transactions in '{output_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random transactions and calculation graphs.")
    parser.add_argument("count", type=int, help="Number of transactions to generate")
    parser.add_argument("--output", type=str, default="transactions.json", help="Output JSON file")
    
    args = parser.parse_args()
    setup_environment()
    generate_transactions(args.count, args.output)
