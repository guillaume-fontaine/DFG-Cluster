#!/usr/bin/env python3
import json
import sys
from typing import List
import time
import os
from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager
from utils.cluster import ClusterMaster
from config import STORE_DIR, REGISTRY_FILE, LEDGER_DIR, VAULT_FILE




def load_transactions_from_json(file_path: str) -> List[OperationNode]:
    """
    Loads a list of transactions from a JSON file and converts them to OperationNode objects.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return [OperationNode.from_dict(item) for item in data]
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from '{file_path}'.")
        return []

def verify_transaction(tx: OperationNode):
    """
    Verifies the output of a transaction by checking if destination files exist.
    """
    print(f"Verifying transaction {tx.id} ({tx.function.value})...")
    all_exist = True
    for dest_rid in tx.destination:
        try:
            content = StorageManager.get_file_content(dest_rid)
            # print(f"  - Destination {dest_rid}: Found (Content: {content})")
        except FileNotFoundError:
            print(f"  - Destination {dest_rid}: MISSING")
            all_exist = False
    
    if all_exist:
        # Check ledger entry
        ledger_path = os.path.join(LEDGER_DIR, f"{tx.id}.json")
        if os.path.exists(ledger_path):
            # print(f"  - Ledger entry found: {ledger_path}")
            pass
        else:
            print(f"  - Ledger entry MISSING: {ledger_path}")

def main():
    # Check if a JSON file is provided as argument
    if len(sys.argv) > 1 and sys.argv[1].endswith('.json'):
        json_file = sys.argv[1]
        print(f"Loading transactions from {json_file}...")
        transactions = load_transactions_from_json(json_file)

        if not transactions:
            print("No transactions found or error loading file.")
            return

        # Start Cluster
        print(f"Starting Cluster with 3 workers...")
        master = ClusterMaster(num_workers=3)
        
        # Use run_dag to handle dependencies
        print(f"Executing {len(transactions)} transactions respecting dependencies...")
        master.run_dag(transactions)

        print("Waiting for transactions to complete...")
        master.stop()
        print("All transactions executed.")
        
        print("\n--- Verification ---")
        for tx in transactions:
            verify_transaction(tx)
            
        return


if __name__ == '__main__':
    main()
