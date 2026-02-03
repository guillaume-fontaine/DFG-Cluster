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

def log(message):
    print(f"[main.py] {message}")

def load_transactions_from_json(file_path: str) -> List[OperationNode]:
    """
    Loads a list of transactions from a JSON file and converts them to OperationNode objects.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return [OperationNode.from_dict(item) for item in data]
    except FileNotFoundError:
        log(f"Error: File '{file_path}' not found.")
        return []
    except json.JSONDecodeError:
        log(f"Error: Failed to decode JSON from '{file_path}'.")
        return []

def verify_transaction(tx: OperationNode):
    """
    Verifies the output of a transaction by checking if destination files exist.
    """
    log(f"Verifying transaction {tx.id} ({tx.function.value})...")
    all_exist = True
    for dest_rid in tx.destination:
        try:
            content = StorageManager.get_file_content(dest_rid)
            # log(f"  - Destination {dest_rid}: Found (Content: {content})")
        except FileNotFoundError:
            log(f"  - Destination {dest_rid}: MISSING")
            all_exist = False
    
    if all_exist:
        # Check ledger entry
        ledger_path = os.path.join(LEDGER_DIR, f"{tx.id}.json")
        if os.path.exists(ledger_path):
            # log(f"  - Ledger entry found: {ledger_path}")
            pass
        else:
            log(f"  - Ledger entry MISSING: {ledger_path}")

def main():
    # Check if a JSON file is provided as argument
    if len(sys.argv) > 1 and sys.argv[1].endswith('.json'):
        json_file = sys.argv[1]
        log(f"Loading transactions from {json_file}...")
        transactions = load_transactions_from_json(json_file)

        if not transactions:
            log("No transactions found or error loading file.")
            return

        # Start Cluster
        log(f"Starting Cluster with 3 workers...")
        master = ClusterMaster(num_workers=3)
        
        # Use run_dag to handle dependencies
        log(f"Executing {len(transactions)} transactions respecting dependencies...")
        master.run_dag(transactions)

        log("Waiting for transactions to complete...")
        master.stop()
        log("All transactions executed.")
        
        log("\n--- Verification ---")
        for tx in transactions:
            verify_transaction(tx)
            
        return

    # Default behavior (Demo)
    # 1. Create initial files
    rid_1 = RIDManager.generate()
    rid_2 = RIDManager.generate()
    rid_3 = RIDManager.generate()
    
    StorageManager.save_file(rid_1, "5")
    StorageManager.save_file(rid_2, "10")
    StorageManager.save_file(rid_3, "2")
    
    log(f"Initial RIDs: {rid_1} (5), {rid_2} (10), {rid_3} (2)")

    # 2. Create Tasks
    tasks = []
    
    # Task 1: ADD(rid_1, rid_2) -> dest1, dest2
    t1_dest1 = RIDManager.generate()
    t1_dest2 = RIDManager.generate()
    task1 = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.ADD,
        sources=[rid_1, rid_2],
        destination=[t1_dest1, t1_dest2]
    )
    tasks.append(task1)

    # Task 2: MULT(rid_2, rid_3) -> dest3, dest4
    t2_dest1 = RIDManager.generate()
    t2_dest2 = RIDManager.generate()
    task2 = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.MULT,
        sources=[rid_2, rid_3],
        destination=[t2_dest1, t2_dest2]
    )
    tasks.append(task2)

    # Task 3: HASH(rid_1, rid_3) -> dest5
    t3_dest1 = RIDManager.generate()
    task3 = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.HASH,
        sources=[rid_1, rid_3],
        destination=[t3_dest1]
    )
    tasks.append(task3)

    # 3. Start Cluster
    log(f"Starting Cluster with 3 workers...")
    master = ClusterMaster(num_workers=3)
    
    # 4. Submit Tasks using DAG execution
    log(f"Submitting {len(tasks)} tasks...")
    master.run_dag(tasks)
    
    # 5. Stop Cluster (waits for completion)
    log("Waiting for tasks to complete...")
    master.stop()
    log("All tasks completed.")
    
    # 6. Verify Results
    log("\n--- Verification ---")

    # Task 1 Results (ADD 5, 10 -> 15, 30)
    try:
        res1 = StorageManager.get_file_content(t1_dest1)
        res2 = StorageManager.get_file_content(t1_dest2)
        log(f"Task 1 (ADD): {res1}, {res2} (Expected: 15, 30)")
    except Exception as e:
        log(f"Task 1 failed: {e}")

    # Task 2 Results (MULT 10, 2 -> 20, 400)
    try:
        res3 = StorageManager.get_file_content(t2_dest1)
        res4 = StorageManager.get_file_content(t2_dest2)
        log(f"Task 2 (MULT): {res3}, {res4} (Expected: 20, 400)")
    except Exception as e:
        log(f"Task 2 failed: {e}")
        
    # Task 3 Results (HASH)
    try:
        res5 = StorageManager.get_file_content(t3_dest1)
        log(f"Task 3 (HASH): {res5}")
    except Exception as e:
        log(f"Task 3 failed: {e}")

    # Check Ledger
    log("\n--- Ledger Check ---")
    if os.path.exists(LEDGER_DIR):
        ledger_files = os.listdir(LEDGER_DIR)
        log(f"Ledger entries found: {len(ledger_files)}")
        for f in ledger_files:
            log(f" - {f}")
    else:
        log("Ledger directory not found.")

if __name__ == '__main__':
    main()
