#!/usr/bin/env python3
import json
import sys
from typing import List
from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager
from utils.worker import Worker

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

def main():
    # Check if a JSON file is provided as argument
    if len(sys.argv) > 1 and sys.argv[1].endswith('.json'):
        json_file = sys.argv[1]
        print(f"Loading transactions from {json_file}...")
        transactions = load_transactions_from_json(json_file)
        
        if not transactions:
            print("No transactions found or error loading file.")
            return

        for tx in transactions:
            print(f"Executing transaction {tx.id} ({tx.function.value})")
            Worker.execute(tx)
            
        print("All transactions executed.")
        return

    # Default behavior (Demo)
    # 1. Create initial files
    rid_a = RIDManager.generate()
    rid_b = RIDManager.generate()
    
    StorageManager.save_file(rid_a, "10")
    StorageManager.save_file(rid_b, "20")
    
    print(f"Created initial files with RIDs: {rid_a} (10), {rid_b} (20)")

    # 2. Prepare for an ADD operation
    rid_dest1 = RIDManager.generate()
    rid_dest2 = RIDManager.generate()
    
    add_node = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.ADD,
        sources=[rid_a, rid_b],
        destination=[rid_dest1, rid_dest2]
    )
    
    print(f"Executing ADD operation: {rid_a} + {rid_b} -> {rid_dest1}, {rid_dest2}")
    
    # 3. Execute the operation
    Worker.execute(add_node)
    
    # 4. Verify results
    try:
        res1 = StorageManager.get_file_content(rid_dest1)
        res2 = StorageManager.get_file_content(rid_dest2)
        
        print(f"Result 1 ({rid_dest1}): {res1}")
        print(f"Result 2 ({rid_dest2}): {res2}")
        
        # Expected: 10+20=30, 30*2=60
        if res1 == "30" and res2 == "60":
            print("ADD operation successful!")
        else:
            print("ADD operation failed: incorrect results.")
            
    except FileNotFoundError as e:
        print(f"Error retrieving results: {e}")

if __name__ == '__main__':
    main()
