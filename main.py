import time
import os
from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager
from utils.cluster import ClusterMaster

def main():
    # 1. Setup
    print("Setting up initial data...")
    rid_1 = RIDManager.generate()
    rid_2 = RIDManager.generate()
    rid_3 = RIDManager.generate()
    
    StorageManager.save_file(rid_1, "5")
    StorageManager.save_file(rid_2, "10")
    StorageManager.save_file(rid_3, "2")
    
    print(f"Initial RIDs: {rid_1} (5), {rid_2} (10), {rid_3} (2)")

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
    print(f"Starting Cluster with 3 workers...")
    master = ClusterMaster(num_workers=3)
    master.start()
    
    # 4. Submit Tasks
    print(f"Submitting {len(tasks)} tasks...")
    for task in tasks:
        master.submit_task(task)
    
    # 5. Stop Cluster (waits for completion)
    print("Waiting for tasks to complete...")
    master.stop()
    print("All tasks completed.")
    
    # 6. Verify Results
    print("\n--- Verification ---")
    
    # Task 1 Results (ADD 5, 10 -> 15, 30)
    try:
        res1 = StorageManager.get_file_content(t1_dest1)
        res2 = StorageManager.get_file_content(t1_dest2)
        print(f"Task 1 (ADD): {res1}, {res2} (Expected: 15, 30)")
    except Exception as e:
        print(f"Task 1 failed: {e}")

    # Task 2 Results (MULT 10, 2 -> 20, 400)
    try:
        res3 = StorageManager.get_file_content(t2_dest1)
        res4 = StorageManager.get_file_content(t2_dest2)
        print(f"Task 2 (MULT): {res3}, {res4} (Expected: 20, 400)")
    except Exception as e:
        print(f"Task 2 failed: {e}")
        
    # Task 3 Results (HASH)
    try:
        res5 = StorageManager.get_file_content(t3_dest1)
        print(f"Task 3 (HASH): {res5}")
    except Exception as e:
        print(f"Task 3 failed: {e}")

    # Check Ledger
    print("\n--- Ledger Check ---")
    ledger_files = os.listdir("ledger")
    print(f"Ledger entries found: {len(ledger_files)}")
    for f in ledger_files:
        print(f" - {f}")

if __name__ == '__main__':
    main()
