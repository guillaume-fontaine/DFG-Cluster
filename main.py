import time
import multiprocessing
from utils.network import Network
from utils.nodes import StorageNode, SchedulerNode, ComputeNode, UserNode
from utils.rid_manager import RIDManager
from models.operation_node import OperationNode, FunctionType

def main():
    print("--- Starting Cluster Simulation ---")
    
    # 1. Initialize Network
    network = Network()
    
    # 2. Create Nodes
    s00 = StorageNode("S00", network)
    m00 = SchedulerNode("M00", network, workers=["M01", "M02", "M03"])
    m01 = ComputeNode("M01", network)
    m02 = ComputeNode("M02", network)
    m03 = ComputeNode("M03", network)
    u01 = UserNode("U01", network)
    
    nodes = [s00, m00, m01, m02, m03, u01]
    
    # 3. Start Nodes
    for node in nodes:
        node.start()
        
    # Allow some time for startup
    time.sleep(1)
    
    # 4. User U01 prepares data
    print("\n[Main] U01 uploading initial data...")
    rid_a = RIDManager.generate()
    rid_b = RIDManager.generate()
    
    u01.upload_file(rid_a, "10")
    u01.upload_file(rid_b, "20")
    
    # 5. User U01 submits graph
    print("\n[Main] U01 submitting graph...")
    
    # Task 1: ADD(A, B) -> C, D
    rid_c = RIDManager.generate()
    rid_d = RIDManager.generate()
    task1 = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.ADD,
        sources=[rid_a, rid_b],
        destination=[rid_c, rid_d]
    )
    
    # Task 2: MULT(C, D) -> E
    rid_e = RIDManager.generate()
    rid_f = RIDManager.generate() # Unused output
    task2 = OperationNode(
        id=RIDManager.generate(),
        function=FunctionType.MULT,
        sources=[rid_c, rid_d],
        destination=[rid_e, rid_f]
    )
    
    u01.submit_graph([task1, task2])
    
    # 6. Wait for completion (Simulated by waiting loop)
    # In a real scenario, we'd have a callback or check status
    print("\n[Main] Waiting for execution...")
    time.sleep(10) # Give enough time for tasks to complete
    
    # 7. Stop Cluster
    print("\n[Main] Stopping cluster...")
    for node in nodes:
        node.send(node.node_id, "STOP", None)
        
    for node in nodes:
        node.join()
        
    print("--- Simulation Finished ---")

if __name__ == '__main__':
    main()
