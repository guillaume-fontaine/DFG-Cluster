from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager
from utils.worker import Worker

def main():
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
