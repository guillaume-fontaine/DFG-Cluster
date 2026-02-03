from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager

def main():
    # Generate RIDs
    rid1 = RIDManager.generate()
    rid2 = RIDManager.generate()
    
    print(f"Generated RIDs: {rid1}, {rid2}")

    # Content to be stored
    content1 = "Hello World"
    content2 = "Hello World" # Same content to test deduplication
    content3 = "Different Content"

    # Save files
    hash1 = StorageManager.save_file(rid1, content1)
    print(f"Saved content for {rid1} with hash {hash1}")
    
    hash2 = StorageManager.save_file(rid2, content2)
    print(f"Saved content for {rid2} with hash {hash2}")
    
    # Verify deduplication
    if hash1 == hash2:
        print("Success: Identical content produced the same hash.")
    else:
        print("Error: Identical content produced different hashes.")

    # Retrieve content
    retrieved_content = StorageManager.get_file_content(rid1)
    print(f"Retrieved content for {rid1}: {retrieved_content}")

if __name__ == '__main__':
    main()
