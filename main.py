from models.operation_node import OperationNode, FunctionType
from utils.rid_manager import RIDManager

def main():
    # Generate some RIDs
    rid1 = RIDManager.generate()
    rid2 = RIDManager.generate()
    rid3 = RIDManager.generate()
    
    print(f"Generated RIDs: {rid1}, {rid2}, {rid3}")

    # Create an OperationNode
    node_data = {
        "id": rid1,
        "function": "ADD",
        "sources": [rid2, rid3],
        "destination": [RIDManager.generate()]
    }
    
    node = OperationNode.from_dict(node_data)
    print(f"Created Node: {node}")

if __name__ == '__main__':
    main()
