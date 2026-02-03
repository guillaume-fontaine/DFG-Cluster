import os
import shutil
import subprocess
import sys
from typing import List
from config import TMP_DIR
from models.operation_node import OperationNode, FunctionType
from utils.storage_manager import StorageManager

class Worker:
    @staticmethod
    def execute(node: OperationNode):
        # 1. Prepare temporary directory
        
        os.makedirs(TMP_DIR, exist_ok=True)
        
        src_paths = []
        for src_rid in node.sources:
            try:
                # Get content from storage
                content = StorageManager.get_file_content(src_rid)
                
                # Write to /tmp/[RID]
                tmp_path = os.path.join(TMP_DIR, src_rid)
                with open(tmp_path, 'w') as f:
                    f.write(content)
                src_paths.append(tmp_path)
            except FileNotFoundError:
                print(f"Error: Source RID {src_rid} not found.")
                return

        dest_paths = []
        for dest_rid in node.destination:
            tmp_path = os.path.join(TMP_DIR, dest_rid)
            dest_paths.append(tmp_path)

        # 2. Determine script to run
        script_name = node.function.value.lower() + ".py"
        script_path = os.path.join(os.getcwd(), "scripts", script_name)
        
        if not os.path.exists(script_path):
            print(f"Error: Script {script_path} not found.")
            return

        # 3. Construct command
        # python script.py src1 src2 - dest1 dest2
        cmd = [sys.executable, script_path] + src_paths + ["-"] + dest_paths
        
        # 4. Execute
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error executing script: {e}")
            return

        # 5. Finalize: Save results to store
        for i, dest_rid in enumerate(node.destination):
            tmp_path = dest_paths[i]
            if os.path.exists(tmp_path):
                with open(tmp_path, 'r') as f:
                    content = f.read()
                
                # Save to storage (hashes and updates registry)
                StorageManager.save_file(dest_rid, content)
                
                # Clean up
                os.remove(tmp_path)
            else:
                print(f"Warning: Destination file {tmp_path} was not created.")

        # Clean up sources
        for src_path in src_paths:
            if os.path.exists(src_path):
                os.remove(src_path)
