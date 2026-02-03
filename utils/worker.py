import os
import shutil
import subprocess
import sys
import time
from typing import List, Dict, Any
from config import TMP_DIR
from models.operation_node import OperationNode, FunctionType
from utils.storage_manager import StorageManager

class WorkerExecutor:
    @staticmethod
    def execute(node: OperationNode, lock=None) -> Dict[str, Any]:
        """
        Executes the operation and returns details for the ledger.
        """
        # 1. Prepare temporary directory for this specific task
        # Use a subdirectory based on the task ID to ensure isolation
        task_tmp_dir = os.path.join(TMP_DIR, node.id)
        os.makedirs(task_tmp_dir, exist_ok=True)
        
        src_paths = []
        src_hashes = {}
        
        try:
            for src_rid in node.sources:
                try:
                    # Get content and hash from storage
                    content = StorageManager.get_file_content(src_rid)
                    src_hash = StorageManager.get_hash(src_rid)
                    src_hashes[src_rid] = src_hash
                    
                    # Write to /tmp/[TASK_ID]/[RID]
                    tmp_path = os.path.join(task_tmp_dir, src_rid)
                    with open(tmp_path, 'w') as f:
                        f.write(content)
                    src_paths.append(tmp_path)
                except FileNotFoundError:
                    print(f"Error: Source RID {src_rid} not found.")
                    return None

            dest_paths = []
            for dest_rid in node.destination:
                tmp_path = os.path.join(task_tmp_dir, dest_rid)
                dest_paths.append(tmp_path)

            # 2. Determine script to run
            script_name = node.function.value.lower() + ".py"
            script_path = os.path.join(os.getcwd(), "scripts", script_name)
            
            if not os.path.exists(script_path):
                print(f"Error: Script {script_path} not found.")
                return None

            # 3. Construct command
            cmd = [sys.executable, script_path] + src_paths + ["-"] + dest_paths
            
            # 4. Execute
            start_time = time.time()
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error executing script: {e}")
                return None
            end_time = time.time()

            # 5. Finalize: Save results to store
            dest_hashes = {}
            for i, dest_rid in enumerate(node.destination):
                tmp_path = dest_paths[i]
                if os.path.exists(tmp_path):
                    with open(tmp_path, 'r') as f:
                        content = f.read()
                    
                    # Save to storage (hashes and updates registry)
                    # Pass the lock here!
                    file_hash = StorageManager.save_file(dest_rid, content, lock=lock)
                    dest_hashes[dest_rid] = file_hash
                else:
                    print(f"Warning: Destination file {tmp_path} was not created.")

            # Return ledger info
            return {
                "transaction_id": node.id,
                "function": node.function.value,
                "sources": src_hashes,
                "destinations": dest_hashes,
                "timestamp_start": start_time,
                "timestamp_end": end_time
            }
            
        finally:
            # Clean up the entire task directory
            if os.path.exists(task_tmp_dir):
                shutil.rmtree(task_tmp_dir)
