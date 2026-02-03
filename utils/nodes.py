import os
import shutil
import multiprocessing
import time
import sys
import subprocess
from typing import List, Dict, Any
from config import CLUSTER_ROOT
from utils.network import Network, Message
from models.operation_node import OperationNode
from utils.rid_manager import RIDManager

class Node(multiprocessing.Process):
    def __init__(self, node_id: str, network: Network):
        super().__init__()
        self.node_id = node_id
        self.network = network
        self.inbox = multiprocessing.Queue()
        self.root_dir = os.path.join(CLUSTER_ROOT, node_id)
        self.tmp_dir = os.path.join(self.root_dir, "tmp")
        self.running = True

    def setup_fs(self):
        if os.path.exists(self.root_dir):
            shutil.rmtree(self.root_dir)
        os.makedirs(self.root_dir)
        os.makedirs(self.tmp_dir)
        
        # Register to network
        self.network.register_node(self.node_id, self.inbox)

    def run(self):
        self.setup_fs()
        print(f"[{self.node_id}] Started. Root: {self.root_dir}")
        while self.running:
            try:
                msg = self.inbox.get(timeout=1)
                self.handle_message(msg)
            except multiprocessing.queues.Empty:
                continue
            except Exception as e:
                print(f"[{self.node_id}] Error: {e}")
                import traceback
                traceback.print_exc()

    def handle_message(self, msg: Message):
        if msg.type == "STOP":
            self.running = False
        else:
            print(f"[{self.node_id}] Unhandled message: {msg.type}")

    def send(self, receiver: str, type: str, payload: Any):
        msg = Message(self.node_id, receiver, type, payload)
        self.network.send(msg)

class StorageNode(Node):
    def __init__(self, node_id: str, network: Network):
        super().__init__(node_id, network)
        self.store_dir = os.path.join(self.root_dir, "store")
        self.registry = {} 

    def setup_fs(self):
        super().setup_fs()
        os.makedirs(self.store_dir)

    def handle_message(self, msg: Message):
        if msg.type == "STOP":
            self.running = False
        elif msg.type == "STORE_FILE":
            rid = msg.payload["rid"]
            content = msg.payload["content"]
            self._save_file(rid, content)
        elif msg.type == "GET_FILE":
            rid = msg.payload["rid"]
            content = self._get_file(rid)
            self.send(msg.sender, "FILE_CONTENT", {"rid": rid, "content": content})

    def _save_file(self, rid, content):
        import hashlib
        h = hashlib.sha1(content.encode('utf-8')).hexdigest()
        path = os.path.join(self.store_dir, h)
        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write(content)
        self.registry[rid] = h
        
    def _get_file(self, rid):
        h = self.registry.get(rid)
        if not h:
            return None
        path = os.path.join(self.store_dir, h)
        if os.path.exists(path):
            with open(path, 'r') as f:
                return f.read()
        return None

class ComputeNode(Node):
    def __init__(self, node_id: str, network: Network):
        super().__init__(node_id, network)
        self.deferred_messages = []

    def handle_message(self, msg: Message):
        if msg.type == "STOP":
            self.running = False
        elif msg.type == "EXECUTE_TASK":
            node = msg.payload
            self._execute_task(node)
        else:
            # Store unexpected messages if we are not waiting (though here we are in main loop)
            # Actually, if we are in main loop, we shouldn't get FILE_CONTENT unless we asked.
            # But if we did ask, we are in _wait_for_file loop.
            # So if we are here, it's a stray message or out of order.
            pass

    def _execute_task(self, node: OperationNode):
        print(f"[{self.node_id}] Executing task {node.id} ({node.function.value})")
        
        # 1. Request sources
        src_contents = {}
        for src_rid in node.sources:
            self.send("S00", "GET_FILE", {"rid": src_rid})
            content = self._wait_for_file(src_rid)
            if content is None:
                print(f"[{self.node_id}] Failed to get source {src_rid}")
                return
            src_contents[src_rid] = content

        # 2. Prepare local environment
        task_dir = os.path.join(self.tmp_dir, node.id)
        os.makedirs(task_dir, exist_ok=True)
        
        src_paths = []
        for src_rid, content in src_contents.items():
            p = os.path.join(task_dir, src_rid)
            with open(p, 'w') as f:
                f.write(content)
            src_paths.append(p)
            
        dest_paths = []
        for dest_rid in node.destination:
            dest_paths.append(os.path.join(task_dir, dest_rid))

        # 3. Execute Script
        script_name = node.function.value.lower() + ".py"
        script_path = os.path.abspath(os.path.join("scripts", script_name))
        
        cmd = [sys.executable, script_path] + src_paths + ["-"] + dest_paths
        
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"[{self.node_id}] Execution failed: {e}")
            return

        # 4. Send results back
        for i, dest_rid in enumerate(node.destination):
            p = dest_paths[i]
            if os.path.exists(p):
                with open(p, 'r') as f:
                    content = f.read()
                self.send("S00", "STORE_FILE", {"rid": dest_rid, "content": content})
        
        self.send("M00", "TASK_COMPLETE", {"node_id": node.id})
        
        # Cleanup
        shutil.rmtree(task_dir)

    def _wait_for_file(self, rid):
        # Check deferred first
        for i, msg in enumerate(self.deferred_messages):
            if msg.type == "FILE_CONTENT" and msg.payload["rid"] == rid:
                self.deferred_messages.pop(i)
                return msg.payload["content"]

        while True:
            try:
                msg = self.inbox.get(timeout=5) # Timeout to avoid deadlocks
                if msg.type == "FILE_CONTENT" and msg.payload["rid"] == rid:
                    return msg.payload["content"]
                elif msg.type == "STOP":
                    self.running = False
                    return None
                else:
                    self.deferred_messages.append(msg)
            except multiprocessing.queues.Empty:
                print(f"[{self.node_id}] Timeout waiting for file {rid}")
                return None

class SchedulerNode(Node):
    def __init__(self, node_id: str, network: Network, workers: List[str]):
        super().__init__(node_id, network)
        self.workers = workers
        self.completed_tasks = set()
        self.total_tasks = 0

    def handle_message(self, msg: Message):
        if msg.type == "STOP":
            self.running = False
        elif msg.type == "SUBMIT_GRAPH":
            tasks = msg.payload
            self.total_tasks += len(tasks)
            print(f"[{self.node_id}] Received graph with {len(tasks)} tasks.")
            for task in tasks:
                self._schedule_task(task)
        elif msg.type == "TASK_COMPLETE":
            tid = msg.payload["node_id"]
            self.completed_tasks.add(tid)
            print(f"[{self.node_id}] Task {tid} complete. ({len(self.completed_tasks)}/{self.total_tasks})")
            if len(self.completed_tasks) >= self.total_tasks and self.total_tasks > 0:
                print(f"[{self.node_id}] All tasks complete.")

    def _schedule_task(self, task: OperationNode):
        import random
        worker = random.choice(self.workers)
        print(f"[{self.node_id}] Scheduling task {task.id} on {worker}")
        self.send(worker, "EXECUTE_TASK", task)

class UserNode(Node):
    def submit_graph(self, tasks: List[OperationNode]):
        self.send("M00", "SUBMIT_GRAPH", tasks)
    
    def upload_file(self, rid, content):
        self.send("S00", "STORE_FILE", {"rid": rid, "content": content})
