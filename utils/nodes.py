import multiprocessing
import os
import time
import sys
import subprocess
import shutil
import queue
from typing import List, Dict, Set, Any
from config import CLUSTER_ROOT, MACHINE_TMP_DIR_NAME
from utils.network import Network, Message
from utils.storage_manager import StorageManager
from models.operation_node import OperationNode

class Node(multiprocessing.Process):
    def __init__(self, node_id: str, network: Network, inbox: Any):
        super().__init__()
        self.node_id = node_id
        self.network = network
        self.inbox = inbox
        self.root_dir = os.path.join(CLUSTER_ROOT, node_id)
        self.storage = StorageManager(self.root_dir)
        self.running = True
        self.deferred_messages = [] # For messages that arrive out of order (e.g. file before request)

    def run(self):
        self.network.register_node(self.node_id, self.inbox)
        print(f"[{self.node_id}] Online.")
        
        while self.running:
            try:
                msg = self.inbox.get(timeout=1)
                self.process_message(msg)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[{self.node_id}] Error: {e}")
                import traceback
                traceback.print_exc()

    def process_message(self, msg: Message):
        if msg.type == "STOP":
            self.running = False
        elif msg.type == "REQUEST_FILE":
            self.handle_request_file(msg)
        elif msg.type == "FILE_TRANSFER":
            self.handle_file_transfer(msg)
        else:
            self.handle_custom_message(msg)

    def handle_custom_message(self, msg: Message):
        pass

    def handle_request_file(self, msg: Message):
        rid = msg.payload["rid"]
        requester = msg.sender
        # print(f"[{self.node_id}] Received request for {rid} from {requester}")
        try:
            content = self.storage.get_file_content(rid)
            self.send_file(requester, rid, content)
        except FileNotFoundError:
            print(f"[{self.node_id}] Requested file {rid} not found.")
            # Optionally send ERROR message

    def handle_file_transfer(self, msg: Message):
        rid = msg.payload["rid"]
        content = msg.payload["content"]
        # print(f"[{self.node_id}] Received file {rid} from {msg.sender}")
        self.storage.save_file(rid, content)
        
        # Log to ledger
        StorageManager.save_ledger_entry(f"transfer_{rid}", {
            "event": "transfer_receive",
            "rid": rid,
            "receiver": self.node_id,
            "sender": msg.sender,
            "timestamp": time.time()
        })

    def send_file(self, target: str, rid: str, content: str):
        payload = {"rid": rid, "content": content}
        self.network.send(Message(self.node_id, target, "FILE_TRANSFER", payload))
        
        StorageManager.save_ledger_entry(f"transfer_{rid}", {
            "event": "transfer_send",
            "rid": rid,
            "sender": self.node_id,
            "receiver": target,
            "timestamp": time.time()
        })

    def request_file(self, target: str, rid: str):
        self.network.send(Message(self.node_id, target, "REQUEST_FILE", {"rid": rid}))

    def wait_for_file(self, rid: str, timeout=10) -> bool:
        """
        Waits until file appears in local storage.
        """
        start = time.time()
        while time.time() - start < timeout:
            if self.storage.has_file(rid):
                return True
            
            # Process incoming messages while waiting
            try:
                msg = self.inbox.get(timeout=0.1)
                self.process_message(msg)
            except queue.Empty:
                pass
        return False

class ComputeNode(Node):
    def handle_custom_message(self, msg: Message):
        if msg.type == "EXECUTE_TASK":
            self.execute_task(msg.payload)

    def execute_task(self, payload: Dict):
        task = payload["task"] # OperationNode dict or obj
        sources_map = payload["sources_loc"] # {rid: node_id}
        
        # Convert dict to obj if needed
        if isinstance(task, dict):
            task = OperationNode.from_dict(task)

        print(f"[{self.node_id}] Executing task {task.id}")

        # 1. Fetch missing sources
        for src in task.sources:
            if not self.storage.has_file(src):
                source_node = sources_map.get(src)
                if source_node:
                    # print(f"[{self.node_id}] Fetching {src} from {source_node}")
                    self.request_file(source_node, src)
                    if not self.wait_for_file(src):
                        print(f"[{self.node_id}] Failed to fetch {src}")
                        self.network.send(Message(self.node_id, "M00", "TASK_FAILED", {"task_id": task.id}))
                        return
                else:
                    print(f"[{self.node_id}] Unknown location for source {src}")
                    self.network.send(Message(self.node_id, "M00", "TASK_FAILED", {"task_id": task.id}))
                    return

        # 2. Execute
        # Create isolated tmp dir
        task_tmp = os.path.join(self.root_dir, MACHINE_TMP_DIR_NAME, task.id)
        os.makedirs(task_tmp, exist_ok=True)
        
        try:
            # Copy sources to tmp
            src_paths = []
            for src in task.sources:
                content = self.storage.get_file_content(src)
                p = os.path.join(task_tmp, src)
                with open(p, 'w') as f:
                    f.write(content)
                src_paths.append(p)
            
            dest_paths = [os.path.join(task_tmp, d) for d in task.destination]
            
            # Run script
            script_name = task.function.value.lower() + ".py"
            script_path = os.path.abspath(os.path.join("scripts", script_name))
            
            cmd = [sys.executable, script_path] + src_paths + ["-"] + dest_paths
            subprocess.run(cmd, check=True)
            
            # Collect results
            produced_rids = []
            for i, dest in enumerate(task.destination):
                p = dest_paths[i]
                if os.path.exists(p):
                    with open(p, 'r') as f:
                        content = f.read()
                    self.storage.save_file(dest, content)
                    produced_rids.append(dest)
            
            # Log execution to ledger
            StorageManager.save_ledger_entry(task.id, {
                "event": "task_executed",
                "task_id": task.id,
                "node": self.node_id,
                "produced": produced_rids,
                "timestamp": time.time()
            })
            
            # Notify M00
            self.network.send(Message(self.node_id, "M00", "TASK_COMPLETED", {
                "task_id": task.id,
                "produced": produced_rids
            }))
            
        except Exception as e:
            print(f"[{self.node_id}] Execution error: {e}")
            self.network.send(Message(self.node_id, "M00", "TASK_FAILED", {"task_id": task.id}))
        finally:
            if os.path.exists(task_tmp):
                shutil.rmtree(task_tmp)

class UserNode(Node):
    def submit_job(self, transactions: List[OperationNode]):
        # Send job
        # Convert to dicts for serialization
        tasks_data = [t.to_dict() for t in transactions]
        self.network.send(Message(self.node_id, "M00", "SUBMIT_JOB", {"tasks": tasks_data}))

class OrchestratorNode(Node):
    def __init__(self, node_id: str, network: Network, workers: List[str], inbox: Any):
        super().__init__(node_id, network, inbox)
        self.workers = workers
        self.rid_locations = {} # RID -> NodeID
        self.pending_tasks = {} # ID -> Task
        self.task_deps = {} # ID -> Set[ID]
        self.task_dependents = {} # ID -> Set[ID]
        self.completed_tasks = set()
        self.submitted_tasks = set()
        
    def handle_custom_message(self, msg: Message):
        if msg.type == "SUBMIT_JOB":
            self.handle_submit_job(msg)
        elif msg.type == "TASK_COMPLETED":
            self.handle_task_completed(msg)
        elif msg.type == "TASK_FAILED":
            print(f"[{self.node_id}] Task {msg.payload['task_id']} failed on {msg.sender}")

    def handle_submit_job(self, msg: Message):
        tasks_data = msg.payload["tasks"]
        user_id = msg.sender
        print(f"[{self.node_id}] Received job from {user_id} with {len(tasks_data)} tasks.")
        
        tasks = [OperationNode.from_dict(t) for t in tasks_data]
        
        # 1. Register initial sources as being at UserID
        # We need to know which RIDs are "inputs" to the graph (not produced by any task)
        produced_rids = set()
        for t in tasks:
            for d in t.destination:
                produced_rids.add(d)
        
        for t in tasks:
            for s in t.sources:
                if s not in produced_rids:
                    self.rid_locations[s] = user_id
        
        # 2. Build Dependency Graph
        rid_producer = {}
        for t in tasks:
            for d in t.destination:
                rid_producer[d] = t.id
        
        for t in tasks:
            self.pending_tasks[t.id] = t
            self.task_dependents[t.id] = set()
            deps = set()
            for s in t.sources:
                if s in rid_producer:
                    producer = rid_producer[s]
                    deps.add(producer)
                    if producer not in self.task_dependents:
                        self.task_dependents[producer] = set()
                    self.task_dependents[producer].add(t.id)
            self.task_deps[t.id] = deps
            
        # 3. Schedule ready tasks
        self.schedule_ready_tasks()

    def schedule_ready_tasks(self):
        for tid, deps in self.task_deps.items():
            if not deps and tid not in self.submitted_tasks and tid not in self.completed_tasks:
                self.submit_task(tid)

    def submit_task(self, tid: str):
        task = self.pending_tasks[tid]
        
        # Select worker (Round Robin or Random)
        import random
        worker = random.choice(self.workers)
        
        # Prepare source locations
        sources_loc = {}
        for s in task.sources:
            if s in self.rid_locations:
                sources_loc[s] = self.rid_locations[s]
            else:
                # Should not happen if graph is complete
                print(f"[{self.node_id}] Warning: Unknown location for source {s}")
        
        payload = {
            "task": task.to_dict(),
            "sources_loc": sources_loc
        }
        
        self.network.send(Message(self.node_id, worker, "EXECUTE_TASK", payload))
        self.submitted_tasks.add(tid)

    def handle_task_completed(self, msg: Message):
        tid = msg.payload["task_id"]
        produced = msg.payload["produced"]
        worker = msg.sender
        
        print(f"[{self.node_id}] Task {tid} completed by {worker}")
        
        # Update locations
        for rid in produced:
            self.rid_locations[rid] = worker
            
        self.completed_tasks.add(tid)
        
        # Check dependents
        if tid in self.task_dependents:
            for dep_id in self.task_dependents[tid]:
                self.task_deps[dep_id].discard(tid)
        
        self.schedule_ready_tasks()
        
        if len(self.completed_tasks) == len(self.pending_tasks):
            print(f"[{self.node_id}] All tasks completed.")
            # Optionally stop everyone?
            # self.network.send(Message(self.node_id, "ALL", "STOP", None))
