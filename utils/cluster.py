import multiprocessing
import time
import os
from typing import List, Dict, Set
from models.operation_node import OperationNode
from utils.worker import WorkerExecutor
from utils.storage_manager import StorageManager

def log(message, worker_name=None):
    prefix = f"[cluster.py]"
    if worker_name:
        prefix += f" [{worker_name}]"
    print(f"{prefix} {message}")

class ClusterWorker(multiprocessing.Process):
    def __init__(self, task_queue, result_queue, registry_lock, ledger_lock):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.registry_lock = registry_lock
        self.ledger_lock = ledger_lock

    def run(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                # Sentinel value to stop the worker
                break
            
            # Execute task
            log(f"Executing task {task.id}", self.name)
            result = WorkerExecutor.execute(task, lock=self.registry_lock)
            
            if result:
                with self.ledger_lock:
                    StorageManager.save_ledger_entry(task.id, result)
                
                # Notify master of completion
                self.result_queue.put((task.id, True))
            else:
                # Notify master of failure
                self.result_queue.put((task.id, False))

class ClusterMaster:
    def __init__(self, num_workers=3):
        self.num_workers = num_workers
        self.task_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.registry_lock = multiprocessing.Lock()
        self.ledger_lock = multiprocessing.Lock()
        self.workers = []
        self.running = False

    def start(self):
        self.running = True
        for i in range(self.num_workers):
            worker = ClusterWorker(self.task_queue, self.result_queue, self.registry_lock, self.ledger_lock)
            worker.start()
            self.workers.append(worker)

    def run_dag(self, tasks: List[OperationNode], enable_retry: bool = False):
        """
        Executes tasks respecting dependencies.
        """
        if not self.running:
            self.start()

        # 1. Build Dependency Graph
        # Map: RID -> Task ID that produces it
        rid_producer = {}
        for t in tasks:
            for dest in t.destination:
                rid_producer[dest] = t.id
        
        # Map: Task ID -> Set of Task IDs it depends on
        task_deps = {}
        # Map: Task ID -> Set of Task IDs that depend on it
        task_dependents = {t.id: set() for t in tasks}
        
        for t in tasks:
            deps = set()
            for src in t.sources:
                if src in rid_producer:
                    producer_id = rid_producer[src]
                    # Only depend if producer is in the current batch of tasks
                    # (It might be a pre-existing file)
                    if producer_id in task_dependents: 
                        deps.add(producer_id)
                        task_dependents[producer_id].add(t.id)
            task_deps[t.id] = deps

        # 2. Initialize Ready Queue
        submitted_tasks = set()
        completed_tasks = set()
        failed_tasks = set()
        retry_counts = {t.id: 0 for t in tasks}
        total_tasks = len(tasks)
        processed_count = 0
        
        # Find tasks with 0 dependencies
        for t in tasks:
            if not task_deps[t.id]:
                self.task_queue.put(t)
                submitted_tasks.add(t.id)
        
        # 3. Event Loop
        while processed_count < total_tasks:
            # Wait for any worker to finish
            result_data = self.result_queue.get()
            
            if result_data is None:
                # Should not happen with updated worker, but handle gracefully
                continue

            tid, success = result_data
            
            if success:
                completed_tasks.add(tid)
                processed_count += 1
                
                # Check dependents
                for dependent_id in task_dependents[tid]:
                    if dependent_id in submitted_tasks or dependent_id in failed_tasks:
                        continue
                    
                    # Remove the satisfied dependency
                    task_deps[dependent_id].discard(tid)
                    
                    # If no more dependencies, submit
                    if not task_deps[dependent_id]:
                        # Find the task object
                        task_obj = next(t for t in tasks if t.id == dependent_id)
                        log(f"DEBUG: Dependencies satisfied for {dependent_id}. Submitting.")
                        self.task_queue.put(task_obj)
                        submitted_tasks.add(dependent_id)
            else:
                # Task failed
                if enable_retry and retry_counts[tid] < 1:
                    log(f"Task {tid} failed. Retrying...")
                    retry_counts[tid] += 1
                    task_obj = next(t for t in tasks if t.id == tid)
                    self.task_queue.put(task_obj)
                else:
                    log(f"Task {tid} failed permanently.")
                    failed_tasks.add(tid)
                    processed_count += 1
                    
                    # Cascade failure to dependents
                    queue = [tid]
                    while queue:
                        curr = queue.pop(0)
                        for dep in task_dependents[curr]:
                            if dep not in failed_tasks and dep not in completed_tasks:
                                log(f"Task {dep} failed due to dependency {curr} failure.")
                                failed_tasks.add(dep)
                                processed_count += 1
                                queue.append(dep)

    def stop(self):
        # Send sentinel values to stop workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        
        for worker in self.workers:
            worker.join()
