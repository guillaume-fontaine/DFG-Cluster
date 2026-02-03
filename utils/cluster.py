import multiprocessing
import time
import os
from typing import List, Dict, Set
from models.operation_node import OperationNode
from utils.worker import WorkerExecutor
from utils.storage_manager import StorageManager

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
            result = WorkerExecutor.execute(task, lock=self.registry_lock)
            
            if result:
                with self.ledger_lock:
                    StorageManager.save_ledger_entry(task.id, result)
                
                # Notify master of completion
                self.result_queue.put(task.id)
            else:
                # Even if failed, we should probably notify so master doesn't wait forever?
                # For now, let's assume failure means we can't proceed with dependents.
                # But we should probably signal failure.
                self.result_queue.put(None)

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

    def process_tasks(self, tasks: List[OperationNode]):
        """
        Manages the execution of tasks respecting dependencies.
        """
        pending_tasks = {t.id: t for t in tasks}
        completed_tasks = set()
        
        # Map: RID -> Transaction ID that produces it
        # This helps to know which transaction must finish before another can start.
        # However, the dependency is on the *source files* (RIDs).
        # We need to know if the source RIDs exist.
        # But since we are generating them, we know which transaction produces which RID.
        
        # Build a dependency graph: Task ID -> Set of Task IDs it depends on
        task_dependencies: Dict[str, Set[str]] = {}
        
        # Map: RID -> Task ID that produces it
        rid_producer: Dict[str, str] = {}
        for t in tasks:
            for dest in t.destination:
                rid_producer[dest] = t.id
                
        for t in tasks:
            deps = set()
            for src in t.sources:
                if src in rid_producer:
                    deps.add(rid_producer[src])
            task_dependencies[t.id] = deps

        # Queue of tasks ready to run
        ready_queue = []
        
        # Find initially ready tasks
        for t_id, deps in task_dependencies.items():
            if not deps:
                ready_queue.append(pending_tasks[t_id])
        
        # Submit initial tasks
        for t in ready_queue:
            self.task_queue.put(t)
            
        # Loop until all tasks are done
        tasks_remaining = len(tasks)
        while tasks_remaining > 0:
            # Wait for a completion signal
            completed_tid = self.result_queue.get()
            
            if completed_tid:
                completed_tasks.add(completed_tid)
                tasks_remaining -= 1
                
                # Check if new tasks are ready
                # We iterate over pending tasks that are not yet completed and not yet submitted?
                # Actually, we need to track which ones are submitted.
                # Let's refine the state tracking.
                
                # Instead of iterating all, let's just check dependencies.
                # Optimization: Reverse dependency map (Task -> Dependents) would be better,
                # but for N=50 it's fine to iterate.
                
                for t_id, deps in task_dependencies.items():
                    if t_id in completed_tasks:
                        continue
                    
                    # If this task was already submitted, skip
                    # We need a set of submitted tasks
                    # Let's use a separate set for submitted
                    pass 

        # Re-implementing the loop with better state tracking
        pass

    def run_dag(self, tasks: List[OperationNode]):
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
        total_tasks = len(tasks)
        
        # Find tasks with 0 dependencies
        for t in tasks:
            if not task_deps[t.id]:
                self.task_queue.put(t)
                submitted_tasks.add(t.id)
        
        # 3. Event Loop
        while len(completed_tasks) < total_tasks:
            # Wait for any worker to finish
            finished_tid = self.result_queue.get()
            
            if finished_tid is None:
                # A task failed. What to do? 
                # For now, we just ignore it or log it. 
                # But we still need to decrement total_tasks or handle it to avoid infinite loop.
                # Let's assume we just count it as "processed" but don't unlock dependents.
                # Or better, just break/raise error.
                print("A task failed execution.")
                # If we break, we leave the process hanging.
                # Let's just count it as completed for the loop, but dependents won't run.
                # Actually, if we don't add it to completed_tasks, we loop forever.
                # If we add it, dependents might fail due to missing files.
                # Let's just continue.
                total_tasks -= 1 # Reduce expectation
                continue

            completed_tasks.add(finished_tid)
            
            # Check dependents
            for dependent_id in task_dependents[finished_tid]:
                if dependent_id in submitted_tasks:
                    continue
                
                # Remove the satisfied dependency
                task_deps[dependent_id].discard(finished_tid)
                
                # If no more dependencies, submit
                if not task_deps[dependent_id]:
                    # Find the task object
                    task_obj = next(t for t in tasks if t.id == dependent_id)
                    print(f"DEBUG: Dependencies satisfied for {dependent_id}. Submitting.")
                    self.task_queue.put(task_obj)
                    submitted_tasks.add(dependent_id)

    def stop(self):
        # Send sentinel values to stop workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        
        for worker in self.workers:
            worker.join()
