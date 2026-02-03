import multiprocessing
import time
import os
from typing import List
from models.operation_node import OperationNode
from utils.worker import WorkerExecutor
from utils.storage_manager import StorageManager

class ClusterWorker(multiprocessing.Process):
    def __init__(self, task_queue, registry_lock, ledger_lock):
        super().__init__()
        self.task_queue = task_queue
        self.registry_lock = registry_lock
        self.ledger_lock = ledger_lock

    def run(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                # Sentinel value to stop the worker
                break
            
            # Execute task
            # We pass registry_lock to execute because it calls StorageManager.save_file
            result = WorkerExecutor.execute(task, lock=self.registry_lock)
            
            if result:
                # Archive to ledger
                # We might want a lock for ledger too if we were writing to a shared file, 
                # but we write to individual files. However, creating the file might need directory lock?
                # The prompt says "Le Ledger : La création des fichiers de transactions individuels ne doit pas entrer en collision."
                # Since filenames are unique RIDs, collision is unlikely unless RIDs collide.
                # But let's use a lock if requested or just to be safe for directory creation race conditions.
                
                # Actually, StorageManager.save_ledger_entry creates directory if needed.
                # Let's pass a lock just in case or wrap it.
                # But wait, save_ledger_entry writes to a unique file.
                # Let's just use the lock to be safe as per "verrous explicites autour de chaque opération d’écriture sur les ressources partagées".
                
                with self.ledger_lock:
                    StorageManager.save_ledger_entry(task.id, result)
            
            # print(f"Worker {self.name} finished task {task.id}")

class ClusterMaster:
    def __init__(self, num_workers=3):
        self.num_workers = num_workers
        self.task_queue = multiprocessing.Queue()
        self.registry_lock = multiprocessing.Lock()
        self.ledger_lock = multiprocessing.Lock()
        self.workers = []

    def start(self):
        for i in range(self.num_workers):
            worker = ClusterWorker(self.task_queue, self.registry_lock, self.ledger_lock)
            worker.start()
            self.workers.append(worker)

    def submit_task(self, node: OperationNode):
        self.task_queue.put(node)

    def stop(self):
        # Send sentinel values to stop workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        
        for worker in self.workers:
            worker.join()
