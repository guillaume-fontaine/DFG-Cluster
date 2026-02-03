import time
import multiprocessing
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class Message:
    sender: str
    receiver: str
    type: str
    payload: Any

class Network:
    def __init__(self):
        self._queues: Dict[str, multiprocessing.Queue] = {}
        self._manager = multiprocessing.Manager()
        self._queues = self._manager.dict()
        self._lock = multiprocessing.Lock()

    def register_node(self, node_id: str, queue: multiprocessing.Queue):
        with self._lock:
            self._queues[node_id] = queue

    def send(self, message: Message):
        # Simulate latency based on payload size
        # We can't easily measure object size, but let's approximate if it's bytes/str
        size = 0
        if isinstance(message.payload, (bytes, str)):
            size = len(message.payload)
        elif isinstance(message.payload, dict) and "content" in message.payload:
             # If payload has content field (file transfer)
             content = message.payload["content"]
             if isinstance(content, (bytes, str)):
                 size = len(content)
        
        # Latency: 0.001s per KB?
        latency = (size / 1024) * 0.001
        if latency > 0:
            time.sleep(latency)
            
        if message.receiver in self._queues:
            self._queues[message.receiver].put(message)
        else:
            print(f"Network Error: Node {message.receiver} not found.")
