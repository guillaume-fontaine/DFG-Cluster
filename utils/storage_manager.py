import hashlib
import json
import os
from typing import Dict, Optional
from config import MACHINE_STORE_DIR_NAME, MACHINE_REGISTRY_FILE_NAME, LEDGER_DIR

def log(message):
    print(f"[storage_manager.py] {message}")

class StorageManager:
    def __init__(self, machine_root: str):
        self.machine_root = machine_root
        self.store_dir = os.path.join(machine_root, MACHINE_STORE_DIR_NAME)
        self.registry_file = os.path.join(machine_root, MACHINE_REGISTRY_FILE_NAME)
        self._registry: Dict[str, str] = {}
        self._ensure_dirs()

    def _ensure_dirs(self):
        os.makedirs(self.store_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.registry_file), exist_ok=True)

    def _load_registry(self):
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, 'r') as f:
                    self._registry = json.load(f)
            except json.JSONDecodeError:
                self._registry = {}
        else:
            self._registry = {}

    def _save_registry(self):
        with open(self.registry_file, 'w') as f:
            json.dump(self._registry, f)

    def _calculate_hash(self, content: str) -> str:
        return hashlib.sha1(content.encode('utf-8')).hexdigest()

    def save_file(self, rid: str, content: str) -> str:
        """
        Saves content to the machine's store.
        """
        file_hash = self._calculate_hash(content)
        file_path = os.path.join(self.store_dir, file_hash)
        
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                f.write(content)
        
        self._update_registry(rid, file_hash)
        return file_hash

    def _update_registry(self, rid: str, file_hash: str):
        self._load_registry()
        self._registry[rid] = file_hash
        self._save_registry()

    def get_file_content(self, rid: str) -> str:
        self._load_registry()
        file_hash = self._registry.get(rid)
        if not file_hash:
            raise FileNotFoundError(f"No file found for RID: {rid} in {self.machine_root}")
            
        file_path = os.path.join(self.store_dir, file_hash)
        if not os.path.exists(file_path):
             raise FileNotFoundError(f"Physical file missing for hash: {file_hash} in {self.store_dir}")
             
        with open(file_path, 'r') as f:
            return f.read()

    def has_file(self, rid: str) -> bool:
        self._load_registry()
        return rid in self._registry

    def get_hash(self, rid: str) -> str:
        self._load_registry()
        return self._registry.get(rid)

    @staticmethod
    def save_ledger_entry(transaction_id: str, entry_data: dict):
        """
        Appends a record to the transaction's ledger file.
        """
        os.makedirs(LEDGER_DIR, exist_ok=True)
        file_path = os.path.join(LEDGER_DIR, f"{transaction_id}.json")
        
        existing_data = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        existing_data = content
                    else:
                        existing_data = [content]
            except:
                pass
        
        existing_data.append(entry_data)
        
        with open(file_path, 'w') as f:
            json.dump(existing_data, f, indent=4)
