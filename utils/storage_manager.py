import hashlib
import json
import os
from typing import Dict, Optional
from config import STORE_DIR, REGISTRY_FILE, LEDGER_DIR

class StorageManager:
    _registry: Dict[str, str] = {}

    @classmethod
    def _load_registry(cls):
        if os.path.exists(REGISTRY_FILE):
            try:
                with open(REGISTRY_FILE, 'r') as f:
                    cls._registry = json.load(f)
            except json.JSONDecodeError:
                cls._registry = {}
        else:
            cls._registry = {}

    @classmethod
    def _save_registry(cls):
        os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
        with open(REGISTRY_FILE, 'w') as f:
            json.dump(cls._registry, f)

    @classmethod
    def _calculate_hash(cls, content: str) -> str:
        return hashlib.sha1(content.encode('utf-8')).hexdigest()

    @classmethod
    def save_file(cls, rid: str, content: str, lock=None) -> str:
        """
        Saves content to the store using its hash as the filename.
        Updates the registry mapping RID -> Hash.
        Returns the hash of the content.
        Thread/Process safe if lock is provided.
        """
        file_hash = cls._calculate_hash(content)
        
        # Save the physical file if it doesn't exist
        os.makedirs(STORE_DIR, exist_ok=True)
        file_path = os.path.join(STORE_DIR, file_hash)
        
        # Writing the file itself is usually atomic or safe enough if we don't interleave writes to same file
        # Since hash is content-based, multiple writers writing same content to same file is fine.
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                f.write(content)
        
        # Update registry - Critical Section
        if lock:
            with lock:
                cls._update_registry(rid, file_hash)
        else:
            cls._update_registry(rid, file_hash)
        
        return file_hash

    @classmethod
    def _update_registry(cls, rid: str, file_hash: str):
        cls._load_registry()
        cls._registry[rid] = file_hash
        cls._save_registry()

    @classmethod
    def get_file_content(cls, rid: str) -> str:
        """
        Retrieves content associated with a RID.
        """
        # Reading registry might need lock if it's being written to?
        # For simplicity, we reload registry every time or assume it's fine.
        # But to be safe, we should probably lock read too if we want strict consistency.
        # However, usually we just need to find the hash.
        
        # Let's just reload to be sure we have latest
        cls._load_registry()
            
        file_hash = cls._registry.get(rid)
        if not file_hash:
            raise FileNotFoundError(f"No file found for RID: {rid}")
            
        file_path = os.path.join(STORE_DIR, file_hash)
        if not os.path.exists(file_path):
             raise FileNotFoundError(f"Physical file missing for hash: {file_hash}")
             
        with open(file_path, 'r') as f:
            return f.read()

    @classmethod
    def get_hash(cls, rid: str) -> str:
        cls._load_registry()
        return cls._registry.get(rid)

    @classmethod
    def save_ledger_entry(cls, transaction_rid: str, entry_data: dict, lock=None):
        """
        Saves a transaction record to the ledger.
        """
        os.makedirs(LEDGER_DIR, exist_ok=True)
        file_path = os.path.join(LEDGER_DIR, f"{transaction_rid}.json")
        
        # Writing a new file for a unique transaction RID shouldn't conflict with others
        # But if we want to be safe or if we append to a global log, we need a lock.
        # Here we write individual files.
        
        with open(file_path, 'w') as f:
            json.dump(entry_data, f, indent=4)
