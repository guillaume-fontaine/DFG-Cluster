import hashlib
import json
import os
from typing import Dict
from config import STORE_DIR, REGISTRY_FILE

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
    def save_file(cls, rid: str, content: str) -> str:
        """
        Saves content to the store using its hash as the filename.
        Updates the registry mapping RID -> Hash.
        Returns the hash of the content.
        """
        if not cls._registry:
            cls._load_registry()

        file_hash = cls._calculate_hash(content)
        
        # Save the physical file if it doesn't exist
        os.makedirs(STORE_DIR, exist_ok=True)
        file_path = os.path.join(STORE_DIR, file_hash)
        
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                f.write(content)
        
        # Update registry
        cls._registry[rid] = file_hash
        cls._save_registry()
        
        return file_hash

    @classmethod
    def get_file_content(cls, rid: str) -> str:
        """
        Retrieves content associated with a RID.
        """
        if not cls._registry:
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
        if not cls._registry:
            cls._load_registry()
        return cls._registry.get(rid)
