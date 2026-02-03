import random
import string
import json
import os
from typing import Set
from config import N, VAULT_FILE

class RIDManager:
    _vault: Set[str] = set()

    @classmethod
    def _load_vault(cls):
        if os.path.exists(VAULT_FILE):
            try:
                with open(VAULT_FILE, 'r') as f:
                    cls._vault = set(json.load(f))
            except json.JSONDecodeError:
                cls._vault = set()
        else:
            cls._vault = set()

    @classmethod
    def _save_vault(cls):
        os.makedirs(os.path.dirname(VAULT_FILE), exist_ok=True)
        with open(VAULT_FILE, 'w') as f:
            json.dump(list(cls._vault), f)

    @classmethod
    def generate(cls) -> str:
        if not cls._vault:
            cls._load_vault()
        
        while True:
            rid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
            if rid not in cls._vault:
                cls._vault.add(rid)
                cls._save_vault()
                return rid

    @classmethod
    def get_vault(cls) -> Set[str]:
        if not cls._vault:
            cls._load_vault()
        return cls._vault
