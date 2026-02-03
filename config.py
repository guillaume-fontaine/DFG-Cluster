N = 10

# Base directory for the entire simulated cluster environment
CLUSTER_ROOT = "cluster_env"

# Define machine IDs and types
ORCHESTRATOR_NODE = "M00"
USER_NODES = ["U01", "U02", "U03"]
COMPUTE_NODES = ["M01", "M02", "M03"]
CENTRAL_SERVICES_NODES = ["C01"] # Example for ID Registrar/Vault

ALL_MACHINE_IDS = [ORCHESTRATOR_NODE] + USER_NODES + COMPUTE_NODES + CENTRAL_SERVICES_NODES

# These will now be relative paths within each machine's CLUSTER_ROOT/<MACHINE_ID> directory
# For example, M00's store will be CLUSTER_ROOT/M00/store
MACHINE_STORE_DIR_NAME = "store"
MACHINE_REGISTRY_FILE_NAME = "registry.json"
MACHINE_VAULT_FILE_NAME = "vault.json"
MACHINE_TMP_DIR_NAME = "tmp" # Each machine will have its own /tmp within its root

# Global ledger for the orchestrator
LEDGER_DIR = "ledger"

# Global vault for ID generation (used by generator script)
GLOBAL_VAULT_FILE = "data/vault.json"
