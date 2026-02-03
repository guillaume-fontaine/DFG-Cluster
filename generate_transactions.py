#!/usr/bin/env python3
import json
import random
import argparse
from models.operation_node import FunctionType
from utils.rid_manager import RIDManager
from utils.storage_manager import StorageManager


def generate_transactions(num_transactions_per_chain: int, num_parallel_chains: int, output_file: str):
    if num_transactions_per_chain < 1 or num_parallel_chains < 1:
        print("Le nombre de transactions et de chaînes doit être au moins de 1.")
        return

    all_transactions = []

    print(f"Génération de {num_parallel_chains} chaînes parallèles de {num_transactions_per_chain} additions...")

    for chain_id in range(num_parallel_chains):
        # Initialisation d'une nouvelle chaîne indépendante
        # 1. Création des deux sources primaires pour cette chaîne
        current_sources = [RIDManager.generate(), RIDManager.generate()]

        for rid in current_sources:
            val = random.randint(1, 100)
            StorageManager.save_file(rid, str(val))

        # 2. Génération des transactions pour cette chaîne
        for i in range(num_transactions_per_chain):
            func_type = FunctionType.ADD
            current_dests = [RIDManager.generate(), RIDManager.generate()]

            tx = {
                "id": RIDManager.generate(),
                "chain_id": chain_id + 1,  # Optionnel : pour faciliter le débogage
                "function": func_type.value,
                "sources": current_sources,
                "destination": current_dests
            }
            all_transactions.append(tx)

            # Les sorties deviennent les entrées du prochain maillon de la chaîne
            current_sources = current_dests

    # 3. Sauvegarde finale
    with open(output_file, 'w') as f:
        json.dump(all_transactions, f, indent=4)

    total_tx = len(all_transactions)
    print(
        f"Succès : {total_tx} transactions générées ({num_parallel_chains} chaînes de {num_transactions_per_chain}) dans '{output_file}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère plusieurs chaînes parallèles d'additions séquentielles.")
    parser.add_argument("count", type=int, help="Nombre de transactions PAR chaîne")
    parser.add_argument("-p", "--parallel", type=int, default=1, help="Nombre de chaînes de calcul parallèles")
    parser.add_argument("--output", type=str, default="transactions.json", help="Fichier JSON de sortie")

    args = parser.parse_args()
    generate_transactions(args.count, args.parallel, args.output)