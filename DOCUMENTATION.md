# Documentation Technique du Projet DFG-Cluster (Branche Multiprocessing)

## Introduction

Ce document présente la documentation technique du projet **DFG-Cluster** sur la branche `feature/mutliprocessing`. Cette version du projet implémente un système d'exécution de graphes de tâches (DAG) en utilisant le module `multiprocessing` de Python pour paralléliser les calculs sur une seule machine, simulant un environnement de cluster.

Contrairement à la branche principale qui simule un réseau complet avec des nœuds distincts (Orchestrator, User, Compute), cette branche se concentre sur l'ordonnancement et l'exécution parallèle des tâches via un modèle Maître-Esclave (Master-Worker) utilisant des files d'attente partagées.

## Architecture Globale

L'architecture repose sur le modèle **Master-Worker** :

1.  **ClusterMaster** : Le processus principal qui gère le graphe de dépendances (DAG), soumet les tâches prêtes aux travailleurs et surveille leur complétion.
2.  **ClusterWorker** : Des processus ouvriers (Workers) qui consomment les tâches depuis une file d'attente, les exécutent de manière isolée et rapportent les résultats.
3.  **StorageManager** : Un gestionnaire de stockage centralisé (pour cette simulation) qui gère la persistance des fichiers (CAS) et le registre des métadonnées.

### Flux de Données

1.  Le script `main.py` charge les transactions (tâches) depuis un fichier JSON.
2.  Il instancie un `ClusterMaster` et lui passe la liste des transactions.
3.  Le `ClusterMaster` construit le graphe de dépendances.
4.  Les tâches sans dépendances (ou dont les dépendances sont satisfaites) sont placées dans la `task_queue`.
5.  Les `ClusterWorker` récupèrent les tâches, les exécutent via `WorkerExecutor`, et placent l'ID de la tâche terminée dans la `result_queue`.
6.  Le `ClusterMaster` récupère les résultats, met à jour le graphe de dépendances et soumet les nouvelles tâches devenues prêtes.

## Composants Détaillés

### ClusterMaster (`utils/cluster.py`)

Responsable de l'orchestration.

*   **Gestion du DAG** : Analyse les dépendances entre les fichiers sources et les fichiers de destination des transactions.
*   **Ordonnancement** : Utilise une boucle d'événements pour surveiller la `result_queue` et débloquer les tâches dépendantes.
*   **Communication** : Utilise `multiprocessing.Queue` pour envoyer les tâches (`task_queue`) et recevoir les notifications de succès (`result_queue`).
*   **Synchronisation** : Gère les verrous (`Lock`) pour l'accès concurrent au registre et au ledger.

### ClusterWorker (`utils/cluster.py`)

Processus exécutant les tâches.

*   Boucle infinie qui attend des tâches dans la `task_queue`.
*   Délègue l'exécution technique à `WorkerExecutor`.
*   Gère la journalisation dans le Ledger de manière thread-safe (via un verrou).
*   S'arrête lorsqu'il reçoit une valeur sentinelle (`None`).

### WorkerExecutor (`utils/worker.py`)

Classe utilitaire statique effectuant le travail concret.

1.  **Préparation** : Crée un répertoire temporaire isolé (`/tmp/[TASK_ID]`).
2.  **Récupération** : Copie les fichiers sources depuis le `StorageManager` vers le répertoire temporaire.
3.  **Exécution** : Lance le script Python correspondant à la fonction (ex: `scripts/add.py`) via `subprocess`.
4.  **Sauvegarde** : Lit les fichiers produits et les sauvegarde dans le `StorageManager`.
5.  **Nettoyage** : Supprime le répertoire temporaire.

### StorageManager (`utils/storage_manager.py`)

Gère le stockage des données.

*   **Store** : Dossier contenant les fichiers nommés par leur hash SHA-1.
*   **Registry** : Fichier JSON mappant les RIDs (Resource IDs) aux hashs.
*   **Ledger** : Dossier stockant les rapports d'exécution de chaque transaction.

## Configuration

Le fichier `config.py` définit les paramètres globaux :

*   `STORE_DIR` : Répertoire de stockage des fichiers (CAS).
*   `REGISTRY_FILE` : Chemin du fichier de registre.
*   `LEDGER_DIR` : Répertoire des logs d'exécution.
*   `TMP_DIR` : Répertoire racine pour les fichiers temporaires d'exécution.
*   `VAULT_FILE` : Fichier utilisé par le générateur d'IDs.

## Exécution

Le point d'entrée est `main.py`.

Usage :
```bash
./main.py transactions.json
```

Le script :
1.  Charge les transactions.
2.  Lance le cluster (Master + 3 Workers).
3.  Exécute le DAG.
4.  Vérifie à la fin que tous les fichiers de destination existent.

