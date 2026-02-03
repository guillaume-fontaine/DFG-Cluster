# Documentation Technique du Projet DFG-Cluster

## Introduction

Ce document présente la documentation technique du projet **DFG-Cluster**. Ce projet simule un cluster de calcul distribué capable d'exécuter des graphes de tâches (Data Flow Graph). Il met en œuvre des nœuds de calcul, un orchestrateur, des nœuds utilisateurs et un système de fichiers distribué simulé.

L'objectif est de fournir une compréhension approfondie de l'architecture, des composants et du fonctionnement du système.

## Architecture Globale

Le système est conçu autour d'une architecture distribuée simulée utilisant le module `multiprocessing` de Python. Les principaux composants communiquent via un réseau simulé.

### Composants Principaux

1.  **OrchestratorNode (M00)** : Le cerveau du cluster. Il reçoit les jobs des utilisateurs, gère les dépendances entre les tâches et planifie leur exécution sur les nœuds de calcul disponibles.
2.  **ComputeNode (M01, M02, ...)** : Les ouvriers. Ils reçoivent des tâches de l'orchestrateur, récupèrent les fichiers nécessaires (sources), exécutent les scripts correspondants et stockent les résultats.
3.  **UserNode (U01, ...)** : Les clients. Ils soumettent des jobs (listes de transactions/opérations) à l'orchestrateur et fournissent les données initiales.
4.  **Network** : Une couche d'abstraction qui simule le réseau, gérant l'envoi de messages entre les nœuds avec une latence simulée.
5.  **StorageManager** : Gère le stockage local de chaque nœud, utilisant un système adressable par contenu (CAS) où les fichiers sont stockés par leur hash SHA-1.

### Flux de Données

1.  L'utilisateur soumet un job (graphe de tâches) à l'orchestrateur.
2.  L'orchestrateur analyse les dépendances et identifie les tâches prêtes à être exécutées.
3.  L'orchestrateur assigne une tâche à un nœud de calcul.
4.  Le nœud de calcul demande les fichiers sources nécessaires aux nœuds qui les possèdent (UserNode ou autres ComputeNodes).
5.  Le nœud de calcul exécute la tâche et produit de nouveaux fichiers.
6.  Le nœud de calcul notifie l'orchestrateur de la complétion de la tâche.
7.  L'orchestrateur met à jour l'état du graphe et planifie les tâches suivantes.

## Structures de Données

### OperationNode

La classe `OperationNode` (dans `models/operation_node.py`) représente une tâche unitaire dans le graphe de dépendances.

*   **id** (`str`): Identifiant unique de l'opération (transaction).
*   **function** (`FunctionType`): Type de fonction à exécuter (ADD, MULT, HASH).
*   **sources** (`List[str]`): Liste des RIDs (Resource IDs) des fichiers d'entrée.
*   **destination** (`List[str]`): Liste des RIDs des fichiers de sortie produits.

### Message

La classe `Message` (dans `utils/network.py`) est utilisée pour la communication inter-nœuds.

*   **sender** (`str`): ID du nœud émetteur.
*   **receiver** (`str`): ID du nœud destinataire.
*   **type** (`str`): Type de message (ex: "SUBMIT_JOB", "EXECUTE_TASK", "FILE_TRANSFER").
*   **payload** (`Any`): Données associées au message.

## Gestion du Stockage (StorageManager)

Chaque nœud possède son propre gestionnaire de stockage (`utils/storage_manager.py`).

*   **Stockage CAS** : Les fichiers sont stockés physiquement avec leur hash SHA-1 comme nom de fichier.
*   **Registre (Registry)** : Un fichier JSON (`registry.json`) maintient la correspondance entre les RIDs (identifiants logiques) et les hashs (identifiants physiques).
*   **Ledger** : Un journal global des événements (transferts, exécutions) est maintenu dans le dossier `ledger/` pour le suivi et le reporting.

## Protocoles de Communication

Les nœuds communiquent via des messages asynchrones. Voici les principaux types de messages :

*   **SUBMIT_JOB** (User -> Orchestrator) : Contient la liste des tâches à exécuter.
*   **EXECUTE_TASK** (Orchestrator -> Compute) : Demande l'exécution d'une tâche spécifique. Le payload inclut la définition de la tâche et la localisation des fichiers sources.
*   **REQUEST_FILE** (Node -> Node) : Demande le contenu d'un fichier identifié par son RID.
*   **FILE_TRANSFER** (Node -> Node) : Transfère le contenu d'un fichier demandé.
*   **TASK_COMPLETED** (Compute -> Orchestrator) : Signale la fin d'une tâche et liste les fichiers produits.
*   **TASK_FAILED** (Compute -> Orchestrator) : Signale l'échec d'une tâche.
*   **STOP** (Main -> Node) : Demande l'arrêt du nœud.

## Cycle de Vie d'une Tâche

1.  **Soumission** : L'Orchestrateur reçoit un job et construit le graphe de dépendances.
2.  **Planification** : Dès que toutes les dépendances d'une tâche sont satisfaites, elle est marquée comme "prête". L'Orchestrateur sélectionne un ComputeNode (actuellement aléatoire) et lui envoie `EXECUTE_TASK`.
3.  **Récupération des Données** : Le ComputeNode vérifie s'il possède les fichiers sources localement. Sinon, il envoie des `REQUEST_FILE` aux nœuds possédant les données (indiqués par l'Orchestrateur).
4.  **Exécution** :
    *   Un répertoire temporaire isolé est créé.
    *   Les fichiers sources y sont copiés.
    *   Le script Python correspondant à la fonction (ex: `scripts/add.py`) est exécuté via `subprocess`.
    *   Les fichiers de sortie sont récupérés et sauvegardés dans le stockage local du ComputeNode.
5.  **Finalisation** : Le ComputeNode envoie `TASK_COMPLETED` à l'Orchestrateur.
6.  **Mise à jour** : L'Orchestrateur enregistre la localisation des nouveaux fichiers et débloque les tâches dépendantes.

## Scripts de Calcul

Les opérations supportées sont définies dans le dossier `scripts/`. Chaque script prend en argument les chemins des fichiers sources, un séparateur `-`, et les chemins des fichiers de destination.

*   `add.py` : Additionne les entiers contenus dans les fichiers sources.
*   `mult.py` : Multiplie les entiers.
*   `hash.py` : Calcule un hash (non implémenté en détail ici, mais prévu).

## Configuration et Exécution

Le fichier `config.py` définit la topologie du cluster (IDs des machines, répertoires).
Le point d'entrée est `main.py`, qui :
1.  Initialise l'environnement (dossiers).
2.  Charge les transactions depuis un fichier JSON.
3.  Lance les processus (Orchestrator, Workers, User).
4.  Peuple le stockage de l'utilisateur avec les données initiales.
5.  Soumet le job et surveille l'avancement via le dossier `ledger/`.

## Outils Annexes

### Générateur de Transactions

Le script `generate_transactions.py` permet de créer des jeux de données de test.
Il génère un fichier JSON contenant une liste de transactions aléatoires, en s'assurant que certaines transactions utilisent les résultats d'autres (création de dépendances). Il pré-peuple également le stockage du nœud utilisateur avec les fichiers sources initiaux nécessaires.

Usage :
```bash
python generate_transactions.py <nombre_de_transactions>
```

### Gestion des Identifiants (RIDManager)

Le module `utils/rid_manager.py` est responsable de la génération d'identifiants uniques (RID) pour les fichiers et les transactions. Il maintient un registre global (`data/vault.json`) pour garantir l'unicité des IDs générés.

### Rapport Ledger

Le script `generate_ledger_report.py` analyse les fichiers JSON présents dans le dossier `ledger/` (générés lors de l'exécution de la simulation) et produit un rapport HTML (`ledger_report.html`). Ce rapport agrège et affiche le contenu de chaque fichier de log, permettant de visualiser les événements survenus (transferts de fichiers, exécutions de tâches).

### Script de Lancement Rapide

Le script `run.sh` automatise le processus de test :
1.  Génération de 10 transactions.
2.  Lancement de la simulation principale.

### ClusterMaster (Obsolète/Legacy)

Le fichier `utils/cluster.py` contient une implémentation alternative de la gestion du cluster (`ClusterMaster` et `ClusterWorker`) qui utilise une file d'attente partagée et des verrous, plutôt que le modèle de passage de messages (Actor Model) utilisé dans l'implémentation principale (`OrchestratorNode`). Cette implémentation n'est pas utilisée par `main.py`.

