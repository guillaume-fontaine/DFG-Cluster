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

## Technologies et Protocoles

### Langages Utilisés
*   **Python 3** : Langage principal du projet. Utilisé pour l'orchestration, la gestion des processus et l'implémentation des scripts de calcul.

### Protocoles et Communication
*   **Multiprocessing (IPC)** : La communication entre le Maître et les Travailleurs se fait via des mécanismes de communication inter-processus (IPC) natifs de Python :
    *   `multiprocessing.Queue` : Pour l'échange de messages (tâches et résultats).
    *   `multiprocessing.Lock` : Pour la synchronisation de l'accès aux ressources partagées (fichiers).
*   **Système de Fichiers** : Utilisé comme moyen de persistance et d'échange de données volumineuses (les fichiers eux-mêmes ne passent pas par les files d'attente, seuls leurs identifiants y transitent).

### Formats de Données
*   **JSON (JavaScript Object Notation)** :
    *   **Transactions** : Le fichier d'entrée décrivant le DAG est au format JSON.
    *   **Registre** : Le mappage RID -> Hash est stocké en JSON.
    *   **Ledger** : Les rapports d'exécution sont stockés individuellement en JSON.
*   **Fichiers Bruts** : Les données traitées par les scripts (contenu des fichiers) sont stockées sous forme de fichiers bruts, identifiés par leur hash SHA-1 (Content Addressable Storage).

## Guide de Démarrage

### Prérequis
*   Python 3.8 ou supérieur.
*   Un système d'exploitation compatible POSIX (Linux/macOS) est recommandé pour la gestion des processus, bien que cela fonctionne sous Windows.

### Lancement du Projet

Le point d'entrée principal est le script `main.py`. Il attend en argument le chemin vers un fichier JSON contenant la liste des transactions à exécuter.

**Commande :**
```bash
python3 main.py <chemin_vers_transactions.json>
```

**Exemple :**
```bash
python3 main.py transactions.json
```

**Déroulement :**
1.  Le script charge et valide le fichier JSON.
2.  Il initialise le cluster avec 3 processus travailleurs (Workers).
3.  Il lance l'exécution du graphe de dépendances (DAG).
4.  Une fois terminé, il vérifie la présence des fichiers de sortie et affiche un rapport.

## Glossaire

*   **DAG (Directed Acyclic Graph)** : Graphe Acyclique Dirigé. Structure de données utilisée pour représenter les dépendances entre les tâches. Chaque tâche est un nœud, et une dépendance est une arête dirigée.
*   **RID (Resource ID)** : Identifiant unique d'une ressource (fichier) dans le système. Il permet de référencer un fichier sans connaître son contenu exact à l'avance.
*   **CAS (Content Addressable Storage)** : Méthode de stockage où les données sont récupérées en utilisant leur contenu (hash) plutôt que leur emplacement.
*   **Worker** : Processus esclave chargé d'exécuter une unité de travail (tâche) de manière isolée.
*   **Master** : Processus maître chargé de la coordination et de la distribution du travail.
*   **Ledger** : Registre immuable (dans le contexte de ce projet, un dossier de logs) enregistrant l'historique et le résultat de chaque transaction exécutée.
*   **IPC (Inter-Process Communication)** : Mécanismes permettant à des processus distincts de communiquer et de se synchroniser (ici, Queues et Locks).
