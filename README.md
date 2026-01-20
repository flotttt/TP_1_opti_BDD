# TP 2
# Projet OpenFoodFacts - Comparaison PostgreSQL vs MongoDB

Ce projet permet de comparer les performances entre PostgreSQL et MongoDB en utilisant les données d'OpenFoodFacts.

## Prérequis

- Docker et Docker Compose
- Python 3.8+
- pip

## Installation

### 1. Démarrer les bases de données avec Docker

```bash
docker-compose up -d
```

Cela va démarrer :
- PostgreSQL (port 5432)
- MongoDB (port 27017)
- Mongo Express - interface web pour MongoDB (port 8081)

### 2. Configurer l'environnement Python

#### Linux / macOS

```bash
# Créer l'environnement virtuel
python3 -m venv venv

# Activer l'environnement virtuel
source venv/bin/activate

# Installer les dépendances
pip install -r tp2/requirements.txt
```

#### Windows

```bash
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement virtuel
venv\Scripts\activate

# Installer les dépendances
pip install -r tp2/requirements.txt
```

## Test de connexion MongoDB

Pour vérifier que MongoDB fonctionne correctement :

```bash
python3 tp2/test_mongo.py
```

## Utilisation - Pipeline d'exécution

Suivez ces étapes dans l'ordre :

### 1. Récupération des catégories

Récupère les catégories de produits depuis l'API OpenFoodFacts.

```bash
python3 tp2/recup_catego.py
```

### 2. Récupération des produits

Télécharge les produits pour chaque catégorie.

```bash
python3 tp2/recup_item.py
```

### 3. Récupération des produits échoués (optionnel)

Si certains produits n'ont pas été téléchargés, ce script réessaie.

```bash
python3 tp2/recup_fail.py
```

### 4. Nettoyage et import dans MongoDB

Nettoie les données et les importe dans MongoDB.

```bash
python3 tp2/clean_data.py
```

### 5. Import dans PostgreSQL

Importe les données nettoyées dans PostgreSQL.

```bash
python3 tp2/import_to_postgres.py
```

### 6. Benchmark PostgreSQL (non optimisé)

Effectue des tests de performance sur PostgreSQL sans optimisations.

```bash
python3 tp2/benchmark_postgres.py
```

### 7. Optimisation PostgreSQL

Applique des optimisations (index, contraintes, etc.) sur PostgreSQL.

```bash
python3 tp2/optimize_postgres.py
```

### 8. Benchmark MongoDB avec optimisations

Effectue des tests de performance sur MongoDB en 3 phases :
1. Benchmark AVANT optimisation
2. Création des index MongoDB
3. Benchmark APRÈS optimisation
4. Comparaison des résultats avant/après

```bash
python3 tp2/benchmark_mongodb.py
```



## Configuration

**Important** : Créez un fichier `.env` à la racine du projet en vous basant sur le fichier `.env.example` fourni :

```bash
cp .env.example .env
```

Le fichier `.env.example` contient toutes les variables nécessaires. Modifiez les valeurs dans votre `.env` si nécessaire (mots de passe, ports, etc.)

## Désactivation de l'environnement virtuel

```bash
deactivate
```

## Arrêt des services Docker

```bash
docker-compose down
```

## Structure du projet

```
.
├── docker-compose.yml          # Configuration Docker
├── .env                        # Variables d'environnement
└── tp2/
    ├── requirements.txt            # Dépendances Python
    ├── recup_catego.py        # 1. Récupération des catégories
    ├── recup_item.py          # 2. Récupération des produits
    ├── recup_fail.py          # 3. Récupération des échecs
    ├── clean_data.py          # 4. Nettoyage + import MongoDB
    ├── import_to_postgres.py  # 5. Import PostgreSQL
    ├── benchmark_postgres.py  # 6. Benchmark PostgreSQL non optimisé
    ├── optimize_postgres.py   # 7. Optimisation PostgreSQL
    ├── benchmark_mongodb.py   # 8. Benchmark MongoDB optimisé
    └── test_mongo.py          # Test connexion MongoDB
```

## Notes

- Assurez-vous que Docker est bien démarré avant de lancer le pipeline
- L'environnement virtuel Python doit être activé pour chaque session
- Les scripts doivent être exécutés dans l'ordre indiqué pour fonctionner correctement
