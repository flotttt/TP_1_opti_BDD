# TP3 - Monitoring et Optimisation PostgreSQL

Système de monitoring PostgreSQL avec collecte de données de vols, optimisations et alertes.

## Setup rapide

### 1. Configuration

```bash
cd tp3
cp .env.exemple .env
```

Modifier le fichier `.env` si nécessaire avec vos propres valeurs.

### 2. Démarrer l'infrastructure

```bash
docker-compose up -d
```

### 3. Lancer le pipeline

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
python -m venv venv

venv\Scripts\activate

pip install -r tp3/requirements.txt
```

```bash
python3 main.py
```

### 4. Configurer Grafana

**URL:** http://localhost:3000 (admin / admin)

**Ajouter les datasources :**
- PostgreSQL : `pg_perf:5432` / perfdb / postgres / postgres
- Prometheus : `http://prometheus:9090`

**Importer les dashboards :**
- `postgres_monitoring_dashboard.json` (datasource: Prometheus)
- `flight_dashboard.json` (datasource: PostgreSQL)

### 5. Site web (optionnel)

```bash
cd ../flight-map
npm install --legacy-peer-deps
npm run dev
```

URL: http://localhost:3001

## Accès

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Alertes | http://localhost:9090/alerts | - |

## Arrêt

```bash
# Arrêter main.py
CTRL+C

# Arrêter Docker
docker-compose stop
```
