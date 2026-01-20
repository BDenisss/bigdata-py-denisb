# Big Data - Pipeline ELT & Analytics Dashboard

Projet Big Data M2 - Pipeline ELT avec orchestration Prefect, stockage MinIO, base NoSQL MongoDB, API FastAPI et Dashboard Streamlit.

## 📋 Architecture du projet

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│    MinIO    │────▶│   MongoDB   │
│  (CSV)      │     │ Bronze/Silver/Gold │           │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │   FastAPI   │
                                        │    (API)    │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Streamlit  │
                                        │ (Dashboard) │
                                        └─────────────┘
```

## 🛠️ Prérequis

- Python 3.11+
- Docker & Docker Compose
- Git

## 🚀 Installation et lancement

### 1. Cloner le projet

```bash
git clone https://github.com/BDenisss/bigdata-py-denisb.git
cd bigdata-py-denisb
```

### 2. Créer l'environnement virtuel

```bash
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

# Linux/Mac
source .venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Lancer l'infrastructure Docker

```bash
docker-compose up -d
```

Cela démarre :
- **MinIO** (stockage objet) - Port 9000/9001
- **PostgreSQL** (base Prefect) - Port 5432
- **Prefect Server** (orchestration) - Port 4200
- **MongoDB** (base NoSQL) - Port 27017
- **Mongo Express** (UI MongoDB) - Port 8081

### 5. Générer les données de test

```bash
python script/generate_data.py
```
(elles sont, normalement, déjà générées sur le projet)

### 6. Exécuter le pipeline ELT complet

```bash
python flows/main_pipeline.py
```

Ce pipeline :
- Ingère les CSV dans MinIO (Bronze)
- Nettoie les données (Silver)
- Crée les agrégations métier (Gold)

### 7. Charger les données Gold dans MongoDB

```bash
python flows/load_to_mongodb.py
```

### 8. Lancer l'API FastAPI

```bash
uvicorn api.app:app --port 8000
```

### 9. Lancer le Dashboard Streamlit

Dans un nouveau terminal :

```bash
streamlit run dashboard/app.py
```

## 🔗 URLs d'accès

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Dashboard Streamlit** | http://localhost:8501 | - |
| **API FastAPI (Swagger)** | http://localhost:8000/docs | - |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Mongo Express** | http://localhost:8081 | - |
| **Prefect UI** | http://localhost:4200 | - |

## 📁 Structure du projet

```
big data/
├── api/                      # API FastAPI
│   ├── app.py               # Application principale
│   ├── database.py          # Connexion MongoDB
│   ├── models.py            # Schémas Pydantic
│   └── routes/              # Endpoints
│       ├── clients.py
│       ├── products.py
│       ├── sales.py
│       └── kpis.py
├── dashboard/               # Dashboard Streamlit
│   └── app.py
├── data/
│   └── sources/             # Données CSV générées
├── flows/                   # Pipelines Prefect
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── gold_transformation.py
│   ├── load_to_mongodb.py
│   ├── main_pipeline.py
│   └── config.py
├── script/
│   └── generate_data.py     # Génération de données fake
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 📊 Fonctionnalités du Dashboard

- **KPIs globaux** : Total clients, achats, CA, panier moyen
- **Graphiques** :
  - Évolution des ventes mensuelles (line chart)
  - Top produits par revenu (bar chart)
  - Répartition CA par pays (pie chart)
  - Top clients (bar chart)
- **Tableaux détaillés** : Clients, Produits, Pays
- **Métriques de performance** : Temps de réponse API/MongoDB
- **Auto-refresh** configurable

## 🔌 Endpoints API

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/collections` | Liste des collections MongoDB |
| GET | `/clients` | Liste paginée des clients |
| GET | `/clients/top` | Top clients |
| GET | `/clients/{id}` | Détail d'un client |
| GET | `/products` | Liste des produits |
| GET | `/products/top` | Top produits |
| GET | `/sales/monthly` | Ventes mensuelles |
| GET | `/sales/by-country` | Ventes par pays |
| GET | `/kpis` | KPIs globaux |
| GET | `/kpis/refresh-metrics` | Métriques de temps de réponse |

## 🛑 Arrêter le projet

```bash
# Arrêter les containers
docker-compose down

# Arrêter et supprimer les volumes (reset complet)
docker-compose down -v
```

## 👤 Auteur

Denis BUCSPUN - M2 IWID
