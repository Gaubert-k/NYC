# NYC Taxi Data Lakehouse

Plateforme Medallion (Bronze → Silver → Gold) + couche **Lake** sur les données NYC TLC.

**Exécution 100 % Docker** — point d'entrée unique via `make`.

## Prérequis

- Docker Desktop (16 Go+ RAM recommandé)
- GNU Make (`choco install make` ou Git Bash / WSL)

## Démarrage rapide

```bash
cp docker/.env.example docker/.env
make up
make test-2026          # pipeline filtré année 2026 (~30 min)
make status             # état stack + HDFS + MongoDB
```

## Commandes Make

| Commande | Description |
|----------|-------------|
| `make up` | Démarre la stack complète |
| `make down` | Arrête et supprime les conteneurs |
| `make pipeline` | Pipeline complet (toutes les données raw) |
| `make test-2026` | Pipeline année 2026 uniquement |
| `make status` | État stack (Docker, HDFS, MongoDB) |
| `make urls` | URLs Grafana + dashboards |

Le pipeline est exécuté par `scripts/docker/pipeline.sh` (spark-submit dans `nyc-spark-client`).

## Architecture

| Couche | Rôle | Stockage Docker |
|--------|------|-----------------|
| **Bronze** | Ingestion brute + sources externes | HDFS `/data/bronze` |
| **Silver** | Nettoyage, enrichissement | HDFS `/data/silver` |
| **Gold** | KPIs warehouse BI | HDFS + MongoDB |
| **Lake** | ML, géo, anomalies, exploration | HDFS + MongoDB `lake_*` |

Sources raw : `data/raw/` (volume monté, non copié dans vhdx).

## Services web

| Service | URL |
|---------|-----|
| Spark Master | http://localhost:8080 |
| **Spark History** | http://localhost:18080 |
| HDFS NameNode | http://localhost:9870 |
| Mongo Express | http://localhost:8081 (admin/admin) |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

## Configuration Spark (docker/.env)

| Variable | Valeur | Description |
|----------|--------|-------------|
| `SPARK_WORKER_MEMORY` | 3G | RAM par worker |
| `SPARK_WORKER_CORES` | 4 | CPU par worker |
| `SPARK_MASTER_MEM_LIMIT` | 1g | RAM master |
| `SPARK_EXECUTOR_MEMORY` | 2g | RAM par executor |
| `SPARK_NUM_EXECUTORS` | 2 | 1 executor par worker |
| `SPARK_EVENT_LOG_DIR` | hdfs://…/spark-logs | Historique jobs |

## Monitoring & KPIs

Tout passe par **Grafana** (http://localhost:3000, admin/admin) — dossier **NYC Lakehouse** :

| Dashboard | Contenu |
|-----------|---------|
| **01 — Pipeline (technique)** | Durées, volumes, qualité Silver (Prometheus) |
Tout passe par **Grafana** (http://localhost:3000, admin/admin) — dossier **NYC Lakehouse** :

| Dashboard | Contenu |
|-----------|---------|
| **01 — Pipeline (technique)** | Durées, volumes, qualité Silver |
| **02 — KPIs metier** | Tarifs, zones, tendances, paiements |
| **03 — Lake** | MAE/R² ML, top routes, anomalies |

Les KPIs métier sont exportés depuis MongoDB vers `logs/kpi_metrics.prom` puis affichés via **Prometheus** (pas de script terminal).

```bash
make status    # état stack (pas de requêtes KPI en terminal)
make urls      # liste des dashboards
```

## Couche Lake

| Output | Description |
|--------|-------------|
| `geospatial/od_zone_flows` | Flux origine-destination |
| `geospatial/zone_heatmap` | Heatmap pickup |
| `geospatial/top_routes` | Top 50 trajets |
| `ml/anomaly_trips` | Outliers tarif/distance |
| `ml/duration_model_metrics.json` | GradientBoosting durée |
| `exploration/traffic_taxi_correlation` | Collisions × volume taxi |

## Soutenance

```bash
make up
make test-2026
make status
# Grafana : http://localhost:3000 → dossier NYC Lakehouse
#   01 Pipeline | 02 KPIs metier | 03 Lake
```

Ne pas relancer le dataset complet (6,8 Go) sans RAM/disque suffisant — `make test-2026` suffit.
