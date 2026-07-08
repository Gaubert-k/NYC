# NYC Taxi Data Lakehouse

Plateforme Medallion (Bronze → Silver → Gold) sur les données NYC TLC Taxi & Ride.

## Prérequis

- Python 3.10+
- Java 11 ou 17 (Temurin recommandé) pour PySpark
- ~16 Go RAM pour un run local complet
- MongoDB optionnel (fallback Parquet dans `data/gold/`)

### Windows : configuration Hadoop (obligatoire pour PySpark)

```powershell
.\scripts\setup_hadoop.ps1
```

Cela installe `winutils.exe` et `hadoop.dll` dans `tools/hadoop/bin/`. Le pipeline configure automatiquement `HADOOP_HOME`.

## Installation

```powershell
cd c:\Users\GuillaumeAUBERT\Desktop\IPSSI\NYC
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy config\.env.example .env
```

## Structure

```
data/raw/       → Parquet sources (déjà présents)
data/bronze/    → Données brutes ingérées + sources externes
data/silver/    → Données nettoyées et enrichies
data/gold/      → KPIs warehouse (Parquet + MongoDB)
data/lake/      → Analytics lakehouse (ML, géo, exploration)
src/            → Code pipeline PySpark
docker/         → Stack complète (ne pas lancer sans RAM dispo)
```

## Architecture : Warehouse vs Lake

| Couche | Rôle | Exemple |
|--------|------|---------|
| **Gold** | Warehouse — KPIs BI prédéfinis | Tarif moyen, top zones |
| **Lake** | Analytics flexibles sur Silver | ML, heatmaps, anomalies, schema drift |

En mode Docker, les couches Bronze/Silver/Gold/Lake sont écrites sur **HDFS** (`hdfs://namenode:9000/data/`) ; les sources raw/reference restent sur le volume local (`file://`). Les analytics Lake sont aussi chargées dans MongoDB (collections `lake_*`).

## Commandes (mode local, sans Docker)

```powershell
# Mode léger ~1 Go RAM (couche Lake si Silver déjà présent)
.\scripts\run_lake_light.ps1

# Pipeline complet en mode échantillon
python -m src.pipeline.run_pipeline --layer all --sample

# Warehouse seul (KPIs)
python -m src.pipeline.run_pipeline --layer gold

# Lake seul (ML, géo, anomalies, exploration)
python -m src.pipeline.run_pipeline --layer lake --light

# Voir les outputs Lake
python scripts\show_lake_outputs.py

# Tableau de bord complet (Docker, HDFS, MongoDB, metriques)
python scripts\status.py

# Requetes MongoDB
python scripts\query_kpis.py
python scripts\query_lake.py
```

## Soutenance (vendredi)

Guide détaillé : `docs/SOUTENANCE.md`

```powershell
# Mercredi/Jeudi : préparer la démo (~15 min)
.\scripts\prepare_soutenance.ps1 -Mode local

# Vendredi matin : vérification rapide
python scripts\status.py
python scripts\query_kpis.py
```

**Ne pas relancer le run complet** (6,8 Go) — utiliser `--sample`. Les logs prouvent le full run (306M lignes Silver).

## KPIs (12 collections MongoDB)

1. Volume par zone × heure
2. Volume par zone × jour
3. Tarif moyen par type véhicule
4. Tarif moyen par heure
5. Distance moyenne par zone
6. Tendance mensuelle
7. Top 10 zones pickup
8. Top 10 zones dropoff
9. Répartition modes paiement
10. Durée moyenne (fhvhv)
11. Taux courses partagées (fhvhv)
12. Corrélation météo × volume

## Monitoring

- Logs JSONL : `logs/pipeline_metrics.jsonl`
- Export Prometheus : `logs/pipeline_metrics.prom` (scrape via node-exporter → Grafana)
- Dashboard Grafana : **NYC Taxi Pipeline Monitoring** (dossier *NYC Lakehouse*, admin/admin)

## Docker — Stack complete

Profils disponibles (RAM estimee) :

| Profil | Services | RAM |
|--------|----------|-----|
| `mongodb` | MongoDB | ~500 Mo |
| `storage` | HDFS (namenode + datanode) | ~2 Go |
| `spark` | HDFS + Spark (2 workers) + client + MongoDB | ~5 Go |
| `monitoring` | Prometheus + Grafana + exporters | ~1.5 Go |
| `data` | MongoDB + HDFS | ~3 Go |
| `full` | Tout | ~10-14 Go |

```powershell
# MongoDB seul (deja fait)
.\scripts\run_mongodb.ps1

# Stack complete (16 Go RAM recommande)
.\scripts\run_docker.ps1 -Profile full

# Sous-ensembles
.\scripts\run_docker.ps1 -Profile spark
.\scripts\run_docker.ps1 -Profile storage
.\scripts\run_docker.ps1 -Profile monitoring

# Pipeline dans le cluster Spark (écritures Medallion sur HDFS)
.\scripts\run_pipeline_docker.ps1 -Sample

# Demo complete (pipeline + verification MongoDB)
.\scripts\run_demo.ps1 -Sample

# Sync couches locales vers HDFS (optionnel)
.\scripts\sync_to_hdfs.ps1

# Arreter
.\scripts\stop_docker.ps1

# Nettoyage disque Docker (apres crash ou full run)
.\scripts\cleanup_docker.ps1
```

### URLs des services (profil full)

| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| MongoDB | mongodb://localhost:27017 |
| Mongo Express | http://localhost:8081 (admin/admin) |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| cAdvisor | http://localhost:8082 |

## Sources externes (Bronze)

- Météo NYC — [Open-Meteo API](https://open-meteo.com/) → `data/bronze/weather/`
- Collisions routières NYC — [NYC Open Data](https://data.cityofnewyork.us/) → `data/bronze/external/traffic_collisions/`

## Couche Lake (`data/lake/`)

| Output | Description |
|--------|-------------|
| `geospatial/od_zone_flows` | Matrice origine-destination par zone/heure |
| `geospatial/zone_heatmap` | Heatmap pickup par zone |
| `geospatial/top_routes` | Top 50 trajets zone → zone |
| `ml/anomaly_trips` | Outliers tarif/distance (z-score) |
| `ml/duration_model_metrics.json` | Modèle ML prédiction durée |
| `exploration/schema_drift_report.json` | Évolution schémas par type véhicule |
| `exploration/traffic_taxi_correlation` | Collisions × volume taxi par borough |
