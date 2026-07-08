# Guide soutenance — vendredi

## Ce que tu montres (15 min demo)

1. **Architecture** (2 min) — Medallion Bronze → Silver → Gold + Lake
2. **Live demo** (8 min) — `python scripts\status.py` puis requêtes KPI/Lake
3. **Full run** (2 min) — montrer `logs/pipeline_metrics.jsonl` : 306M lignes Silver traitées
4. **Stack Docker** (3 min) — MongoDB, Spark UI, Grafana si profil monitoring actif

## Mercredi (aujourd'hui)

```powershell
.\scripts\prepare_soutenance.ps1 -Mode local
```

Valider que tout passe. Noter les chiffres (lignes Silver, collections MongoDB).

## Jeudi (répétition)

```powershell
# Option A : local (stable, ~15 min)
.\scripts\prepare_soutenance.ps1 -Mode local

# Option B : Docker cluster (impressionnant, ~5 Go RAM)
.\scripts\prepare_soutenance.ps1 -Mode docker
```

Répéter le discours. Ne pas relancer le **full** (6,8 Go) — risque disque.

## Vendredi matin (avant jury)

```powershell
.\scripts\run_mongodb.ps1                    # si Docker
python -m src.pipeline.run_pipeline --layer gold --sample   # refresh KPIs si besoin
python scripts\status.py
```

## Chiffres clés à citer

| Métrique | Valeur |
|----------|--------|
| Volume source | 6,82 Go, 53 fichiers, 4 types |
| Run sample Silver | ~24M lignes |
| Run full Silver (logs) | **306M lignes** |
| KPIs warehouse | 12 collections MongoDB |
| Lake analytics | 6 collections (géo, ML, anomalies) |
| Durée sample | ~12-15 min |
| Durée full (logs) | ~12 h |

## URLs demo

| Service | URL |
|---------|-----|
| Mongo Express | http://localhost:8081 (admin/admin) |
| Spark Master | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin/admin) |
| HDFS | http://localhost:9870 |

## En cas de problème

| Problème | Solution |
|----------|----------|
| MongoDB vide | `.\scripts\run_mongodb.ps1` puis `--layer gold --sample` |
| Docker crash | Mode **local** sans Docker |
| PySpark Windows | `.\scripts\setup_hadoop.ps1` |
| RAM insuffisante | `--sample` ou `--light` |

## Ne pas faire avant vendredi

- Run complet sans `--sample` (saturation disque)
- Profil Docker `full` si < 16 Go RAM libre
- Supprimer les volumes Docker
