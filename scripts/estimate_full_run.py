"""Estimation durée run complet (hors --sample)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
raw = ROOT / "data" / "raw"
metrics = ROOT / "logs" / "pipeline_metrics.jsonl"

sample_files = 4  # 1 mois × 4 types
full_files = sum(1 for _ in raw.rglob("*.parquet"))
full_gb = sum(f.stat().st_size for f in raw.rglob("*.parquet")) / (1024**3)

# Dernier run Docker sample (4 types) ~12 min mesuré
sample_min = 12.0

# Ratio fichiers et volume
file_ratio = full_files / sample_files
size_ratio = full_gb / 0.55  # ~0.55 Go ingéré en sample

# Lake/anomalies plus que linéaire → facteur conservateur
lake_factor = 1.8

docker_est = sample_min * max(file_ratio, size_ratio**0.85) * lake_factor
local_est = docker_est * 1.4  # local[2], moins de parallélisme

print("=== Estimation run COMPLET (6,82 Go, 53 fichiers) ===\n")
print(f"Données raw : {full_gb:.2f} Go, {full_files} fichiers Parquet")
print(f"Référence sample : ~{sample_min:.0f} min (Docker, 4 types, 1 mois/type)\n")
print(f"Docker cluster (2 workers 1G) : {docker_est/60:.1f} - {docker_est*1.3/60:.1f} h")
print(f"Local Windows 16 Go        : {local_est/60:.1f} - {local_est*1.5/60:.1f} h")
print(f"\nBottleneck : fhvhv = {5.63:.1f} Go (~83% du volume)")
print("Risque OOM local si < 32 Go RAM sur le run complet.")
