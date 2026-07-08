"""Analyse stockage Medallion + projection dataset complet."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def fmt_gb(nbytes: int) -> float:
    return round(nbytes / (1024**3), 3)


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob("*") if _.is_file())


def layer_breakdown(base: Path) -> list[tuple[str, float, int]]:
    if not base.exists():
        return []
    rows = []
    for child in sorted(base.iterdir()):
        if child.is_dir():
            sz = dir_size(child)
            rows.append((child.name, fmt_gb(sz), count_files(child)))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


def raw_stats(month: str | None = None) -> dict:
    raw = ROOT / "data" / "raw"
    total_bytes = 0
    month_bytes = 0
    file_count = 0
    month_files = 0
    by_type: dict[str, dict] = {}

    for vdir in sorted(raw.iterdir()):
        if not vdir.is_dir():
            continue
        files = list(vdir.glob("*.parquet"))
        t_bytes = sum(f.stat().st_size for f in files)
        m_files = [f for f in files if month and month in f.name]
        m_bytes = sum(f.stat().st_size for f in m_files)
        by_type[vdir.name] = {
            "files": len(files),
            "gb": fmt_gb(t_bytes),
            "month_gb": fmt_gb(m_bytes) if month else None,
        }
        total_bytes += t_bytes
        month_bytes += m_bytes
        file_count += len(files)
        month_files += len(m_files)

    return {
        "total_gb": fmt_gb(total_bytes),
        "month_gb": fmt_gb(month_bytes) if month else None,
        "files": file_count,
        "month_files": month_files,
        "by_type": by_type,
    }


def read_metrics_tail(n: int = 20) -> list[dict]:
    path = ROOT / "logs" / "pipeline_metrics.jsonl"
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    out = []
    for line in lines[-n:]:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def silver_partition_detail() -> list[tuple[str, float, int]]:
    silver = ROOT / "data" / "silver" / "trips_unified"
    if not silver.exists():
        return []
    rows = []
    for vt_dir in silver.iterdir():
        if not vt_dir.is_dir():
            continue
        for ym_dir in vt_dir.iterdir():
            if ym_dir.is_dir():
                label = f"{vt_dir.name}/{ym_dir.name}"
                rows.append((label, fmt_gb(dir_size(ym_dir)), count_files(ym_dir)))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


def project_full(
    raw_total_gb: float,
    raw_month_gb: float,
    bronze_gb: float,
    silver_gb: float,
    gold_gb: float,
    lake_gb: float,
) -> dict:
    if raw_month_gb <= 0:
        return {}
    ratio = raw_total_gb / raw_month_gb
    return {
        "bronze_gb": round(bronze_gb * ratio, 1),
        "silver_gb": round(silver_gb * ratio, 1),
        "gold_gb": round(gold_gb * ratio, 2),
        "lake_gb": round(lake_gb * ratio, 1),
        "total_gb": round((bronze_gb + silver_gb + gold_gb + lake_gb) * ratio, 1),
        "hdfs_docker_gb": round((bronze_gb + silver_gb + gold_gb + lake_gb) * ratio * 1.3, 1),
        "months_ratio": round(ratio, 1),
    }


def build_report(month: str, duration_min: float) -> str:
    raw = raw_stats(month)
    layers = {
        "bronze": fmt_gb(dir_size(ROOT / "data" / "bronze")),
        "silver": fmt_gb(dir_size(ROOT / "data" / "silver")),
        "gold": fmt_gb(dir_size(ROOT / "data" / "gold")),
        "lake": fmt_gb(dir_size(ROOT / "data" / "lake")),
    }
    generated = sum(layers.values())
    raw_month = raw["month_gb"] or 0
    amp = round(generated / max(raw_month, 0.001), 1)

    proj = project_full(
        raw["total_gb"], raw_month, layers["bronze"], layers["silver"],
        layers["gold"], layers["lake"],
    )

    metrics = read_metrics_tail(25)
    silver_rows = sum(
        m.get("rows_written", 0)
        for m in metrics
        if m.get("layer") == "silver" and m.get("status") == "success"
    )
    bronze_rows = sum(
        m.get("rows_written", 0)
        for m in metrics
        if m.get("layer") == "bronze" and m.get("operation", "").startswith("ingest_trips")
        and m.get("status") == "success"
    )

    lines = [
        f"# Rapport stockage — {month}",
        "",
        f"**Mode** : local Windows (sans Docker)  ",
        f"**Duree pipeline** : {duration_min} min",
        "",
        "## 1. Entree (raw)",
        "",
        f"| Metrique | Valeur |",
        f"|----------|--------|",
        f"| Dataset complet | {raw['total_gb']} Go ({raw['files']} fichiers) |",
        f"| Mois {month} | {raw_month} Go ({raw['month_files']} fichiers) |",
        f"| Part du dataset | {round(100 * raw_month / max(raw['total_gb'], 0.001), 1)} % |",
        "",
        "### Par type vehicule (raw)",
        "",
        "| Type | Fichiers | Total Go | Mois Go |",
        "|------|----------|----------|---------|",
    ]
    for vt, info in raw["by_type"].items():
        lines.append(f"| {vt} | {info['files']} | {info['gb']} | {info.get('month_gb', '-')} |")

    lines += [
        "",
        "## 2. Stockage genere (1 mois)",
        "",
        "| Couche | Go | Fichiers | % du total genere |",
        "|--------|-----|----------|-------------------|",
    ]
    for name, gb in layers.items():
        fc = count_files(ROOT / "data" / name)
        pct = round(100 * gb / max(generated, 0.001), 1)
        lines.append(f"| {name.capitalize()} | {gb} | {fc} | {pct} % |")
    lines.append(f"| **Total** | **{round(generated, 3)}** | | 100 % |")
    lines.append(f"| Ratio amplification (genere / raw mois) | **x{amp}** | | |")

    if bronze_rows:
        lines += ["", f"Lignes ingerees Bronze : **{bronze_rows:,}**"]
    if silver_rows:
        lines.append(f"Lignes ecrites Silver : **{silver_rows:,}**")

    lines += [
        "",
        "## 3. Pourquoi Silver explose (surtout en full run)",
        "",
        "### Bronze vs Silver — logique",
        "",
        "| Etape | Ce qui se passe | Impact taille |",
        "|-------|-----------------|---------------|",
        "| **Bronze** | Copie integrale du Parquet raw + 4 colonnes metadata | ~1x raw (legere hausse) |",
        "| **Silver** | Schema unifie 22 colonnes + jointure zones (noms texte) | Souvent **plus gros que le raw** car : |",
        "",
        "**Causes principales de l'explosion Silver :**",
        "",
        "1. **Bronze garde TOUTES les colonnes source** (fhvhv = 20+ colonnes Uber/Lyft). Silver lit Bronze entier en memoire.",
        "2. **Colonnes texte zones** (`pu_borough`, `pu_zone_name`, `do_borough`, `do_zone_name`) — strings repetees, mal compressees vs entiers.",
        "3. **Partitionnement** `vehicle_type` x `year_month` → des centaines de petits fichiers Parquet (overhead metadata ~30-50 %).",
        "4. **Pas de coalesce** avant ecriture → Spark cree 1 fichier par partition shuffle (8 par defaut).",
        "5. **Full run** : Silver unifie **tous les mois** en une seule table overwrite → 306M lignes accumulees.",
        "6. **Docker/HDFS** : triple stockage possible (raw monte + bronze HDFS + silver HDFS + logs Spark).",
        "",
        f"Sur {month} : Silver = {layers['silver']} Go pour {raw_month} Go raw → ratio **x{round(layers['silver']/max(raw_month,0.001),1)}**",
        "",
        "### Detail partitions Silver",
        "",
    ]
    for label, gb, fc in silver_partition_detail():
        lines.append(f"- `{label}` : {gb} Go ({fc} fichiers)")

    lines += [
        "",
        "### Detail Bronze",
        "",
    ]
    for name, gb, fc in layer_breakdown(ROOT / "data" / "bronze"):
        lines.append(f"- `{name}` : {gb} Go ({fc} fichiers)")

    if proj:
        lines += [
            "",
            "## 4. Projection dataset COMPLET (6,82 Go raw)",
            "",
            f"Extrapolation lineaire depuis {month} (ratio x{proj['months_ratio']} sur le volume raw) :",
            "",
            "| Couche | 1 mois | **Estime full local** | **Estime full Docker/HDFS** |",
            "|--------|--------|----------------------|------------------------------|",
            f"| Bronze | {layers['bronze']} Go | {proj['bronze_gb']} Go | {round(proj['bronze_gb']*1.2,1)} Go |",
            f"| Silver | {layers['silver']} Go | **{proj['silver_gb']} Go** | **{round(proj['silver_gb']*1.3,1)} Go** |",
            f"| Gold | {layers['gold']} Go | {proj['gold_gb']} Go | {proj['gold_gb']} Go |",
            f"| Lake | {layers['lake']} Go | {proj['lake_gb']} Go | {round(proj['lake_gb']*1.5,1)} Go |",
            f"| **Total Medallion** | {round(generated,2)} Go | **{proj['total_gb']} Go** | **{proj['hdfs_docker_gb']} Go** |",
            "",
            "Ajouts Docker non comptes ci-dessus :",
            "- Spark event logs (/spark-logs) : 5-15 Go sur run 12h",
            "- Shuffles temporaires HDFS : 10-30 Go pic",
            "- Images Docker + MongoDB + Prometheus : 2-5 Go",
            "",
            f"**Estimation realiste crash 150 Go** : {proj['hdfs_docker_gb']} Go Medallion + 30-50 Go temporaires + raw monte = **100-180 Go**",
            "",
            "> fhvhv = 5,63 Go (83 % du raw). C'est lui qui drive Silver en full.",
        ]

    lines += [
        "",
        "## 5. Suffisant pour la soutenance ?",
        "",
        "**Oui.** Un mois (4 types, ~25-30M lignes estime) permet de demontrer :",
        "- Architecture Medallion Bronze → Silver → Gold → Lake",
        "- 12 KPIs warehouse + collections Lake (geo, anomalies, ML)",
        "- Sources externes (meteo Open-Meteo, trafic NYC Open Data)",
        "- Pipeline reproductible en <20 min en local",
        "",
        "Le full run (306M lignes) sert de **preuve d'echelle** via `logs/pipeline_metrics.jsonl`, pas obligatoire en live.",
        "",
        "## 6. Optimisations recommandees",
        "",
        "| Priorite | Action | Gain estime |",
        "|----------|--------|-------------|",
        "| **P1** | `coalesce(4)` avant ecriture Silver | -40 % fichiers, -20 % taille |",
        "| **P1** | Stocker zones en ID seulement, join a la lecture Gold/Lake | -30 % Silver |",
        "| **P2** | Bronze : projection colonnes utiles (pas copie integrale) | -50 % Bronze fhvhv |",
        "| **P2** | Traitement incremental par mois (append) | evite re-ecriture 306M lignes |",
        "| **P3** | Desactiver Spark event log en demo | -5 Go Docker |",
        "| **P3** | `shuffle_partitions=4` en local sample | moins de CPU/RAM |",
        "",
        "## 7. Dernieres metriques",
        "",
    ]
    for m in metrics[-12:]:
        op = m.get("operation", "")
        status = m.get("status", "")
        rr = m.get("rows_read", 0)
        rw = m.get("rows_written", 0)
        dur = round(m.get("duration_ms", 0) / 1000, 1)
        lines.append(f"- `{m.get('layer')}/{op}` {status} — read {rr:,} write {rw:,} ({dur}s)")

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-01"
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 0
    report = build_report(month, duration)
    out = ROOT / "logs" / f"rapport_{month.replace('-', '')}.md"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n>>> Sauvegarde : {out}")
