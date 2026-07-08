"""Lake layer: schema drift & ad-hoc exploration reports."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession

from src.common.metrics import MetricsTracker
from src.common.storage import ensure_local_dir, local_path


def run_schema_exploration(spark: SparkSession, config: dict[str, Any], metrics: MetricsTracker) -> None:
    raw_dir = local_path(config, "raw")
    report: dict[str, Any] = {"vehicle_types": {}}

    with metrics.track("lake", "schema_drift_exploration") as metric:
        for vtype in config.get("vehicle_types", []):
            vdir = raw_dir / vtype
            files = sorted(vdir.glob("*.parquet"))
            if config.get("sample_mode") or config.get("light_mode"):
                files = files[:1]
            if not files:
                continue

            schemas = []
            for f in files:
                df = spark.read.parquet(f.as_uri())
                schemas.append(
                    {
                        "file": f.name,
                        "columns": df.columns,
                        "column_count": len(df.columns),
                        "row_count": df.count() if not config.get("light_mode") else None,
                    }
                )
            all_cols = set()
            for s in schemas:
                all_cols.update(s["columns"])
            report["vehicle_types"][vtype] = {
                "files_analyzed": len(schemas),
                "union_columns": sorted(all_cols),
                "per_file": schemas,
            }
            metric.rows_read += sum(s["row_count"] or 0 for s in schemas)

        out_dir = ensure_local_dir(config, "lake", "exploration")
        (out_dir / "schema_drift_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        metric.rows_written = len(report["vehicle_types"])
