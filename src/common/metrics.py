"""Pipeline metrics: JSONL logs + Prometheus export."""

from __future__ import annotations

import json
import re
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


_PROM_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)$")


@dataclass
class MetricRecord:
    layer: str
    operation: str
    status: str = "success"
    rows_read: int = 0
    rows_written: int = 0
    duration_ms: float = 0.0
    null_rate: float = 0.0
    duplicates_removed: int = 0
    invalid_records: int = 0
    extra: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class MetricsTracker:
    def __init__(self, log_path: str, prometheus_path: str):
        self.log_path = Path(log_path)
        self.prometheus_path = Path(prometheus_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.prometheus_path.parent.mkdir(parents=True, exist_ok=True)
        self._records: list[MetricRecord] = []
        self._session_keys: set[tuple[str, str]] = set()

    def record(self, metric: MetricRecord) -> None:
        self._records.append(metric)
        self._session_keys.add((metric.layer, metric.operation))
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(metric), ensure_ascii=False) + "\n")
        self._export_prometheus()

    @contextmanager
    def track(
        self,
        layer: str,
        operation: str,
        **extra: Any,
    ) -> Generator[MetricRecord, None, None]:
        metric = MetricRecord(layer=layer, operation=operation, extra=dict(extra))
        start = time.perf_counter()
        try:
            yield metric
            metric.status = "success"
        except Exception as exc:
            metric.status = "error"
            metric.extra["error"] = str(exc)
            raise
        finally:
            metric.duration_ms = round((time.perf_counter() - start) * 1000, 2)
            self.record(metric)

    @classmethod
    def rebuild_prometheus_from_jsonl(cls, log_path: str | Path, prometheus_path: str | Path) -> None:
        """Rebuild .prom from latest JSONL record per (layer, operation)."""
        log = Path(log_path)
        prom = Path(prometheus_path)
        if not log.exists():
            return

        latest: dict[tuple[str, str], MetricRecord] = {}
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = json.loads(line)
            key = (data["layer"], data["operation"])
            rec = MetricRecord(
                layer=data["layer"],
                operation=data["operation"],
                status=data.get("status", "success"),
                rows_read=int(data.get("rows_read", 0)),
                rows_written=int(data.get("rows_written", 0)),
                duration_ms=float(data.get("duration_ms", 0)),
                null_rate=float(data.get("null_rate", 0)),
                duplicates_removed=int(data.get("duplicates_removed", 0)),
                invalid_records=int(data.get("invalid_records", 0)),
                extra=data.get("extra", {}),
                timestamp=data.get("timestamp", ""),
            )
            prev = latest.get(key)
            if prev is None or rec.timestamp >= prev.timestamp:
                latest[key] = rec

        prom.parent.mkdir(parents=True, exist_ok=True)
        cls._write_prometheus_records(latest.values(), prom)

    def _parse_existing_prom(self) -> dict[str, str]:
        if not self.prometheus_path.exists():
            return {}
        merged: dict[str, str] = {}
        for line in self.prometheus_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = _PROM_LINE.match(line)
            if match:
                merged[f"{match.group(1)}{match.group(2) or ''}"] = match.group(3)
        return merged

    def _export_prometheus(self) -> None:
        existing = self._parse_existing_prom()
        for rec in self._records:
            labels = f'layer="{rec.layer}",operation="{rec.operation}",status="{rec.status}"'
            existing[f"pipeline_rows_read{{{labels}}}"] = str(rec.rows_read)
            existing[f"pipeline_rows_written{{{labels}}}"] = str(rec.rows_written)
            existing[f"pipeline_duration_ms{{{labels}}}"] = str(rec.duration_ms)
            existing[f"pipeline_null_rate{{{labels}}}"] = str(rec.null_rate)
            existing[f"pipeline_duplicates_removed{{{labels}}}"] = str(rec.duplicates_removed)
            existing[f"pipeline_invalid_records{{{labels}}}"] = str(rec.invalid_records)

        existing["pipeline_last_export_timestamp"] = str(int(time.time()))
        self._write_prometheus_lines(existing, self.prometheus_path)

    @staticmethod
    def _write_prometheus_records(records: Any, prom_path: Path) -> None:
        lines: dict[str, str] = {}
        for rec in records:
            labels = f'layer="{rec.layer}",operation="{rec.operation}",status="{rec.status}"'
            lines[f"pipeline_rows_read{{{labels}}}"] = str(rec.rows_read)
            lines[f"pipeline_rows_written{{{labels}}}"] = str(rec.rows_written)
            lines[f"pipeline_duration_ms{{{labels}}}"] = str(rec.duration_ms)
            lines[f"pipeline_null_rate{{{labels}}}"] = str(rec.null_rate)
            lines[f"pipeline_duplicates_removed{{{labels}}}"] = str(rec.duplicates_removed)
            lines[f"pipeline_invalid_records{{{labels}}}"] = str(rec.invalid_records)
        lines["pipeline_last_export_timestamp"] = str(int(time.time()))
        MetricsTracker._write_prometheus_lines(lines, prom_path)

    @staticmethod
    def _write_prometheus_lines(lines: dict[str, str], prom_path: Path) -> None:
        header = [
            "# HELP pipeline_duration_ms Duration of a pipeline step in milliseconds.",
            "# TYPE pipeline_duration_ms gauge",
            "# HELP pipeline_rows_read Rows read during a pipeline step.",
            "# TYPE pipeline_rows_read gauge",
            "# HELP pipeline_rows_written Rows written during a pipeline step.",
            "# TYPE pipeline_rows_written gauge",
            "# HELP pipeline_null_rate Null rate for a pipeline step (0-1).",
            "# TYPE pipeline_null_rate gauge",
            "# HELP pipeline_duplicates_removed Duplicate rows removed.",
            "# TYPE pipeline_duplicates_removed gauge",
            "# HELP pipeline_invalid_records Invalid records filtered out.",
            "# TYPE pipeline_invalid_records gauge",
            "# HELP pipeline_last_export_timestamp Unix time of last metrics export.",
            "# TYPE pipeline_last_export_timestamp gauge",
        ]
        body = [f"{name} {value}" for name, value in sorted(lines.items())]
        prom_path.write_text("\n".join(header + body) + "\n", encoding="utf-8")
