"""Pipeline metrics: JSONL logs + Prometheus export."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


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

    def record(self, metric: MetricRecord) -> None:
        self._records.append(metric)
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

    def _export_prometheus(self) -> None:
        lines: list[str] = []
        for rec in self._records:
            labels = f'layer="{rec.layer}",operation="{rec.operation}",status="{rec.status}"'
            lines.append(f"pipeline_rows_read{{{labels}}} {rec.rows_read}")
            lines.append(f"pipeline_rows_written{{{labels}}} {rec.rows_written}")
            lines.append(f"pipeline_duration_ms{{{labels}}} {rec.duration_ms}")
            lines.append(f"pipeline_null_rate{{{labels}}} {rec.null_rate}")
            lines.append(f"pipeline_duplicates_removed{{{labels}}} {rec.duplicates_removed}")
            lines.append(f"pipeline_invalid_records{{{labels}}} {rec.invalid_records}")
        self.prometheus_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
