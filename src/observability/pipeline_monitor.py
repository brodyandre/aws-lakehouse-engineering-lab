"""Persistência e consolidação de métricas de observabilidade dos pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from src.config.settings import Settings
from src.observability.metrics_collector import PipelineExecutionMetric
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


@dataclass(slots=True)
class PipelineMonitor:
    """Mantém um histórico local das execuções de pipeline."""

    settings: Settings = field(default_factory=Settings)
    json_path: Path | None = None
    markdown_path: Path | None = None

    def record(self, metric: PipelineExecutionMetric) -> dict[str, object]:
        json_path, markdown_path = self._resolve_paths()
        json_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.parent.mkdir(parents=True, exist_ok=True)

        records = self._load_records(json_path)
        records.append(metric.to_record())
        records = sorted(records, key=lambda record: record["started_at"], reverse=True)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": _build_summary(records),
            "pipelines": records,
        }

        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        markdown_path.write_text(_render_markdown(payload), encoding="utf-8")

        LOGGER.info(
            "Observabilidade atualizada para o job '%s' em %s e %s",
            metric.job_name,
            json_path,
            markdown_path,
        )
        return payload

    def _resolve_paths(self) -> tuple[Path, Path]:
        json_path = (
            self.json_path or self.settings.observability_report_path / "pipeline_metrics.json"
        ).resolve()
        markdown_path = (
            self.markdown_path or self.settings.observability_report_path / "pipeline_metrics.md"
        ).resolve()
        return json_path, markdown_path

    def _load_records(self, json_path: Path) -> list[dict[str, object]]:
        if not json_path.exists():
            return []

        payload = json.loads(json_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            pipelines = payload.get("pipelines", [])
            if isinstance(pipelines, list):
                return [record for record in pipelines if isinstance(record, dict)]
        if isinstance(payload, list):
            return [record for record in payload if isinstance(record, dict)]
        return []


def record_pipeline_metric(
    metric: PipelineExecutionMetric,
    settings: Settings | None = None,
    json_path: Path | None = None,
    markdown_path: Path | None = None,
) -> dict[str, object]:
    """Conveniência para persistir uma métrica sem instanciar o monitor manualmente."""

    monitor = PipelineMonitor(
        settings=settings or Settings(),
        json_path=json_path,
        markdown_path=markdown_path,
    )
    return monitor.record(metric)


def _build_summary(records: list[dict[str, object]]) -> dict[str, object]:
    status_counts = {"success": 0, "warning": 0, "failed": 0}
    for record in records:
        status = str(record.get("status", "success"))
        if status in status_counts:
            status_counts[status] += 1

    return {
        "total_executions": len(records),
        "success_executions": status_counts["success"],
        "warning_executions": status_counts["warning"],
        "failed_executions": status_counts["failed"],
        "last_job_name": records[0]["job_name"] if records else None,
    }


def _render_markdown(payload: dict[str, object]) -> str:
    summary = payload["summary"]
    records = payload["pipelines"]

    lines = [
        "# Pipeline Observability Metrics",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Total executions: `{summary['total_executions']}`",
        f"- Success: `{summary['success_executions']}`",
        f"- Warning: `{summary['warning_executions']}`",
        f"- Failed: `{summary['failed_executions']}`",
        "",
        (
            "| Job | Status | Source Layer | Target Layer | Started At | Finished At | "
            "Duration (s) | Records In | Records Out | Invalid Records | Valid % | "
            "Generated Files | Approx Size |"
        ),
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for record in records:
        generated_files = record.get("generated_files", [])
        lines.append(
            f"| {record['job_name']} | {record['status']} | {record['source_layer']} | "
            f"{record['target_layer']} | `{record['started_at']}` | `{record['finished_at']}` | "
            f"{record['duration_seconds']} | {record['records_in']} | {record['records_out']} | "
            f"{record['invalid_records']} | {record['valid_data_percentage']} | "
            f"{len(generated_files)} | {record['approx_file_size_human']} |"
        )

    for record in records:
        lines.extend(
            [
                "",
                f"## {record['job_name']} | {record['status']}",
                "",
                f"- Source layer: `{record['source_layer']}`",
                f"- Target layer: `{record['target_layer']}`",
                f"- Started at: `{record['started_at']}`",
                f"- Finished at: `{record['finished_at']}`",
                f"- Duration seconds: `{record['duration_seconds']}`",
                f"- Records in: `{record['records_in']}`",
                f"- Records out: `{record['records_out']}`",
                f"- Invalid records: `{record['invalid_records']}`",
                f"- Valid data percentage: `{record['valid_data_percentage']}`",
                f"- Approx file size: `{record['approx_file_size_human']}`",
            ]
        )

        if record.get("error_message"):
            lines.append(f"- Error: `{record['error_message']}`")

        generated_files = record.get("generated_files", [])
        if generated_files:
            lines.extend(
                [
                    "",
                    "### Generated Files",
                    "",
                    "| Path | File Count | Size |",
                    "| --- | ---: | --- |",
                ]
            )
            for artifact in generated_files:
                lines.append(
                    f"| `{artifact['path']}` | "
                    f"{artifact['file_count']} | {artifact['size_human']} |"
                )

        entity_metrics = record.get("entity_metrics", [])
        if entity_metrics:
            lines.extend(
                [
                    "",
                    "### Entity Metrics",
                    "",
                    "| Entity | Records In | Records Out | Invalid Records | Notes |",
                    "| --- | ---: | ---: | ---: | --- |",
                ]
            )
            for metric in entity_metrics:
                lines.append(
                    f"| {metric.get('entity', metric.get('table', '-'))} | "
                    f"{metric.get('records_in', metric.get('records_read', '-'))} | "
                    f"{metric.get('records_out', metric.get('records_written', '-'))} | "
                    f"{metric.get('invalid_records', '-')} | "
                    f"{metric.get('notes', '-') or '-'} |"
                )

    return "\n".join(lines) + "\n"


__all__ = ["PipelineMonitor", "record_pipeline_metric"]
