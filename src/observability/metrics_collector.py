"""Coleta e estrutura métricas operacionais para observabilidade local."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

VALID_STATUSES = ("success", "warning", "failed")


@dataclass(frozen=True, slots=True)
class GeneratedArtifact:
    """Representa um artefato produzido por uma execução de pipeline."""

    path: str
    size_bytes: int
    file_count: int

    @property
    def size_human(self) -> str:
        return format_size_bytes(self.size_bytes)

    def to_record(self) -> dict[str, object]:
        return {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "size_human": self.size_human,
            "file_count": self.file_count,
        }


@dataclass(frozen=True, slots=True)
class PipelineExecutionMetric:
    """Métrica consolidada de uma execução de pipeline."""

    job_name: str
    started_at: datetime
    finished_at: datetime
    source_layer: str
    target_layer: str
    records_in: int
    records_out: int
    invalid_records: int
    valid_data_percentage: float
    generated_files: tuple[GeneratedArtifact, ...]
    approx_file_size_bytes: int
    status: str
    entity_metrics: tuple[dict[str, object], ...] = ()
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        return round((self.finished_at - self.started_at).total_seconds(), 2)

    @property
    def approx_file_size_human(self) -> str:
        return format_size_bytes(self.approx_file_size_bytes)

    def to_record(self) -> dict[str, object]:
        return {
            "job_name": self.job_name,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "source_layer": self.source_layer,
            "target_layer": self.target_layer,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "invalid_records": self.invalid_records,
            "valid_data_percentage": self.valid_data_percentage,
            "generated_files": [artifact.to_record() for artifact in self.generated_files],
            "approx_file_size_bytes": self.approx_file_size_bytes,
            "approx_file_size_human": self.approx_file_size_human,
            "status": self.status,
            "entity_metrics": [dict(metric) for metric in self.entity_metrics],
            "error_message": self.error_message,
        }


def calculate_valid_data_percentage(records_in: int, invalid_records: int) -> float:
    """Calcula o percentual aproximado de dados válidos para uma execução."""

    if records_in <= 0:
        return 100.0
    valid_records = max(records_in - invalid_records, 0)
    return round((valid_records / records_in) * 100, 2)


def determine_execution_status(
    invalid_records: int = 0,
    explicit_status: str | None = None,
    error_message: str | None = None,
) -> str:
    """Define o status final da execução com base nas métricas e no erro."""

    if explicit_status is not None:
        if explicit_status not in VALID_STATUSES:
            raise ValueError(f"Status inválido: {explicit_status}. Use {VALID_STATUSES}.")
        return explicit_status
    if error_message:
        return "failed"
    if invalid_records > 0:
        return "warning"
    return "success"


def collect_generated_artifacts(paths: Iterable[Path]) -> tuple[GeneratedArtifact, ...]:
    """Coleta tamanho aproximado e quantidade de arquivos dos artefatos gerados."""

    artifacts: list[GeneratedArtifact] = []
    seen_paths: set[str] = set()

    for raw_path in paths:
        path = raw_path.resolve()
        if not path.exists():
            continue
        normalized_path = str(path)
        if normalized_path in seen_paths:
            continue
        seen_paths.add(normalized_path)

        file_paths = _collect_files(path)
        size_bytes = sum(file_path.stat().st_size for file_path in file_paths)
        artifacts.append(
            GeneratedArtifact(
                path=normalized_path,
                size_bytes=size_bytes,
                file_count=len(file_paths),
            )
        )

    return tuple(sorted(artifacts, key=lambda artifact: artifact.path))


def build_pipeline_execution_metric(
    job_name: str,
    started_at: datetime,
    finished_at: datetime,
    source_layer: str,
    target_layer: str,
    records_in: int,
    records_out: int,
    invalid_records: int,
    generated_paths: Iterable[Path],
    status: str | None = None,
    entity_metrics: Iterable[dict[str, object]] | None = None,
    error_message: str | None = None,
) -> PipelineExecutionMetric:
    """Monta uma métrica consolidada pronta para persistência."""

    artifacts = collect_generated_artifacts(generated_paths)
    total_size_bytes = sum(artifact.size_bytes for artifact in artifacts)
    resolved_status = determine_execution_status(
        invalid_records=invalid_records,
        explicit_status=status,
        error_message=error_message,
    )

    return PipelineExecutionMetric(
        job_name=job_name,
        started_at=started_at,
        finished_at=finished_at,
        source_layer=source_layer,
        target_layer=target_layer,
        records_in=records_in,
        records_out=records_out,
        invalid_records=invalid_records,
        valid_data_percentage=calculate_valid_data_percentage(records_in, invalid_records),
        generated_files=artifacts,
        approx_file_size_bytes=total_size_bytes,
        status=resolved_status,
        entity_metrics=tuple(entity_metrics or ()),
        error_message=error_message,
    )


def format_size_bytes(size_bytes: int) -> str:
    """Converte bytes para uma representação legível."""

    size = float(max(size_bytes, 0))
    units = ("B", "KB", "MB", "GB", "TB")

    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{int(size_bytes)} B"


def _collect_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(candidate for candidate in path.rglob("*") if candidate.is_file())


__all__ = [
    "GeneratedArtifact",
    "PipelineExecutionMetric",
    "build_pipeline_execution_metric",
    "calculate_valid_data_percentage",
    "collect_generated_artifacts",
    "determine_execution_status",
    "format_size_bytes",
]
