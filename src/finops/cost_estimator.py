"""Estimador local de custo para simulação de FinOps no Lakehouse."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.config.settings import Settings
from src.utils.logger import configure_logging, get_logger

LOGGER = get_logger(__name__)
SUPPORTED_LAYERS = ("raw", "bronze", "silver", "gold")


@dataclass(frozen=True, slots=True)
class FinOpsParameters:
    """Parâmetros configuráveis usados na simulação de custo."""

    storage_cost_per_gb_month: float = 0.023
    athena_cost_per_tb_scanned: float = 5.0
    parquet_scan_ratio: float = 0.35
    partition_pruning_ratio: float = 0.25
    small_file_threshold_mb: float = 16.0
    small_file_ratio_threshold: float = 0.5
    min_file_count_for_warning: int = 5


@dataclass(frozen=True, slots=True)
class LayerCostEstimate:
    """Resumo de custo e volume para uma camada do Lakehouse."""

    layer: str
    path: str
    file_count: int
    total_size_bytes: int
    total_size_human: str
    average_file_size_bytes: int
    average_file_size_human: str
    small_file_count: int
    small_file_ratio: float
    has_small_files_problem: bool
    estimated_s3_storage_cost_usd: float
    simulated_athena_scan_bytes: int
    simulated_athena_scan_cost_usd: float
    optimized_athena_scan_bytes: int
    optimized_athena_scan_cost_usd: float
    estimated_savings_bytes: int
    estimated_savings_cost_usd: float
    notes: str

    def to_record(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FinOpsEstimationReport:
    """Relatório consolidado de FinOps local."""

    generated_at: datetime
    parameters: FinOpsParameters
    layers: tuple[LayerCostEstimate, ...]

    def to_record(self) -> dict[str, object]:
        total_storage_bytes = sum(layer.total_size_bytes for layer in self.layers)
        total_storage_cost_usd = round(
            sum(layer.estimated_s3_storage_cost_usd for layer in self.layers),
            6,
        )
        total_scan_cost_usd = round(
            sum(layer.simulated_athena_scan_cost_usd for layer in self.layers),
            6,
        )
        total_optimized_scan_cost_usd = round(
            sum(layer.optimized_athena_scan_cost_usd for layer in self.layers),
            6,
        )
        total_savings_cost_usd = round(
            sum(layer.estimated_savings_cost_usd for layer in self.layers),
            6,
        )
        layers_with_small_files = [
            layer.layer for layer in self.layers if layer.has_small_files_problem
        ]

        return {
            "generated_at": self.generated_at.isoformat(),
            "parameters": asdict(self.parameters),
            "summary": {
                "total_layers": len(self.layers),
                "total_files": sum(layer.file_count for layer in self.layers),
                "total_storage_bytes": total_storage_bytes,
                "total_storage_human": format_size_bytes(total_storage_bytes),
                "total_estimated_s3_storage_cost_usd": total_storage_cost_usd,
                "total_simulated_athena_scan_cost_usd": total_scan_cost_usd,
                "total_optimized_athena_scan_cost_usd": total_optimized_scan_cost_usd,
                "total_estimated_savings_cost_usd": total_savings_cost_usd,
                "layers_with_small_files_problem": layers_with_small_files,
            },
            "layers": [layer.to_record() for layer in self.layers],
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Estima custo local e simulado de uma arquitetura Lakehouse sem usar AWS real."
    )
    parser.add_argument("--raw-dir", type=Path, default=settings.raw_data_path)
    parser.add_argument("--bronze-dir", type=Path, default=settings.bronze_data_path)
    parser.add_argument("--silver-dir", type=Path, default=settings.silver_data_path)
    parser.add_argument("--gold-dir", type=Path, default=settings.gold_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.finops_report_path / "cost_estimation.md",
    )
    parser.add_argument(
        "--json-path",
        type=Path,
        default=settings.finops_report_path / "cost_estimation.json",
    )
    parser.add_argument("--storage-cost-per-gb-month", type=float, default=0.023)
    parser.add_argument("--athena-cost-per-tb-scanned", type=float, default=5.0)
    parser.add_argument("--parquet-scan-ratio", type=float, default=0.35)
    parser.add_argument("--partition-pruning-ratio", type=float, default=0.25)
    parser.add_argument("--small-file-threshold-mb", type=float, default=16.0)
    parser.add_argument("--small-file-ratio-threshold", type=float, default=0.5)
    parser.add_argument("--min-file-count-for-warning", type=int, default=5)
    return parser.parse_args(argv)


def estimate_costs(
    settings: Settings | None = None,
    raw_dir: Path | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
    gold_dir: Path | None = None,
    parameters: FinOpsParameters | None = None,
) -> FinOpsEstimationReport:
    """Calcula as estimativas de custo por camada usando apenas o filesystem local."""

    active_settings = settings or Settings()
    active_parameters = parameters or FinOpsParameters()

    layer_paths = {
        "raw": (raw_dir or active_settings.raw_data_path).resolve(),
        "bronze": (bronze_dir or active_settings.bronze_data_path).resolve(),
        "silver": (silver_dir or active_settings.silver_data_path).resolve(),
        "gold": (gold_dir or active_settings.gold_data_path).resolve(),
    }

    layers = tuple(
        _scan_layer(layer=layer, layer_path=layer_paths[layer], parameters=active_parameters)
        for layer in SUPPORTED_LAYERS
    )

    return FinOpsEstimationReport(
        generated_at=datetime.now(timezone.utc),
        parameters=active_parameters,
        layers=layers,
    )


def write_reports(
    report: FinOpsEstimationReport,
    markdown_path: Path,
    json_path: Path,
) -> None:
    """Escreve os relatórios de FinOps em Markdown e JSON."""

    markdown_path = markdown_path.resolve()
    json_path = json_path.resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    payload = report.to_record()
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")

    LOGGER.info("Relatórios de FinOps gerados em %s e %s", markdown_path, json_path)


def run_cost_estimation(
    settings: Settings | None = None,
    raw_dir: Path | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
    gold_dir: Path | None = None,
    report_path: Path | None = None,
    json_path: Path | None = None,
    parameters: FinOpsParameters | None = None,
) -> FinOpsEstimationReport:
    """Executa o fluxo completo de estimação e persistência."""

    active_settings = settings or Settings()
    markdown_report_path = (
        report_path or active_settings.finops_report_path / "cost_estimation.md"
    ).resolve()
    json_report_path = (
        json_path or active_settings.finops_report_path / "cost_estimation.json"
    ).resolve()

    report = estimate_costs(
        settings=active_settings,
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        parameters=parameters,
    )
    write_reports(report, markdown_report_path, json_report_path)
    return report


def _scan_layer(
    layer: str,
    layer_path: Path,
    parameters: FinOpsParameters,
) -> LayerCostEstimate:
    data_files = _collect_data_files(layer_path)
    file_sizes = [file_path.stat().st_size for file_path in data_files]
    total_size_bytes = sum(file_sizes)
    file_count = len(data_files)
    average_file_size_bytes = int(total_size_bytes / file_count) if file_count else 0

    small_file_threshold_bytes = int(parameters.small_file_threshold_mb * 1024 * 1024)
    small_file_count = sum(1 for size in file_sizes if size < small_file_threshold_bytes)
    small_file_ratio = round((small_file_count / file_count), 4) if file_count else 0.0
    has_small_files_problem = _has_small_files_problem(
        file_count=file_count,
        average_file_size_bytes=average_file_size_bytes,
        small_file_count=small_file_count,
        parameters=parameters,
        small_file_threshold_bytes=small_file_threshold_bytes,
    )

    storage_cost = round(
        bytes_to_gb(total_size_bytes) * parameters.storage_cost_per_gb_month,
        6,
    )
    simulated_athena_scan_bytes = total_size_bytes
    simulated_athena_scan_cost = round(
        bytes_to_tb(simulated_athena_scan_bytes) * parameters.athena_cost_per_tb_scanned,
        6,
    )
    optimized_athena_scan_bytes = int(
        total_size_bytes * parameters.parquet_scan_ratio * parameters.partition_pruning_ratio
    )
    optimized_athena_scan_cost = round(
        bytes_to_tb(optimized_athena_scan_bytes) * parameters.athena_cost_per_tb_scanned,
        6,
    )
    estimated_savings_bytes = max(simulated_athena_scan_bytes - optimized_athena_scan_bytes, 0)
    estimated_savings_cost = round(
        max(simulated_athena_scan_cost - optimized_athena_scan_cost, 0.0),
        6,
    )

    notes = _build_layer_notes(
        layer=layer,
        file_count=file_count,
        has_small_files_problem=has_small_files_problem,
    )

    return LayerCostEstimate(
        layer=layer,
        path=str(layer_path),
        file_count=file_count,
        total_size_bytes=total_size_bytes,
        total_size_human=format_size_bytes(total_size_bytes),
        average_file_size_bytes=average_file_size_bytes,
        average_file_size_human=format_size_bytes(average_file_size_bytes),
        small_file_count=small_file_count,
        small_file_ratio=small_file_ratio,
        has_small_files_problem=has_small_files_problem,
        estimated_s3_storage_cost_usd=storage_cost,
        simulated_athena_scan_bytes=simulated_athena_scan_bytes,
        simulated_athena_scan_cost_usd=simulated_athena_scan_cost,
        optimized_athena_scan_bytes=optimized_athena_scan_bytes,
        optimized_athena_scan_cost_usd=optimized_athena_scan_cost,
        estimated_savings_bytes=estimated_savings_bytes,
        estimated_savings_cost_usd=estimated_savings_cost,
        notes=notes,
    )


def _collect_data_files(layer_path: Path) -> list[Path]:
    if not layer_path.exists():
        return []
    return sorted(
        file_path
        for file_path in layer_path.rglob("*")
        if file_path.is_file() and file_path.name != ".gitkeep"
    )


def _has_small_files_problem(
    file_count: int,
    average_file_size_bytes: int,
    small_file_count: int,
    parameters: FinOpsParameters,
    small_file_threshold_bytes: int,
) -> bool:
    if file_count < parameters.min_file_count_for_warning:
        return False
    if file_count == 0:
        return False
    ratio = small_file_count / file_count
    return (
        ratio >= parameters.small_file_ratio_threshold
        or average_file_size_bytes < small_file_threshold_bytes
    )


def _build_layer_notes(layer: str, file_count: int, has_small_files_problem: bool) -> str:
    if file_count == 0:
        return "Camada sem arquivos de dados no momento da análise."
    if layer == "raw":
        base_note = (
            "Camada bruta; a economia representa o potencial de migrar leitura "
            "para Parquet particionado."
        )
    else:
        base_note = (
            "Camada analítica/curada; a economia simula column pruning e "
            "partition pruning em consultas."
        )

    if has_small_files_problem:
        return (
            f"{base_note} Há indício de small files e possível custo extra de listagem e metadata."
        )
    return base_note


def _render_markdown(payload: dict[str, object]) -> str:
    summary = payload["summary"]
    parameters = payload["parameters"]
    layers = payload["layers"]

    lines = [
        "# FinOps Cost Estimation",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Total storage: `{summary['total_storage_human']}`",
        f"- Total files: `{summary['total_files']}`",
        (
            f"- Estimated S3 storage cost/month: "
            f"`USD {summary['total_estimated_s3_storage_cost_usd']}`"
        ),
        (
            f"- Simulated Athena full scan cost: "
            f"`USD {summary['total_simulated_athena_scan_cost_usd']}`"
        ),
        (
            f"- Simulated Athena optimized cost: "
            f"`USD {summary['total_optimized_athena_scan_cost_usd']}`"
        ),
        (
            f"- Estimated savings with Parquet and partitioning: "
            f"`USD {summary['total_estimated_savings_cost_usd']}`"
        ),
        "",
        "| Layer | Files | Total Volume | Avg File Size | Small Files | Small File Issue | "
        "S3 Cost/Month | Athena Full Scan | Athena Optimized | Estimated Savings |",
        "| --- | ---: | --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
    ]

    for layer in layers:
        lines.append(
            f"| {layer['layer']} | {layer['file_count']} | {layer['total_size_human']} | "
            f"{layer['average_file_size_human']} | {layer['small_file_count']} | "
            f"{layer['has_small_files_problem']} | USD {layer['estimated_s3_storage_cost_usd']} | "
            f"USD {layer['simulated_athena_scan_cost_usd']} | "
            f"USD {layer['optimized_athena_scan_cost_usd']} | "
            f"USD {layer['estimated_savings_cost_usd']} |"
        )

    lines.extend(
        [
            "",
            "## Assumptions",
            "",
            f"- Storage cost per GB-month: `USD {parameters['storage_cost_per_gb_month']}`",
            f"- Athena cost per TB scanned: `USD {parameters['athena_cost_per_tb_scanned']}`",
            f"- Parquet scan ratio: `{parameters['parquet_scan_ratio']}`",
            f"- Partition pruning ratio: `{parameters['partition_pruning_ratio']}`",
            f"- Small file threshold: `{parameters['small_file_threshold_mb']} MB`",
            "",
            "## Layer Notes",
            "",
        ]
    )

    for layer in layers:
        lines.append(f"- `{layer['layer']}`: {layer['notes']}")

    if not any(layer["file_count"] > 0 for layer in layers):
        lines.extend(
            [
                "",
                "## Observation",
                "",
                "Nenhuma camada continha arquivos de dados no momento da execução. "
                "Os custos estimados ficaram em zero até que o pipeline gere artefatos reais.",
            ]
        )

    return "\n".join(lines) + "\n"


def bytes_to_gb(size_bytes: int) -> float:
    return size_bytes / (1024**3)


def bytes_to_tb(size_bytes: int) -> float:
    return size_bytes / (1024**4)


def format_size_bytes(size_bytes: int) -> str:
    size = float(max(size_bytes, 0))
    units = ("B", "KB", "MB", "GB", "TB")

    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{int(size_bytes)} B"


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    parameters = FinOpsParameters(
        storage_cost_per_gb_month=args.storage_cost_per_gb_month,
        athena_cost_per_tb_scanned=args.athena_cost_per_tb_scanned,
        parquet_scan_ratio=args.parquet_scan_ratio,
        partition_pruning_ratio=args.partition_pruning_ratio,
        small_file_threshold_mb=args.small_file_threshold_mb,
        small_file_ratio_threshold=args.small_file_ratio_threshold,
        min_file_count_for_warning=args.min_file_count_for_warning,
    )
    report = run_cost_estimation(
        raw_dir=args.raw_dir,
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        report_path=args.report_path,
        json_path=args.json_path,
        parameters=parameters,
    )
    LOGGER.info(
        "Estimativa de FinOps concluída: storage=%s, savings=USD %s",
        report.to_record()["summary"]["total_storage_human"],
        report.to_record()["summary"]["total_estimated_savings_cost_usd"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
