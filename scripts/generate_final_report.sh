#!/usr/bin/env bash
set -euo pipefail

IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
REPORT_PATH="${REPORT_PATH:-reports/final_project_report.md}"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(timestamp)" "$1"
}

log_info "Consolidando relatório final do projeto em ${REPORT_PATH}."

PROJECT_ROOT="${PROJECT_ROOT}" REPORT_PATH="${REPORT_PATH}" "${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


project_root = Path(os.environ["PROJECT_ROOT"]).resolve()
report_path = (project_root / os.environ["REPORT_PATH"]).resolve()
report_path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def format_size(size_bytes: int) -> str:
    size = float(max(size_bytes, 0))
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{int(size_bytes)} B"


def collect_layer_stats(layer_name: str) -> dict[str, object]:
    layer_path = project_root / "data" / layer_name
    files = [
        path
        for path in layer_path.rglob("*")
        if path.is_file() and path.name != ".gitkeep"
    ]
    total_size = sum(path.stat().st_size for path in files)
    return {
        "layer": layer_name,
        "path": str(layer_path.relative_to(project_root)),
        "file_count": len(files),
        "total_size_bytes": total_size,
        "total_size_human": format_size(total_size),
    }


observability_payload = load_json(
    project_root / "reports" / "observability" / "pipeline_metrics.json"
)
data_quality_payload = load_json(
    project_root / "reports" / "data_quality" / "data_quality_results.json"
)
finops_payload = load_json(project_root / "reports" / "finops" / "cost_estimation.json")

layer_stats = [collect_layer_stats(layer) for layer in ("raw", "bronze", "silver", "gold")]
pipeline_reports = sorted(
    path.name
    for path in (project_root / "reports" / "pipeline_runs").glob("*.md")
    if path.name != ".gitkeep"
)

observability_summary = (observability_payload or {}).get("summary", {})
pipeline_runs = (observability_payload or {}).get("pipelines", [])
data_quality_summary = (data_quality_payload or {}).get("summary", {})
data_quality_checks = (data_quality_payload or {}).get("checks", [])
finops_summary = (finops_payload or {}).get("summary", {})
finops_layers = (finops_payload or {}).get("layers", [])

total_executions = int(observability_summary.get("total_executions", 0) or 0)
total_checks = int(data_quality_summary.get("total_checks", 0) or 0)
failed_executions = int(observability_summary.get("failed_executions", 0) or 0)
warning_executions = int(observability_summary.get("warning_executions", 0) or 0)
failed_checks = int(data_quality_summary.get("failed_checks", 0) or 0)

if total_executions == 0 and total_checks == 0:
    overall_status = "not_started"
elif failed_executions > 0:
    overall_status = "failed"
elif failed_checks > 0 or warning_executions > 0:
    overall_status = "warning"
else:
    overall_status = "success"

started_candidates = [
    str(record.get("started_at"))
    for record in pipeline_runs
    if isinstance(record, dict) and record.get("started_at")
]
finished_candidates = [
    str(record.get("finished_at"))
    for record in pipeline_runs
    if isinstance(record, dict) and record.get("finished_at")
]

execution_window = {
    "started_at": min(started_candidates) if started_candidates else "n/a",
    "finished_at": max(finished_candidates) if finished_candidates else "n/a",
}

pipeline_rows = []
for record in pipeline_runs[:5]:
    if not isinstance(record, dict):
        continue
    pipeline_rows.append(
        {
            "job_name": record.get("job_name", "n/a"),
            "status": record.get("status", "n/a"),
            "duration_seconds": record.get("duration_seconds", "n/a"),
            "records_in": record.get("records_in", "n/a"),
            "records_out": record.get("records_out", "n/a"),
            "invalid_records": record.get("invalid_records", "n/a"),
        }
    )

failed_quality_rules = [
    check
    for check in data_quality_checks
    if isinstance(check, dict) and not check.get("passed", False)
]

small_file_layers = finops_summary.get("layers_with_small_files_problem", [])
if not isinstance(small_file_layers, list):
    small_file_layers = []

learning_points = [
    "A separação em camadas Raw, Bronze, Silver e Gold aumenta rastreabilidade e clareza operacional.",
    "Observabilidade local em JSON e Markdown gera evidências técnicas sem depender de serviços gerenciados.",
]

if total_checks == 0:
    learning_points.append(
        "Nenhuma execução completa foi consolidada ainda; rode o pipeline local para materializar evidências de qualidade, custo e observabilidade."
    )
elif failed_quality_rules:
    learning_points.append(
        "As regras de Data Quality encontraram desvios controlados, o que reforça a utilidade de validações automatizadas antes do consumo analítico."
    )
else:
    learning_points.append(
        "As validações automatizadas de qualidade passaram sem falhas, sinalizando consistência mínima para uso analítico local."
    )

if small_file_layers:
    learning_points.append(
        "A simulação de FinOps indica atenção para small files, sugerindo compactação e particionamento mais seletivo."
    )

if float(finops_summary.get("total_estimated_savings_cost_usd", 0) or 0) > 0:
    learning_points.append(
        "Parquet e particionamento demonstram economia potencial de leitura em comparação com scans completos de arquivos brutos."
    )

lines = [
    "# Final Project Report",
    "",
    "## Resumo da Execução",
    "",
    f"- Status consolidado: `{overall_status}`",
    f"- Relatório gerado em: `{datetime.now(timezone.utc).isoformat()}`",
    f"- Janela de execução observada: `{execution_window['started_at']}` até `{execution_window['finished_at']}`",
    f"- Jobs monitorados: `{observability_summary.get('total_executions', 0)}`",
    f"- Último job registrado: `{observability_summary.get('last_job_name', 'n/a')}`",
    "",
    "## Camadas Geradas",
    "",
    "| Camada | Caminho | Arquivos | Tamanho Aproximado |",
    "| --- | --- | ---: | --- |",
]

for layer in layer_stats:
    lines.append(
        f"| {layer['layer']} | `{layer['path']}` | {layer['file_count']} | {layer['total_size_human']} |"
    )

lines.extend(
    [
        "",
        "## Métricas Operacionais",
        "",
        f"- Execuções com sucesso: `{observability_summary.get('success_executions', 0)}`",
        f"- Execuções com warning: `{observability_summary.get('warning_executions', 0)}`",
        f"- Execuções com falha: `{observability_summary.get('failed_executions', 0)}`",
        "",
        "| Job | Status | Duração (s) | Registros de Entrada | Registros de Saída | Inválidos |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
)

if pipeline_rows:
    for row in pipeline_rows:
        lines.append(
            f"| {row['job_name']} | {row['status']} | {row['duration_seconds']} | "
            f"{row['records_in']} | {row['records_out']} | {row['invalid_records']} |"
        )
else:
    lines.append("| n/a | n/a | n/a | n/a | n/a | n/a |")

lines.extend(
    [
        "",
        "## Qualidade de Dados",
        "",
        f"- Total de checks: `{data_quality_summary.get('total_checks', 0)}`",
        f"- Checks aprovados: `{data_quality_summary.get('passed_checks', 0)}`",
        f"- Checks com falha: `{data_quality_summary.get('failed_checks', 0)}`",
    ]
)

if failed_quality_rules:
    lines.extend(
        [
            "",
            "| Layer | Entidade | Regra | Registros com Falha |",
            "| --- | --- | --- | ---: |",
        ]
    )
    for check in failed_quality_rules[:10]:
        lines.append(
            f"| {check.get('layer', 'n/a')} | {check.get('entity', 'n/a')} | "
            f"{check.get('rule_name', 'n/a')} | {check.get('failed_records', 'n/a')} |"
        )

lines.extend(
    [
        "",
        "## FinOps Simulado",
        "",
        f"- Total de arquivos analisados: `{finops_summary.get('total_files', 0)}`",
        f"- Volume total observado: `{finops_summary.get('total_storage_human', 'n/a')}`",
        f"- Storage simulado estilo S3: `${finops_summary.get('total_estimated_s3_storage_cost_usd', 0)}` por mês",
        f"- Scan simulado estilo Athena: `${finops_summary.get('total_simulated_athena_scan_cost_usd', 0)}`",
        f"- Scan otimizado estimado: `${finops_summary.get('total_optimized_athena_scan_cost_usd', 0)}`",
        f"- Economia estimada: `${finops_summary.get('total_estimated_savings_cost_usd', 0)}`",
    ]
)

if finops_layers:
    lines.extend(
        [
            "",
            "| Camada | Arquivos | Tamanho Médio | Small Files |",
            "| --- | ---: | --- | --- |",
        ]
    )
    for layer in finops_layers:
        if not isinstance(layer, dict):
            continue
        lines.append(
            f"| {layer.get('layer', 'n/a')} | {layer.get('file_count', 0)} | "
            f"{layer.get('average_file_size_human', 'n/a')} | {layer.get('has_small_files_problem', False)} |"
        )

lines.extend(
    [
        "",
        "## Evidências Geradas",
        "",
        f"- Relatórios de pipeline: `{', '.join(pipeline_reports) if pipeline_reports else 'nenhum relatório encontrado'}`",
        f"- Observabilidade: `reports/observability/pipeline_metrics.md` e `reports/observability/pipeline_metrics.json`",
        f"- Data Quality: `reports/data_quality/data_quality_report.md` e `reports/data_quality/data_quality_results.json`",
        f"- FinOps: `reports/finops/cost_estimation.md` e `reports/finops/cost_estimation.json`",
        "",
        "## Principais Aprendizados",
        "",
    ]
)

for learning_point in learning_points:
    lines.append(f"- {learning_point}")

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(report_path)
PY

log_info "Relatório final criado com sucesso."
