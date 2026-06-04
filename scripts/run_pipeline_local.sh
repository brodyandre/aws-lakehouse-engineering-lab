#!/usr/bin/env bash
set -euo pipefail

IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
SPARK_MASTER_VALUE="${PIPELINE_SPARK_MASTER:-${SPARK_MASTER:-local[*]}}"

CUSTOMERS_COUNT="${PIPELINE_CUSTOMERS:-200}"
PRODUCTS_COUNT="${PIPELINE_PRODUCTS:-60}"
CAMPAIGNS_COUNT="${PIPELINE_CAMPAIGNS:-12}"
ORDERS_COUNT="${PIPELINE_ORDERS:-400}"
ORDER_ITEMS_COUNT="${PIPELINE_ORDER_ITEMS:-1000}"
WEB_EVENTS_COUNT="${PIPELINE_WEB_EVENTS:-1500}"

OBSERVABILITY_JSON="reports/observability/pipeline_metrics.json"
OBSERVABILITY_MARKDOWN="reports/observability/pipeline_metrics.md"
RAW_TO_BRONZE_REPORT="reports/pipeline_runs/raw_to_bronze_report.md"
BRONZE_TO_SILVER_REPORT="reports/pipeline_runs/bronze_to_silver_report.md"
SILVER_TO_GOLD_REPORT="reports/pipeline_runs/silver_to_gold_report.md"
DATA_QUALITY_REPORT="reports/data_quality/data_quality_report.md"
DATA_QUALITY_JSON="reports/data_quality/data_quality_results.json"
FINOPS_REPORT="reports/finops/cost_estimation.md"
FINOPS_JSON="reports/finops/cost_estimation.json"
FINAL_REPORT="reports/final_project_report.md"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(timestamp)" "$1"
}

sync_airflow_user_ids() {
  local host_uid

  host_uid="$(id -u)"

  if grep -q '^AIRFLOW_UID=' .env; then
    sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=${host_uid}/" .env
  else
    printf '\nAIRFLOW_UID=%s\n' "${host_uid}" >> .env
  fi

  if grep -q '^AIRFLOW_GID=' .env; then
    sed -i "s/^AIRFLOW_GID=.*/AIRFLOW_GID=0/" .env
  else
    printf 'AIRFLOW_GID=0\n' >> .env
  fi
}

assert_path_exists() {
  local path="$1"
  if [[ ! -e "${path}" ]]; then
    printf '[%s] [ERROR] Artefato esperado não encontrado: %s\n' "$(timestamp)" "${path}" >&2
    exit 1
  fi
}

run_step() {
  local label="$1"
  shift
  log_info "Iniciando etapa: ${label}"
  "$@"
  log_info "Etapa concluída: ${label}"
}

run_observability_step() {
  log_info "Validando métricas de observabilidade geradas pelos jobs Spark."
  assert_path_exists "${OBSERVABILITY_JSON}"
  assert_path_exists "${OBSERVABILITY_MARKDOWN}"

  PROJECT_ROOT="${PROJECT_ROOT}" OBSERVABILITY_JSON="${OBSERVABILITY_JSON}" "${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

project_root = Path(os.environ["PROJECT_ROOT"]).resolve()
observability_path = (project_root / os.environ["OBSERVABILITY_JSON"]).resolve()
payload = json.loads(observability_path.read_text(encoding="utf-8"))
summary = payload.get("summary", {})

print(
    "Observabilidade local atualizada: "
    f"execucoes={summary.get('total_executions', 0)}, "
    f"success={summary.get('success_executions', 0)}, "
    f"warning={summary.get('warning_executions', 0)}, "
    f"failed={summary.get('failed_executions', 0)}"
)
PY
}

main() {
  log_info "Executando o pipeline local ponta a ponta do laboratório."

  mkdir -p \
    data/raw \
    data/bronze \
    data/silver \
    data/gold \
    reports/pipeline_runs \
    reports/data_quality \
    reports/observability \
    reports/finops
  chmod -R a+rwX data reports

  if [[ ! -f ".env" && -f ".env.example" ]]; then
    cp .env.example .env
    log_info "Arquivo .env criado automaticamente a partir de .env.example."
  fi

  if [[ -f ".env" ]]; then
    sync_airflow_user_ids
  fi

  run_step "checagem do ambiente" bash scripts/check_environment.sh

  run_step \
    "geração de dados sintéticos" \
    "${PYTHON_BIN}" src/ingestion/generate_synthetic_data.py \
      --output-dir data/raw \
      --customers "${CUSTOMERS_COUNT}" \
      --products "${PRODUCTS_COUNT}" \
      --campaigns "${CAMPAIGNS_COUNT}" \
      --orders "${ORDERS_COUNT}" \
      --order-items "${ORDER_ITEMS_COUNT}" \
      --web-events "${WEB_EVENTS_COUNT}"

  run_step \
    "raw para bronze" \
    "${PYTHON_BIN}" spark/jobs/raw_to_bronze.py \
      --raw-dir data/raw \
      --bronze-dir data/bronze \
      --report-path "${RAW_TO_BRONZE_REPORT}" \
      --observability-json-path "${OBSERVABILITY_JSON}" \
      --observability-markdown-path "${OBSERVABILITY_MARKDOWN}" \
      --master "${SPARK_MASTER_VALUE}"

  run_step \
    "bronze para silver" \
    "${PYTHON_BIN}" spark/jobs/bronze_to_silver.py \
      --bronze-dir data/bronze \
      --silver-dir data/silver \
      --report-path "${BRONZE_TO_SILVER_REPORT}" \
      --observability-json-path "${OBSERVABILITY_JSON}" \
      --observability-markdown-path "${OBSERVABILITY_MARKDOWN}" \
      --master "${SPARK_MASTER_VALUE}"

  run_step \
    "silver para gold" \
    "${PYTHON_BIN}" spark/jobs/silver_to_gold.py \
      --silver-dir data/silver \
      --gold-dir data/gold \
      --report-path "${SILVER_TO_GOLD_REPORT}" \
      --observability-json-path "${OBSERVABILITY_JSON}" \
      --observability-markdown-path "${OBSERVABILITY_MARKDOWN}" \
      --master "${SPARK_MASTER_VALUE}"

  run_step \
    "data quality" \
    "${PYTHON_BIN}" src/quality/data_quality_checks.py \
      --silver-dir data/silver \
      --gold-dir data/gold \
      --report-path "${DATA_QUALITY_REPORT}" \
      --json-path "${DATA_QUALITY_JSON}" \
      --master "${SPARK_MASTER_VALUE}"

  run_step "observabilidade" run_observability_step

  run_step \
    "finops simulado" \
    "${PYTHON_BIN}" src/finops/cost_estimator.py \
      --raw-dir data/raw \
      --bronze-dir data/bronze \
      --silver-dir data/silver \
      --gold-dir data/gold \
      --report-path "${FINOPS_REPORT}" \
      --json-path "${FINOPS_JSON}"

  run_step "relatório final" bash scripts/generate_final_report.sh

  assert_path_exists "${RAW_TO_BRONZE_REPORT}"
  assert_path_exists "${BRONZE_TO_SILVER_REPORT}"
  assert_path_exists "${SILVER_TO_GOLD_REPORT}"
  assert_path_exists "${DATA_QUALITY_REPORT}"
  assert_path_exists "${FINOPS_REPORT}"
  assert_path_exists "${FINAL_REPORT}"

  log_info "Pipeline concluído. Evidência final disponível em ${FINAL_REPORT}."
}

main "$@"
