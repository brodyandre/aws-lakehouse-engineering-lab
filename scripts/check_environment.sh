#!/usr/bin/env bash
set -euo pipefail

IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

DEFAULT_PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
  DEFAULT_PYTHON_BIN=".venv/bin/python"
fi
PYTHON_BIN="${PYTHON_BIN:-${DEFAULT_PYTHON_BIN}}"
REQUIRED_DIRS=(
  "data/raw"
  "data/bronze"
  "data/silver"
  "data/gold"
  "reports/pipeline_runs"
  "reports/data_quality"
  "reports/observability"
  "reports/finops"
)

ERRORS=0
WARNINGS=0

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(timestamp)" "$1"
}

log_warn() {
  WARNINGS=$((WARNINGS + 1))
  printf '[%s] [WARN] %s\n' "$(timestamp)" "$1"
}

log_error() {
  ERRORS=$((ERRORS + 1))
  printf '[%s] [ERROR] %s\n' "$(timestamp)" "$1"
}

log_success() {
  printf '[%s] [OK] %s\n' "$(timestamp)" "$1"
}

check_python_version() {
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    log_error "Python não encontrado com o binário '${PYTHON_BIN}'."
    return
  fi

  if "${PYTHON_BIN}" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
  then
    log_success "$("${PYTHON_BIN}" --version 2>&1) é compatível com o projeto."
  else
    log_error "O projeto requer Python 3.10 ou superior."
  fi
}

check_python_dependencies() {
  local missing

  if ! missing="$("${PYTHON_BIN}" - <<'PY'
import importlib

required_modules = {
    "faker": "Faker",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "pyspark": "pyspark",
    "yaml": "PyYAML",
    "dotenv": "python-dotenv",
}

missing = []
for module_name, package_name in required_modules.items():
    try:
        importlib.import_module(module_name)
    except Exception:
        missing.append(package_name)

print(",".join(missing))
PY
)"; then
    log_error "Não foi possível verificar as dependências Python."
    return
  fi

  if [[ -n "${missing}" ]]; then
    log_error "Dependências Python ausentes: ${missing}. Execute 'make setup-dev' ou 'pip install -r requirements.txt'."
  else
    log_success "Dependências Python essenciais disponíveis."
  fi
}

check_java_runtime() {
  if command -v java >/dev/null 2>&1; then
    log_success "Java disponível para execução local do PySpark."
  else
    log_warn "Java não encontrado. Os jobs PySpark locais podem falhar sem um runtime Java."
  fi
}

check_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    log_success "docker compose disponível."
  else
    log_error "docker compose não está disponível."
  fi
}

ensure_required_directories() {
  local directory

  for directory in "${REQUIRED_DIRS[@]}"; do
    if [[ -d "${directory}" ]]; then
      log_success "Diretório verificado: ${directory}"
    else
      mkdir -p "${directory}"
      log_warn "Diretório ausente e criado automaticamente: ${directory}"
    fi

    chmod a+rwX "${directory}"
  done
}

check_env_file() {
  if [[ -f ".env" ]]; then
    sync_airflow_user_ids
    log_success "Arquivo .env encontrado."
    return
  fi

  if [[ -f ".env.example" ]]; then
    cp .env.example .env
    sync_airflow_user_ids
    log_warn "Arquivo .env ausente. Um .env local foi criado automaticamente a partir de .env.example."
  else
    log_error "Arquivo .env não encontrado e .env.example não está disponível."
  fi
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

print_summary() {
  printf '\n'
  printf '[%s] [INFO] Verificação concluída: %s erro(s), %s aviso(s).\n' \
    "$(timestamp)" "${ERRORS}" "${WARNINGS}"
}

main() {
  log_info "Validando ambiente local do AWS Lakehouse Engineering Lab."

  if command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    log_success "Python encontrado em $(command -v "${PYTHON_BIN}")"
    check_python_version
    check_python_dependencies
  else
    log_error "Python não encontrado com o binário '${PYTHON_BIN}'."
  fi

  if command -v docker >/dev/null 2>&1; then
    log_success "Docker encontrado em $(command -v docker)"
    check_docker_compose
  else
    log_error "Docker não encontrado."
  fi

  check_java_runtime
  ensure_required_directories
  check_env_file

  print_summary

  if (( ERRORS > 0 )); then
    exit 1
  fi
}

main "$@"
