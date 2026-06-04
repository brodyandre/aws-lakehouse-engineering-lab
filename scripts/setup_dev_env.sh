#!/usr/bin/env bash
set -euo pipefail

IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

PYTHON_BIN="${PYTHON_DEV_BIN:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(timestamp)" "$1"
}

log_error() {
  printf '[%s] [ERROR] %s\n' "$(timestamp)" "$1" >&2
}

log_warning() {
  printf '[%s] [WARN] %s\n' "$(timestamp)" "$1" >&2
}

ensure_python() {
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    log_error "Python não encontrado com o binário '${PYTHON_BIN}'."
    exit 1
  fi

  if ! "${PYTHON_BIN}" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
  then
    log_error "O setup local requer Python 3.10 ou superior."
    exit 1
  fi
}

main() {
  ensure_python

  log_info "Criando ambiente virtual em ${VENV_DIR}."
  if ! "${PYTHON_BIN}" -m venv "${VENV_DIR}"; then
    log_warning "Falha ao criar venv com o módulo padrão. Tentando fallback com virtualenv."
    if ! "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
      log_error "O Python local não possui pip disponível. Instale os pacotes do sistema para venv/pip ou use o fluxo 100% Docker com 'make up'."
      exit 1
    fi

    "${PYTHON_BIN}" -m pip install --user virtualenv
    "${PYTHON_BIN}" -m virtualenv "${VENV_DIR}"
  fi

  log_info "Atualizando pip e instalando dependências do projeto."
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r requirements.txt

  log_info "Ambiente pronto. Ative com: source ${VENV_DIR}/bin/activate"
}

main "$@"
