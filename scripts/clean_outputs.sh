#!/usr/bin/env bash
set -euo pipefail

IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(timestamp)" "$1"
}

clean_directory_contents() {
  local directory="$1"

  if [[ ! -d "${directory}" ]]; then
    log_info "Diretório não encontrado, nada para limpar: ${directory}"
    return
  fi

  find "${directory}" -mindepth 1 \
    ! -name '.gitkeep' \
    ! -name 'README.md' \
    -exec rm -rf {} +

  log_info "Saídas removidas de ${directory}"
}

main() {
  log_info "Limpando camadas geradas e relatórios do laboratório."

  clean_directory_contents "data/raw"
  clean_directory_contents "data/bronze"
  clean_directory_contents "data/silver"
  clean_directory_contents "data/gold"
  clean_directory_contents "data/serving"
  clean_directory_contents "reports/pipeline_runs"
  clean_directory_contents "reports/data_quality"
  clean_directory_contents "reports/observability"
  clean_directory_contents "reports/finops"
  clean_directory_contents "reports/query"

  if [[ -f "reports/final_project_report.md" ]]; then
    rm -f "reports/final_project_report.md"
    log_info "Arquivo removido: reports/final_project_report.md"
  fi

  log_info "Limpeza concluída sem remover documentação nem código-fonte."
}

main "$@"
