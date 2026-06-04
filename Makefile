PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE ?= docker compose
PYTHON_SOURCES := $(shell find src spark airflow tests -type f -name '*.py' | sort)

.PHONY: help bootstrap setup-dev init up down restart logs ps check run-local clean-outputs final-report sync-readme-screenshots lint format test smoke clean

help:
	@printf "Targets disponíveis:\n"
	@printf "  make bootstrap  - cria .env a partir de .env.example se necessário\n"
	@printf "  make setup-dev  - cria .venv local e instala dependências do projeto\n"
	@printf "  make check      - valida pré-requisitos locais do laboratório\n"
	@printf "  make init       - prepara Postgres, MinIO, bucket e metadados iniciais do Airflow\n"
	@printf "  make up         - sobe o laboratório local\n"
	@printf "  make down       - derruba o laboratório local\n"
	@printf "  make restart    - reinicia os serviços\n"
	@printf "  make logs       - acompanha logs do docker compose\n"
	@printf "  make ps         - lista os serviços\n"
	@printf "  make run-local  - executa o pipeline ponta a ponta fora do Airflow\n"
	@printf "  make final-report - consolida o relatório final do projeto\n"
	@printf "  make sync-readme-screenshots - ativa screenshots no README quando os arquivos existirem\n"
	@printf "  make clean-outputs - limpa dados e relatórios gerados\n"
	@printf "  make lint       - executa checagens de estilo\n"
	@printf "  make format     - formata o código Python\n"
	@printf "  make test       - executa a suíte de testes\n"
	@printf "  make smoke      - valida a sintaxe principal do projeto\n"
	@printf "  make clean      - remove containers, volumes e caches locais\n"

bootstrap:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env
	@sed -i "s/^AIRFLOW_GID=.*/AIRFLOW_GID=0/" .env

setup-dev:
	bash scripts/setup_dev_env.sh

init: bootstrap
	$(COMPOSE) up -d postgres minio minio-init airflow-init

up: bootstrap
	$(COMPOSE) up -d

down:
	$(COMPOSE) down --remove-orphans

restart: down up

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

check: bootstrap
	bash scripts/check_environment.sh

run-local:
	bash scripts/run_pipeline_local.sh

final-report:
	bash scripts/generate_final_report.sh

sync-readme-screenshots:
	$(PYTHON) scripts/enable_readme_screenshots.py

clean-outputs:
	bash scripts/clean_outputs.sh

lint:
	RUFF_CACHE_DIR=/tmp/aws-lakehouse-engineering-lab-ruff-cache $(PYTHON) -m ruff check $(PYTHON_SOURCES)
	find src spark airflow tests -type f -name '*.py' -print0 | xargs -0 -n 1 $(PYTHON) -m black --workers 1 --check

format:
	find src spark airflow tests -type f -name '*.py' -print0 | xargs -0 -n 1 $(PYTHON) -m black --workers 1
	RUFF_CACHE_DIR=/tmp/aws-lakehouse-engineering-lab-ruff-cache $(PYTHON) -m ruff check $(PYTHON_SOURCES) --fix

test:
	$(PYTHON) -m unittest discover -s tests/unit -p 'test_*.py'

smoke:
	PYTHONPYCACHEPREFIX=/tmp/aws-lakehouse-engineering-lab-pycache $(PYTHON) -m compileall src airflow/dags tests

clean:
	$(COMPOSE) down --remove-orphans --volumes
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type d \( -name '.pytest_cache' -o -name '.ruff_cache' -o -name '.mypy_cache' \) -prune -exec rm -rf {} +
	rm -rf htmlcov .coverage .coverage.*
