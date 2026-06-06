PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE_ENV_FILE := $(if $(wildcard .env),.env,.env.example)
COMPOSE ?= docker compose --env-file $(COMPOSE_ENV_FILE)
COMPOSE_STORAGE := $(COMPOSE) -f compose/storage.yml
COMPOSE_SPARK := $(COMPOSE) -f compose/spark.yml
COMPOSE_AIRFLOW := $(COMPOSE) -f compose/airflow.yml
COMPOSE_QUERY := $(COMPOSE) -f compose/query.yml
COMPOSE_AISTOR := $(COMPOSE) -f docker-compose.yml -f compose/aistor.yml
PYTHON_SOURCES := $(shell find src spark airflow scripts tests -type f -name '*.py' | sort)

.PHONY: help bootstrap setup-dev init up up-storage up-spark up-airflow up-query up-aistor down restart logs ps check run-local trino-catalog verify-readme-screenshots clean-outputs final-report sync-readme-screenshots lint format test smoke clean

help:
	@printf "Targets disponíveis:\n"
	@printf "  make bootstrap  - cria .env a partir de .env.example se necessário\n"
	@printf "  make setup-dev  - cria .venv local e instala dependências do projeto\n"
	@printf "  make check      - valida pré-requisitos locais do laboratório\n"
	@printf "  make init       - prepara storage, Spark Connect e metadados iniciais do Airflow\n"
	@printf "  make up         - sobe a stack completa do laboratório\n"
	@printf "  make up-storage - sobe apenas a stack de object storage\n"
	@printf "  make up-spark   - sobe storage + cluster Spark + Spark Connect\n"
	@printf "  make up-airflow - sobe a stack de orquestração com Airflow 3\n"
	@printf "  make up-query   - sobe a camada de query com Trino (requer catálogo em data/serving)\n"
	@printf "  make up-aistor  - sobe a stack completa substituindo MinIO por AIStor\n"
	@printf "  make down       - derruba o laboratório local\n"
	@printf "  make restart    - reinicia os serviços\n"
	@printf "  make logs       - acompanha logs do docker compose\n"
	@printf "  make ps         - lista os serviços\n"
	@printf "  make run-local  - executa o pipeline ponta a ponta fora do Airflow\n"
	@printf "  make trino-catalog - materializa o catálogo DuckDB consumido pelo Trino\n"
	@printf "  make verify-readme-screenshots - valida automaticamente coerência das capturas do README\n"
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
	$(COMPOSE) up -d postgres minio minio-init spark-master spark-worker spark-connect airflow-init

up: bootstrap
	$(COMPOSE) up -d

up-storage: bootstrap
	$(COMPOSE_STORAGE) up -d

up-spark: bootstrap
	$(COMPOSE_SPARK) up -d

up-airflow: bootstrap
	$(COMPOSE_AIRFLOW) up -d

up-query: bootstrap
	$(COMPOSE_QUERY) up -d

up-aistor: bootstrap
	$(COMPOSE_AISTOR) up -d

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

trino-catalog:
	$(PYTHON) scripts/build_serving_catalog.py

verify-readme-screenshots:
	$(PYTHON) scripts/verify_readme_screenshots.py

final-report:
	bash scripts/generate_final_report.sh

sync-readme-screenshots:
	$(PYTHON) scripts/enable_readme_screenshots.py

clean-outputs:
	bash scripts/clean_outputs.sh

lint:
	RUFF_CACHE_DIR=/tmp/aws-lakehouse-engineering-lab-ruff-cache $(PYTHON) -m ruff check $(PYTHON_SOURCES)
	find src spark airflow scripts tests -type f -name '*.py' -print0 | xargs -0 -n 1 $(PYTHON) -m black --workers 1 --check

format:
	find src spark airflow scripts tests -type f -name '*.py' -print0 | xargs -0 -n 1 $(PYTHON) -m black --workers 1
	RUFF_CACHE_DIR=/tmp/aws-lakehouse-engineering-lab-ruff-cache $(PYTHON) -m ruff check $(PYTHON_SOURCES) --fix

test:
	$(PYTHON) -m unittest discover -s tests/unit -p 'test_*.py'

smoke:
	PYTHONPYCACHEPREFIX=/tmp/aws-lakehouse-engineering-lab-pycache $(PYTHON) -m compileall src airflow/dags scripts tests

clean:
	$(COMPOSE) down --remove-orphans --volumes
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type d \( -name '.pytest_cache' -o -name '.ruff_cache' -o -name '.mypy_cache' \) -prune -exec rm -rf {} +
	rm -rf htmlcov .coverage .coverage.*
