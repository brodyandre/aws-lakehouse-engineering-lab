"""DAG principal para orquestrar o laboratório Lakehouse local-first."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_ROOT = Path("/opt/project")
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
REPORTS_DIR = PROJECT_ROOT / "reports"
PIPELINE_REPORTS_DIR = REPORTS_DIR / "pipeline_runs"
DATA_QUALITY_REPORTS_DIR = REPORTS_DIR / "data_quality"
FINOPS_REPORTS_DIR = REPORTS_DIR / "finops"
OBSERVABILITY_REPORTS_DIR = REPORTS_DIR / "observability"
FINAL_REPORT_PATH = REPORTS_DIR / "final_project_report.md"
BENCHMARK_REPORT_PATH = PIPELINE_REPORTS_DIR / "spark_optimization_benchmark.md"
BENCHMARK_SCRIPT_PATH = PROJECT_ROOT / "spark" / "benchmarks" / "spark_optimization_benchmark.py"
OBSERVABILITY_JSON_PATH = OBSERVABILITY_REPORTS_DIR / "pipeline_metrics.json"
OBSERVABILITY_MARKDOWN_PATH = OBSERVABILITY_REPORTS_DIR / "pipeline_metrics.md"
RAW_TO_BRONZE_REPORT_PATH = PIPELINE_REPORTS_DIR / "raw_to_bronze_report.md"
BRONZE_TO_SILVER_REPORT_PATH = PIPELINE_REPORTS_DIR / "bronze_to_silver_report.md"
SILVER_TO_GOLD_REPORT_PATH = PIPELINE_REPORTS_DIR / "silver_to_gold_report.md"
QUERY_REPORTS_DIR = REPORTS_DIR / "query"
SERVING_DATABASE_PATH = PROJECT_ROOT / "data" / "serving" / "lakehouse.duckdb"
SERVING_REPORT_PATH = QUERY_REPORTS_DIR / "serving_catalog.md"
SERVING_JSON_PATH = QUERY_REPORTS_DIR / "serving_catalog.json"

DEFAULT_ARGS = {
    "owner": "lakehouse-lab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _project_bash(command: str) -> str:
    return dedent(
        f"""
        set -euo pipefail
        mkdir -p \
          "{PIPELINE_REPORTS_DIR}" \
          "{DATA_QUALITY_REPORTS_DIR}" \
          "{FINOPS_REPORTS_DIR}" \
          "{OBSERVABILITY_REPORTS_DIR}" \
          "{QUERY_REPORTS_DIR}"
        cd "{PROJECT_ROOT}"
        {command}
        """
    ).strip()


with DAG(
    dag_id="lakehouse_pipeline_dag",
    description=(
        "Orquestra o pipeline Lakehouse local: ingestão, Spark, " "qualidade, FinOps e benchmark."
    ),
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "pyspark", "data-quality", "finops", "local-lab"],
    doc_md=dedent(
        """
        ## Lakehouse Pipeline DAG

        DAG local-first para executar o fluxo ponta a ponta do laboratório:

        1. geração de dados sintéticos
        2. raw para bronze
        3. bronze para silver
        4. silver para gold
        5. data quality
        6. FinOps simulado
        7. catálogo de serving para Trino
        8. benchmark Spark opcional
        9. relatório final consolidado

        O benchmark pode ser habilitado definindo `ENABLE_SPARK_OPTIMIZATION_BENCHMARK=true`
        no ambiente do container do Airflow.
        """
    ).strip(),
) as dag:
    generate_synthetic_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'src' / 'ingestion' / 'generate_synthetic_data.py'}" \
                  --output-dir "{RAW_DIR}"
                """
            ).strip()
        ),
    )

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'spark' / 'jobs' / 'raw_to_bronze.py'}" \
                  --raw-dir "{RAW_DIR}" \
                  --bronze-dir "{BRONZE_DIR}" \
                  --report-path "{RAW_TO_BRONZE_REPORT_PATH}" \
                  --observability-json-path "{OBSERVABILITY_JSON_PATH}" \
                  --observability-markdown-path "{OBSERVABILITY_MARKDOWN_PATH}" \
                  --master "${{SPARK_MASTER_URL:-local[*]}}" \
                  --remote "${{SPARK_REMOTE:-}}"
                """
            ).strip()
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'spark' / 'jobs' / 'bronze_to_silver.py'}" \
                  --bronze-dir "{BRONZE_DIR}" \
                  --silver-dir "{SILVER_DIR}" \
                  --report-path "{BRONZE_TO_SILVER_REPORT_PATH}" \
                  --observability-json-path "{OBSERVABILITY_JSON_PATH}" \
                  --observability-markdown-path "{OBSERVABILITY_MARKDOWN_PATH}" \
                  --master "${{SPARK_MASTER_URL:-local[*]}}" \
                  --remote "${{SPARK_REMOTE:-}}"
                """
            ).strip()
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'spark' / 'jobs' / 'silver_to_gold.py'}" \
                  --silver-dir "{SILVER_DIR}" \
                  --gold-dir "{GOLD_DIR}" \
                  --report-path "{SILVER_TO_GOLD_REPORT_PATH}" \
                  --observability-json-path "{OBSERVABILITY_JSON_PATH}" \
                  --observability-markdown-path "{OBSERVABILITY_MARKDOWN_PATH}" \
                  --master "${{SPARK_MASTER_URL:-local[*]}}" \
                  --remote "${{SPARK_REMOTE:-}}"
                """
            ).strip()
        ),
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'src' / 'quality' / 'data_quality_checks.py'}" \
                  --silver-dir "{SILVER_DIR}" \
                  --gold-dir "{GOLD_DIR}" \
                  --report-path "{DATA_QUALITY_REPORTS_DIR / 'data_quality_report.md'}" \
                  --json-path "{DATA_QUALITY_REPORTS_DIR / 'data_quality_results.json'}" \
                  --master "${{SPARK_MASTER_URL:-local[*]}}" \
                  --remote "${{SPARK_REMOTE:-}}"
                """
            ).strip()
        ),
    )

    cost_estimator = BashOperator(
        task_id="cost_estimator",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'src' / 'finops' / 'cost_estimator.py'}" \
                  --raw-dir "{RAW_DIR}" \
                  --bronze-dir "{BRONZE_DIR}" \
                  --silver-dir "{SILVER_DIR}" \
                  --gold-dir "{GOLD_DIR}" \
                  --report-path "{FINOPS_REPORTS_DIR / 'cost_estimation.md'}" \
                  --json-path "{FINOPS_REPORTS_DIR / 'cost_estimation.json'}"
                """
            ).strip()
        ),
    )

    prepare_query_serving = BashOperator(
        task_id="prepare_query_serving",
        bash_command=_project_bash(
            dedent(
                f"""
                python3 "{PROJECT_ROOT / 'scripts' / 'build_serving_catalog.py'}" \
                  --gold-dir "{GOLD_DIR}" \
                  --database-path "{SERVING_DATABASE_PATH}" \
                  --report-path "{SERVING_REPORT_PATH}" \
                  --json-path "{SERVING_JSON_PATH}"
                """
            ).strip()
        ),
    )

    spark_optimization_benchmark = BashOperator(
        task_id="spark_optimization_benchmark",
        bash_command=_project_bash(
            dedent(
                f"""
                if [ "${{ENABLE_SPARK_OPTIMIZATION_BENCHMARK:-false}}" = "true" ]; then
                  python3 "{BENCHMARK_SCRIPT_PATH}" \
                    --gold-dir "{GOLD_DIR}" \
                    --report-path "{BENCHMARK_REPORT_PATH}" \
                    --master "${{SPARK_MASTER_URL:-local[*]}}" \
                    --remote "${{SPARK_REMOTE:-}}"
                else
                  printf '%s\n' \
                    '# Spark Optimization Benchmark' \
                    '' \
                    '- Status: `skipped by configuration`' \
                    '- Enable with: `ENABLE_SPARK_OPTIMIZATION_BENCHMARK=true`' \
                    '- Reason: benchmark opcional para evitar custo de tempo ' \
                    'em execuções locais rotineiras.' \
                    > "{BENCHMARK_REPORT_PATH}"
                fi
                """
            ).strip()
        ),
    )

    generate_final_report = BashOperator(
        task_id="generate_final_report",
        bash_command=_project_bash(
            dedent(
                f"""
                bash "{PROJECT_ROOT / 'scripts' / 'generate_final_report.sh'}"
                test -f "{FINAL_REPORT_PATH}"
                """
            ).strip()
        ),
    )

    (generate_synthetic_data >> raw_to_bronze >> bronze_to_silver >> silver_to_gold)
    silver_to_gold >> [
        data_quality_checks,
        cost_estimator,
        prepare_query_serving,
        spark_optimization_benchmark,
    ]
    [
        data_quality_checks,
        cost_estimator,
        prepare_query_serving,
        spark_optimization_benchmark,
    ] >> generate_final_report
