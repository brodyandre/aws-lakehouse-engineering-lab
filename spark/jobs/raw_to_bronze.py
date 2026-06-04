"""Primeiro job PySpark do projeto: carregamento da camada Raw para Bronze."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import Settings, local_spark_runtime_conf, prepare_local_spark_environment
from src.observability.metrics_collector import build_pipeline_execution_metric
from src.observability.pipeline_monitor import record_pipeline_metric
from src.utils.logger import configure_logging, get_logger

LOGGER = get_logger(__name__)

ENTITY_SPECS = {
    "customers": {
        "file_name": "customers.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    "products": {
        "file_name": "products.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    "campaigns": {
        "file_name": "campaigns.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    "orders": {
        "file_name": "orders.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    "order_items": {
        "file_name": "order_items.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    "web_events": {
        "file_name": "web_events.json",
        "format": "json",
        "options": {"multiline": "true"},
    },
}


@dataclass(frozen=True, slots=True)
class JobResult:
    entity_name: str
    source_path: Path
    target_path: Path
    records_read: int
    records_written: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Lê arquivos da camada raw e grava datasets Parquet na camada bronze."
    )
    parser.add_argument("--raw-dir", type=Path, default=settings.raw_data_path)
    parser.add_argument("--bronze-dir", type=Path, default=settings.bronze_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.pipeline_runs_report_path / "raw_to_bronze_report.md",
    )
    parser.add_argument(
        "--observability-json-path",
        type=Path,
        default=settings.observability_report_path / "pipeline_metrics.json",
    )
    parser.add_argument(
        "--observability-markdown-path",
        type=Path,
        default=settings.observability_report_path / "pipeline_metrics.md",
    )
    parser.add_argument("--app-name", default="raw-to-bronze-job")
    parser.add_argument("--master", default=None)
    return parser.parse_args(argv)


def build_spark_session(
    settings: Settings | None = None,
    app_name: str | None = None,
    master: str | None = None,
) -> SparkSession:
    active_settings = settings or Settings()
    resolved_app_name = app_name or active_settings.spark.app_name
    resolved_master = master or active_settings.spark.master

    prepare_local_spark_environment(resolved_master)
    builder = SparkSession.builder.appName(resolved_app_name).master(resolved_master)
    for key, value in active_settings.spark_conf.items():
        if key in {"spark.app.name", "spark.master"}:
            continue
        builder = builder.config(key, value)
    for key, value in local_spark_runtime_conf(resolved_master).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def run_raw_to_bronze(
    settings: Settings | None = None,
    raw_dir: Path | None = None,
    bronze_dir: Path | None = None,
    report_path: Path | None = None,
    observability_json_path: Path | None = None,
    observability_markdown_path: Path | None = None,
    app_name: str | None = None,
    master: str | None = None,
    spark: SparkSession | None = None,
) -> list[JobResult]:
    active_settings = settings or Settings()
    source_root = raw_dir or active_settings.raw_data_path
    target_root = bronze_dir or active_settings.bronze_data_path
    markdown_report_path = (
        report_path or active_settings.pipeline_runs_report_path / "raw_to_bronze_report.md"
    )
    observability_json_report_path = (
        observability_json_path
        or active_settings.observability_report_path / "pipeline_metrics.json"
    )
    observability_markdown_report_path = (
        observability_markdown_path
        or active_settings.observability_report_path / "pipeline_metrics.md"
    )

    source_root = source_root.resolve()
    target_root = target_root.resolve()
    markdown_report_path = markdown_report_path.resolve()
    observability_json_report_path = observability_json_report_path.resolve()
    observability_markdown_report_path = observability_markdown_report_path.resolve()
    target_root.mkdir(parents=True, exist_ok=True)
    markdown_report_path.parent.mkdir(parents=True, exist_ok=True)

    if not source_root.exists():
        raise FileNotFoundError(f"Diretório raw não encontrado: {source_root}")

    execution_started_at = datetime.now(timezone.utc)
    own_spark_session = spark is None
    spark_session = spark or build_spark_session(
        settings=active_settings,
        app_name=app_name or "raw-to-bronze-job",
        master=master,
    )
    results: list[JobResult] = []
    generated_paths: list[Path] = []

    try:
        for entity_name, spec in ENTITY_SPECS.items():
            source_path = source_root / spec["file_name"]
            if not source_path.exists():
                raise FileNotFoundError(f"Arquivo de origem não encontrado: {source_path}")

            target_path = target_root / entity_name
            LOGGER.info("Processando entidade '%s' a partir de %s", entity_name, source_path)

            dataframe = _read_entity(
                spark=spark_session,
                source_path=source_path,
                input_format=spec["format"],
                options=spec["options"],
            )
            dataframe = _add_technical_columns(dataframe, source_path, execution_started_at)

            records_read = dataframe.count()
            LOGGER.info("Entidade '%s': %s registros lidos", entity_name, records_read)

            dataframe.write.mode("overwrite").parquet(str(target_path))
            records_written = spark_session.read.parquet(str(target_path)).count()
            LOGGER.info(
                "Entidade '%s': %s registros gravados em %s",
                entity_name,
                records_written,
                target_path,
            )
            generated_paths.append(target_path)

            results.append(
                JobResult(
                    entity_name=entity_name,
                    source_path=source_path,
                    target_path=target_path,
                    records_read=records_read,
                    records_written=records_written,
                )
            )

        execution_finished_at = datetime.now(timezone.utc)
        _write_markdown_report(
            report_path=markdown_report_path,
            results=results,
            started_at=execution_started_at,
            finished_at=execution_finished_at,
        )
        generated_paths.append(markdown_report_path)
        _record_observability(
            settings=active_settings,
            results=results,
            started_at=execution_started_at,
            finished_at=execution_finished_at,
            generated_paths=generated_paths,
            json_path=observability_json_report_path,
            markdown_path=observability_markdown_report_path,
        )
        LOGGER.info("Relatório gerado em %s", markdown_report_path)
        return results
    except Exception as exc:
        execution_finished_at = datetime.now(timezone.utc)
        _record_observability(
            settings=active_settings,
            results=results,
            started_at=execution_started_at,
            finished_at=execution_finished_at,
            generated_paths=generated_paths,
            json_path=observability_json_report_path,
            markdown_path=observability_markdown_report_path,
            status="failed",
            error_message=str(exc),
        )
        raise
    finally:
        if own_spark_session:
            spark_session.stop()


def _read_entity(
    spark: SparkSession,
    source_path: Path,
    input_format: str,
    options: dict[str, str],
) -> DataFrame:
    reader = spark.read.format(input_format)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(str(source_path))


def _add_technical_columns(
    dataframe: DataFrame,
    source_path: Path,
    execution_started_at: datetime,
) -> DataFrame:
    processing_date = execution_started_at.date().isoformat()
    return (
        dataframe.withColumn(
            "ingestion_timestamp",
            F.to_timestamp(F.lit(execution_started_at.isoformat())),
        )
        .withColumn("source_file", F.lit(source_path.name))
        .withColumn("processing_date", F.to_date(F.lit(processing_date)))
    )


def _write_markdown_report(
    report_path: Path,
    results: list[JobResult],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    duration_seconds = round((finished_at - started_at).total_seconds(), 2)
    total_read = sum(result.records_read for result in results)
    total_written = sum(result.records_written for result in results)

    lines = [
        "# Raw to Bronze Report",
        "",
        f"- Started at: `{started_at.isoformat()}`",
        f"- Finished at: `{finished_at.isoformat()}`",
        f"- Duration seconds: `{duration_seconds}`",
        f"- Total entities: `{len(results)}`",
        f"- Total records read: `{total_read}`",
        f"- Total records written: `{total_written}`",
        "",
        "| Entity | Source | Target | Records Read | Records Written |",
        "| --- | --- | --- | ---: | ---: |",
    ]

    for result in results:
        lines.append(
            f"| {result.entity_name} | `{result.source_path.name}` | `{result.target_path}` | "
            f"{result.records_read} | {result.records_written} |"
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _record_observability(
    settings: Settings,
    results: list[JobResult],
    started_at: datetime,
    finished_at: datetime,
    generated_paths: list[Path],
    json_path: Path,
    markdown_path: Path,
    status: str | None = None,
    error_message: str | None = None,
) -> None:
    total_records_in = sum(result.records_read for result in results)
    total_records_out = sum(result.records_written for result in results)
    entity_metrics = [
        {
            "entity": result.entity_name,
            "records_in": result.records_read,
            "records_out": result.records_written,
            "invalid_records": 0,
            "notes": f"Origem `{result.source_path.name}` para `{result.target_path.name}`.",
        }
        for result in results
    ]

    metric = build_pipeline_execution_metric(
        job_name="raw_to_bronze",
        started_at=started_at,
        finished_at=finished_at,
        source_layer="raw",
        target_layer="bronze",
        records_in=total_records_in,
        records_out=total_records_out,
        invalid_records=0,
        generated_paths=generated_paths,
        status=status,
        entity_metrics=entity_metrics,
        error_message=error_message,
    )

    try:
        record_pipeline_metric(
            metric,
            settings=settings,
            json_path=json_path,
            markdown_path=markdown_path,
        )
    except Exception:
        LOGGER.exception("Falha ao registrar observabilidade do job raw_to_bronze.")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    started_at = time.perf_counter()
    run_raw_to_bronze(
        raw_dir=args.raw_dir,
        bronze_dir=args.bronze_dir,
        report_path=args.report_path,
        observability_json_path=args.observability_json_path,
        observability_markdown_path=args.observability_markdown_path,
        app_name=args.app_name,
        master=args.master,
    )
    elapsed_seconds = round(time.perf_counter() - started_at, 2)
    LOGGER.info("Job raw_to_bronze finalizado em %s segundos", elapsed_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
