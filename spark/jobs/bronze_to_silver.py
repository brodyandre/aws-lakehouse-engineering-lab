"""Job PySpark para promover dados da camada Bronze para a camada Silver."""

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

from src.config.settings import Settings
from src.observability.metrics_collector import build_pipeline_execution_metric
from src.observability.pipeline_monitor import record_pipeline_metric
from src.utils.logger import configure_logging, get_logger
from src.utils.spark import create_spark_session

LOGGER = get_logger(__name__)
VALID_ORDER_STATUSES = ("created", "paid", "shipped", "cancelled", "refunded")
ENTITY_NAMES = ("customers", "products", "campaigns", "orders", "order_items", "web_events")


@dataclass(frozen=True, slots=True)
class SilverJobResult:
    entity_name: str
    source_path: Path
    target_path: Path
    records_in: int
    records_out: int
    invalid_records: int
    quality_percentage: float
    partition_columns: tuple[str, ...]
    notes: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Lê dados da camada bronze e grava datasets tratados na camada silver."
    )
    parser.add_argument("--bronze-dir", type=Path, default=settings.bronze_data_path)
    parser.add_argument("--silver-dir", type=Path, default=settings.silver_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.pipeline_runs_report_path / "bronze_to_silver_report.md",
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
    parser.add_argument("--app-name", default="bronze-to-silver-job")
    parser.add_argument("--master", default=None)
    parser.add_argument("--remote", default=settings.spark.remote)
    return parser.parse_args(argv)


def build_spark_session(
    settings: Settings | None = None,
    app_name: str | None = None,
    master: str | None = None,
    remote: str | None = None,
) -> SparkSession:
    return create_spark_session(
        settings=settings,
        app_name=app_name,
        master=master,
        remote=remote,
    )


def run_bronze_to_silver(
    settings: Settings | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
    report_path: Path | None = None,
    observability_json_path: Path | None = None,
    observability_markdown_path: Path | None = None,
    app_name: str | None = None,
    master: str | None = None,
    remote: str | None = None,
    spark: SparkSession | None = None,
) -> list[SilverJobResult]:
    active_settings = settings or Settings()
    source_root = (bronze_dir or active_settings.bronze_data_path).resolve()
    target_root = (silver_dir or active_settings.silver_data_path).resolve()
    markdown_report_path = (
        report_path or active_settings.pipeline_runs_report_path / "bronze_to_silver_report.md"
    ).resolve()
    observability_json_report_path = (
        observability_json_path
        or active_settings.observability_report_path / "pipeline_metrics.json"
    ).resolve()
    observability_markdown_report_path = (
        observability_markdown_path
        or active_settings.observability_report_path / "pipeline_metrics.md"
    ).resolve()

    if not source_root.exists():
        raise FileNotFoundError(f"Diretório bronze não encontrado: {source_root}")

    target_root.mkdir(parents=True, exist_ok=True)
    markdown_report_path.parent.mkdir(parents=True, exist_ok=True)

    own_spark_session = spark is None
    spark_session = spark or build_spark_session(
        settings=active_settings,
        app_name=app_name or "bronze-to-silver-job",
        master=master,
        remote=remote,
    )

    started_at = datetime.now(timezone.utc)
    results: list[SilverJobResult] = []
    generated_paths: list[Path] = []

    try:
        for entity_name in ENTITY_NAMES:
            source_path = source_root / entity_name
            target_path = target_root / entity_name

            if not source_path.exists():
                raise FileNotFoundError(f"Dataset bronze não encontrado: {source_path}")

            LOGGER.info("Processando entidade '%s' a partir de %s", entity_name, source_path)
            bronze_df = spark_session.read.parquet(str(source_path))
            records_in = bronze_df.count()

            silver_df, invalid_records, partition_columns, notes = _transform_entity(
                entity_name,
                bronze_df,
            )
            silver_df = silver_df.cache()
            records_out = silver_df.count()

            LOGGER.info(
                "Entidade '%s': entrada=%s, saída=%s, inválidos=%s",
                entity_name,
                records_in,
                records_out,
                invalid_records,
            )

            _write_entity(silver_df, target_path, partition_columns)
            records_written = spark_session.read.parquet(str(target_path)).count()
            silver_df.unpersist()
            generated_paths.append(target_path)

            quality_percentage = _calculate_quality_percentage(records_in, invalid_records)
            LOGGER.info(
                "Entidade '%s': gravados=%s, qualidade aproximada=%s%%",
                entity_name,
                records_written,
                quality_percentage,
            )

            results.append(
                SilverJobResult(
                    entity_name=entity_name,
                    source_path=source_path,
                    target_path=target_path,
                    records_in=records_in,
                    records_out=records_written,
                    invalid_records=invalid_records,
                    quality_percentage=quality_percentage,
                    partition_columns=partition_columns,
                    notes=notes,
                )
            )

        finished_at = datetime.now(timezone.utc)
        _write_markdown_report(markdown_report_path, results, started_at, finished_at)
        generated_paths.append(markdown_report_path)
        _record_observability(
            settings=active_settings,
            results=results,
            started_at=started_at,
            finished_at=finished_at,
            generated_paths=generated_paths,
            json_path=observability_json_report_path,
            markdown_path=observability_markdown_report_path,
        )
        LOGGER.info("Relatório gerado em %s", markdown_report_path)
        return results
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        _record_observability(
            settings=active_settings,
            results=results,
            started_at=started_at,
            finished_at=finished_at,
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


def _transform_entity(
    entity_name: str,
    dataframe: DataFrame,
) -> tuple[DataFrame, int, tuple[str, ...], str]:
    transformers = {
        "customers": _transform_customers,
        "products": _transform_products,
        "campaigns": _transform_campaigns,
        "orders": _transform_orders,
        "order_items": _transform_order_items,
        "web_events": _transform_web_events,
    }
    return transformers[entity_name](dataframe)


def _standardize_common_columns(dataframe: DataFrame) -> DataFrame:
    standardized = dataframe
    if "ingestion_timestamp" in standardized.columns:
        standardized = standardized.withColumn(
            "ingestion_timestamp",
            F.to_timestamp("ingestion_timestamp"),
        )
    if "processing_date" in standardized.columns:
        standardized = standardized.withColumn("processing_date", F.to_date("processing_date"))
    if "source_file" in standardized.columns:
        standardized = standardized.withColumn("source_file", F.col("source_file").cast("string"))
    return standardized


def _normalize_token(column_name: str) -> F.Column:
    return F.lower(F.regexp_replace(F.trim(F.col(column_name)), r"[\s\-]+", "_"))


def _transform_customers(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = (
        _standardize_common_columns(dataframe)
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn(
            "email",
            F.when(
                F.col("email").isNull() | (F.trim(F.col("email")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.lower(F.trim(F.col("email")))),
        )
    )

    missing_customer_id = F.col("customer_id").isNull() | (F.col("customer_id") == "")
    valid_email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    invalid_email = F.col("email").isNull() | ~F.col("email").rlike(valid_email_pattern)

    enriched = standardized.withColumn("is_email_valid", ~invalid_email)
    invalid_records = enriched.filter(missing_customer_id | invalid_email).count()
    transformed = enriched.filter(~missing_customer_id)

    notes = (
        "Emails em minúsculo, clientes sem customer_id removidos e emails inválidos " "sinalizados."
    )
    return transformed, invalid_records, ("processing_date",), notes


def _transform_products(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = _standardize_common_columns(dataframe).withColumn(
        "product_id",
        F.trim(F.col("product_id")),
    )
    standardized = standardized.withColumn(
        "category",
        F.when(
            F.col("category").isNull() | (F.trim(F.col("category")) == ""),
            F.lit("unknown"),
        ).otherwise(_normalize_token("category")),
    )

    raw_unit_price = F.col("unit_price").cast("double")
    missing_product_id = F.col("product_id").isNull() | (F.col("product_id") == "")
    invalid_price = raw_unit_price.isNull() | (raw_unit_price < 0)

    enriched = standardized.withColumn(
        "unit_price",
        F.greatest(F.coalesce(raw_unit_price, F.lit(0.0)), F.lit(0.0)),
    )
    invalid_records = enriched.filter(missing_product_id | invalid_price).count()
    transformed = enriched.filter(~missing_product_id)

    notes = (
        "Categorias padronizadas, preços negativos ajustados para zero e produtos sem "
        "product_id removidos."
    )
    return transformed, invalid_records, ("processing_date",), notes


def _transform_campaigns(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = _standardize_common_columns(dataframe)
    raw_budget = F.col("budget").cast("double")
    standardized = standardized.withColumn("start_date", F.to_date("start_date")).withColumn(
        "end_date",
        F.to_date("end_date"),
    )
    standardized = standardized.withColumn(
        "budget",
        F.greatest(F.coalesce(raw_budget, F.lit(0.0)), F.lit(0.0)),
    )

    invalid_budget = raw_budget.isNull() | (raw_budget < 0)
    invalid_dates = F.col("start_date").isNull() | F.col("end_date").isNull()
    invalid_records = standardized.filter(invalid_budget | invalid_dates).count()

    notes = "Budgets negativos ajustados para zero e datas convertidas para o tipo date."
    return standardized, invalid_records, ("processing_date",), notes


def _transform_orders(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = _standardize_common_columns(dataframe).withColumn(
        "order_date",
        F.to_timestamp("order_date"),
    )
    standardized = standardized.withColumn("normalized_status", _normalize_token("order_status"))

    valid_status = F.col("normalized_status").isin(*VALID_ORDER_STATUSES)
    enriched = (
        standardized.withColumn("is_valid_status", valid_status)
        .withColumn(
            "order_status",
            F.when(valid_status, F.col("normalized_status")).otherwise(F.lit(None).cast("string")),
        )
        .drop("normalized_status")
    )
    invalid_records = enriched.filter(
        ~F.col("is_valid_status") | F.col("order_date").isNull()
    ).count()

    notes = (
        "Status padronizados; valores fora da lista válida são mantidos com flag e "
        "order_status nulo."
    )
    return enriched, invalid_records, ("processing_date",), notes


def _transform_order_items(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = _standardize_common_columns(dataframe).withColumn(
        "quantity",
        F.coalesce(F.col("quantity").cast("int"), F.lit(0)),
    )
    standardized = standardized.withColumn(
        "unit_price",
        F.coalesce(F.col("unit_price").cast("double"), F.lit(0.0)),
    ).withColumn(
        "discount_amount",
        F.coalesce(F.col("discount_amount").cast("double"), F.lit(0.0)),
    )

    enriched = (
        standardized.withColumn("is_quantity_valid", F.col("quantity") > 0)
        .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
    )
    invalid_records = enriched.filter(~F.col("is_quantity_valid")).count()

    notes = (
        "Valores monetários derivados adicionados e quantidades não positivas "
        "sinalizadas como inválidas."
    )
    return enriched, invalid_records, ("processing_date",), notes


def _transform_web_events(dataframe: DataFrame) -> tuple[DataFrame, int, tuple[str, ...], str]:
    standardized = _standardize_common_columns(dataframe)
    standardized = standardized.withColumn(
        "event_type",
        F.when(
            F.col("event_type").isNull() | (F.trim(F.col("event_type")) == ""),
            F.lit(None).cast("string"),
        ).otherwise(_normalize_token("event_type")),
    )
    standardized = standardized.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    invalid_records = standardized.filter(
        F.col("event_type").isNull() | F.col("event_timestamp").isNull()
    ).count()

    notes = (
        "event_type padronizado, event_timestamp convertido para timestamp e "
        "campaign_id nulo permitido."
    )
    return standardized, invalid_records, ("processing_date",), notes


def _write_entity(
    dataframe: DataFrame,
    target_path: Path,
    partition_columns: tuple[str, ...],
) -> None:
    writer = dataframe.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(target_path))


def _calculate_quality_percentage(records_in: int, invalid_records: int) -> float:
    if records_in == 0:
        return 100.0
    valid_records = max(records_in - invalid_records, 0)
    return round((valid_records / records_in) * 100, 2)


def _write_markdown_report(
    report_path: Path,
    results: list[SilverJobResult],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    duration_seconds = round((finished_at - started_at).total_seconds(), 2)
    total_in = sum(result.records_in for result in results)
    total_out = sum(result.records_out for result in results)
    total_invalid = sum(result.invalid_records for result in results)

    lines = [
        "# Bronze to Silver Report",
        "",
        f"- Started at: `{started_at.isoformat()}`",
        f"- Finished at: `{finished_at.isoformat()}`",
        f"- Duration seconds: `{duration_seconds}`",
        f"- Total entities: `{len(results)}`",
        f"- Total records in: `{total_in}`",
        f"- Total records out: `{total_out}`",
        f"- Total invalid records: `{total_invalid}`",
        "",
        (
            "| Entity | Records In | Records Out | Invalid Records | Quality % | "
            "Partition Columns | Notes |"
        ),
        "| --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]

    for result in results:
        partition_description = (
            ", ".join(result.partition_columns) if result.partition_columns else "-"
        )
        lines.append(
            f"| {result.entity_name} | {result.records_in} | {result.records_out} | "
            f"{result.invalid_records} | {result.quality_percentage} | `{partition_description}` | "
            f"{result.notes} |"
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _record_observability(
    settings: Settings,
    results: list[SilverJobResult],
    started_at: datetime,
    finished_at: datetime,
    generated_paths: list[Path],
    json_path: Path,
    markdown_path: Path,
    status: str | None = None,
    error_message: str | None = None,
) -> None:
    total_records_in = sum(result.records_in for result in results)
    total_records_out = sum(result.records_out for result in results)
    total_invalid_records = sum(result.invalid_records for result in results)
    entity_metrics = [
        {
            "entity": result.entity_name,
            "records_in": result.records_in,
            "records_out": result.records_out,
            "invalid_records": result.invalid_records,
            "notes": result.notes,
        }
        for result in results
    ]

    metric = build_pipeline_execution_metric(
        job_name="bronze_to_silver",
        started_at=started_at,
        finished_at=finished_at,
        source_layer="bronze",
        target_layer="silver",
        records_in=total_records_in,
        records_out=total_records_out,
        invalid_records=total_invalid_records,
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
        LOGGER.exception("Falha ao registrar observabilidade do job bronze_to_silver.")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    started_at = time.perf_counter()
    run_bronze_to_silver(
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        report_path=args.report_path,
        observability_json_path=args.observability_json_path,
        observability_markdown_path=args.observability_markdown_path,
        app_name=args.app_name,
        master=args.master,
        remote=args.remote,
    )
    elapsed_seconds = round(time.perf_counter() - started_at, 2)
    LOGGER.info("Job bronze_to_silver finalizado em %s segundos", elapsed_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
