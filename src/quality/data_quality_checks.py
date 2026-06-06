"""Checagens automatizadas de Data Quality para as camadas Silver e Gold."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import Settings
from src.utils.logger import configure_logging, get_logger
from src.utils.spark import create_spark_session

LOGGER = get_logger(__name__)
ALLOWED_ORDER_STATUSES = ("created", "paid", "shipped", "cancelled", "refunded")
EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


@dataclass(frozen=True, slots=True)
class QualityCheckResult:
    layer: str
    entity: str
    rule_name: str
    description: str
    total_records: int
    failed_records: int
    passed: bool

    @property
    def quality_percentage(self) -> float:
        if self.total_records == 0:
            return 100.0
        valid_records = max(self.total_records - self.failed_records, 0)
        return round((valid_records / self.total_records) * 100, 2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Executa checagens automatizadas de qualidade nas camadas Silver e Gold."
    )
    parser.add_argument("--silver-dir", type=Path, default=settings.silver_data_path)
    parser.add_argument("--gold-dir", type=Path, default=settings.gold_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.data_quality_report_path / "data_quality_report.md",
    )
    parser.add_argument(
        "--json-path",
        type=Path,
        default=settings.data_quality_report_path / "data_quality_results.json",
    )
    parser.add_argument("--app-name", default="data-quality-checks")
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


def run_data_quality_checks(
    settings: Settings | None = None,
    silver_dir: Path | None = None,
    gold_dir: Path | None = None,
    report_path: Path | None = None,
    json_path: Path | None = None,
    app_name: str | None = None,
    master: str | None = None,
    remote: str | None = None,
    spark: SparkSession | None = None,
) -> list[QualityCheckResult]:
    active_settings = settings or Settings()
    markdown_report_path = (
        report_path or active_settings.data_quality_report_path / "data_quality_report.md"
    ).resolve()
    json_report_path = (
        json_path or active_settings.data_quality_report_path / "data_quality_results.json"
    ).resolve()
    markdown_report_path.parent.mkdir(parents=True, exist_ok=True)
    json_report_path.parent.mkdir(parents=True, exist_ok=True)

    own_spark_session = spark is None
    spark_session = spark or build_spark_session(
        settings=active_settings,
        app_name=app_name or "data-quality-checks",
        master=master,
        remote=remote,
    )

    started_at = datetime.now(timezone.utc)
    try:
        results = []
        results.extend(
            run_silver_quality_checks(spark_session, silver_dir or active_settings.silver_data_path)
        )
        results.extend(
            run_gold_quality_checks(spark_session, gold_dir or active_settings.gold_data_path)
        )
        finished_at = datetime.now(timezone.utc)
        write_quality_reports(
            results, markdown_report_path, json_report_path, started_at, finished_at
        )
        LOGGER.info(
            "Relatórios de Data Quality gerados em %s e %s", markdown_report_path, json_report_path
        )
        return results
    finally:
        if own_spark_session:
            spark_session.stop()


def run_silver_quality_checks(
    spark: SparkSession,
    silver_dir: Path,
) -> list[QualityCheckResult]:
    source_root = silver_dir.resolve()
    if not source_root.exists():
        raise FileNotFoundError(f"Diretório silver não encontrado: {source_root}")

    customers = spark.read.parquet(str(source_root / "customers"))
    products = spark.read.parquet(str(source_root / "products"))
    orders = spark.read.parquet(str(source_root / "orders"))
    order_items = spark.read.parquet(str(source_root / "order_items"))

    results = [
        _evaluate_rule(
            layer="silver",
            entity="customers",
            rule_name="customer_id_not_null",
            description="customer_id não pode ser nulo ou vazio.",
            dataframe=customers,
            failure_condition=F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""),
        ),
        _evaluate_rule(
            layer="silver",
            entity="products",
            rule_name="product_id_not_null",
            description="product_id não pode ser nulo ou vazio.",
            dataframe=products,
            failure_condition=F.col("product_id").isNull() | (F.trim(F.col("product_id")) == ""),
        ),
        _evaluate_rule(
            layer="silver",
            entity="orders",
            rule_name="order_id_not_null",
            description="order_id não pode ser nulo ou vazio.",
            dataframe=orders,
            failure_condition=F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""),
        ),
        _evaluate_rule(
            layer="silver",
            entity="order_items",
            rule_name="valid_quantity_positive",
            description="quantity deve ser maior que zero para registros válidos.",
            dataframe=order_items,
            failure_condition=(
                F.coalesce(F.col("is_quantity_valid"), F.lit(False)) & (F.col("quantity") <= 0)
            )
            | ((F.col("quantity") > 0) & (~F.coalesce(F.col("is_quantity_valid"), F.lit(False)))),
        ),
        _evaluate_rule(
            layer="silver",
            entity="order_items",
            rule_name="valid_net_amount_non_negative",
            description="net_amount não deve ser negativo para registros válidos.",
            dataframe=order_items,
            failure_condition=(
                F.coalesce(F.col("is_quantity_valid"), F.lit(False)) & (F.col("net_amount") < 0)
            ),
        ),
        _evaluate_rule(
            layer="silver",
            entity="orders",
            rule_name="order_status_allowed",
            description="order_status deve estar dentro da lista permitida.",
            dataframe=orders,
            failure_condition=(
                F.col("order_status").isNotNull()
                & (~F.lower(F.col("order_status")).isin(*ALLOWED_ORDER_STATUSES))
            )
            | (F.coalesce(F.col("is_valid_status"), F.lit(False)) & F.col("order_status").isNull()),
        ),
        _evaluate_rule(
            layer="silver",
            entity="customers",
            rule_name="invalid_email_flagged",
            description="emails inválidos devem estar sinalizados em is_email_valid.",
            dataframe=customers,
            failure_condition=(
                (F.col("email").isNull() | ~F.lower(F.col("email")).rlike(EMAIL_PATTERN))
                & F.coalesce(F.col("is_email_valid"), F.lit(True))
            )
            | (
                F.col("email").isNotNull()
                & F.lower(F.col("email")).rlike(EMAIL_PATTERN)
                & (~F.coalesce(F.col("is_email_valid"), F.lit(False)))
            ),
        ),
    ]
    return results


def run_gold_quality_checks(
    spark: SparkSession,
    gold_dir: Path,
) -> list[QualityCheckResult]:
    source_root = gold_dir.resolve()
    if not source_root.exists():
        raise FileNotFoundError(f"Diretório gold não encontrado: {source_root}")

    dim_customer = spark.read.parquet(str(source_root / "dim_customer"))
    dim_product = spark.read.parquet(str(source_root / "dim_product"))
    dim_campaign = spark.read.parquet(str(source_root / "dim_campaign"))
    dim_date = spark.read.parquet(str(source_root / "dim_date"))
    fct_sales = spark.read.parquet(str(source_root / "fct_sales"))

    results = [
        _evaluate_rule(
            layer="gold",
            entity="dim_customer",
            rule_name="customer_key_not_null",
            description="customer_key não pode ser nulo.",
            dataframe=dim_customer,
            failure_condition=F.col("customer_key").isNull(),
        ),
        _evaluate_rule(
            layer="gold",
            entity="dim_product",
            rule_name="product_key_not_null",
            description="product_key não pode ser nulo.",
            dataframe=dim_product,
            failure_condition=F.col("product_key").isNull(),
        ),
        _evaluate_rule(
            layer="gold",
            entity="dim_campaign",
            rule_name="campaign_key_not_null",
            description="campaign_key não pode ser nulo.",
            dataframe=dim_campaign,
            failure_condition=F.col("campaign_key").isNull(),
        ),
        _evaluate_rule(
            layer="gold",
            entity="dim_date",
            rule_name="date_key_not_null",
            description="date_key não pode ser nulo.",
            dataframe=dim_date,
            failure_condition=F.col("date_key").isNull(),
        ),
        _evaluate_rule(
            layer="gold",
            entity="fct_sales",
            rule_name="fct_sales_required_dimension_keys",
            description="fct_sales deve ter customer_key e product_key preenchidos.",
            dataframe=fct_sales,
            failure_condition=F.col("customer_key").isNull() | F.col("product_key").isNull(),
        ),
        _evaluate_rule(
            layer="gold",
            entity="fct_sales",
            rule_name="net_amount_numeric",
            description="net_amount deve ser numérico.",
            dataframe=fct_sales,
            failure_condition=F.col("net_amount").isNull()
            | F.col("net_amount").cast("double").isNull(),
        ),
        _evaluate_uniqueness_rule(
            layer="gold",
            entity="dim_date",
            rule_name="date_key_unique",
            description="dim_date deve ter date_key único.",
            dataframe=dim_date,
            key_columns=("date_key",),
        ),
        _evaluate_uniqueness_rule(
            layer="gold",
            entity="fct_sales",
            rule_name="order_item_id_unique",
            description="fct_sales não deve ter order_item_id duplicado.",
            dataframe=fct_sales,
            key_columns=("order_item_id",),
        ),
    ]
    return results


def write_quality_reports(
    results: list[QualityCheckResult],
    markdown_path: Path,
    json_path: Path,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    report_started_at = started_at or datetime.now(timezone.utc)
    report_finished_at = finished_at or datetime.now(timezone.utc)
    summary = _build_summary(results, report_started_at, report_finished_at)

    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    markdown_lines = [
        "# Data Quality Report",
        "",
        f"- Started at: `{summary['started_at']}`",
        f"- Finished at: `{summary['finished_at']}`",
        f"- Duration seconds: `{summary['duration_seconds']}`",
        f"- Total checks: `{summary['total_checks']}`",
        f"- Passed checks: `{summary['passed_checks']}`",
        f"- Failed checks: `{summary['failed_checks']}`",
        "",
        "| Layer | Entity | Rule | Total Records | Failed Records | Quality % | Passed |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]

    for result in results:
        markdown_lines.append(
            f"| {result.layer} | {result.entity} | {result.rule_name} | {result.total_records} | "
            f"{result.failed_records} | {result.quality_percentage} | {result.passed} |"
        )

    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")

    json_payload = {
        "summary": summary,
        "checks": [
            {
                **asdict(result),
                "quality_percentage": result.quality_percentage,
            }
            for result in results
        ],
    }
    json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_summary(
    results: list[QualityCheckResult],
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, object]:
    return {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 2),
        "total_checks": len(results),
        "passed_checks": sum(1 for result in results if result.passed),
        "failed_checks": sum(1 for result in results if not result.passed),
    }


def _evaluate_rule(
    layer: str,
    entity: str,
    rule_name: str,
    description: str,
    dataframe: DataFrame,
    failure_condition: F.Column,
) -> QualityCheckResult:
    total_records = dataframe.count()
    failed_records = dataframe.filter(failure_condition).count()
    passed = failed_records == 0

    LOGGER.info(
        "DQ %s.%s.%s -> total=%s failed=%s passed=%s",
        layer,
        entity,
        rule_name,
        total_records,
        failed_records,
        passed,
    )

    return QualityCheckResult(
        layer=layer,
        entity=entity,
        rule_name=rule_name,
        description=description,
        total_records=total_records,
        failed_records=failed_records,
        passed=passed,
    )


def _evaluate_uniqueness_rule(
    layer: str,
    entity: str,
    rule_name: str,
    description: str,
    dataframe: DataFrame,
    key_columns: tuple[str, ...],
) -> QualityCheckResult:
    total_records = dataframe.count()
    duplicates = dataframe.groupBy(*key_columns).count().filter(F.col("count") > 1)
    failed_records = duplicates.count()
    passed = failed_records == 0

    LOGGER.info(
        "DQ %s.%s.%s -> total=%s duplicate_keys=%s passed=%s",
        layer,
        entity,
        rule_name,
        total_records,
        failed_records,
        passed,
    )

    return QualityCheckResult(
        layer=layer,
        entity=entity,
        rule_name=rule_name,
        description=description,
        total_records=total_records,
        failed_records=failed_records,
        passed=passed,
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    started_at = time.perf_counter()
    run_data_quality_checks(
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        report_path=args.report_path,
        json_path=args.json_path,
        app_name=args.app_name,
        master=args.master,
        remote=args.remote,
    )
    elapsed_seconds = round(time.perf_counter() - started_at, 2)
    LOGGER.info("Checagens de Data Quality finalizadas em %s segundos", elapsed_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
