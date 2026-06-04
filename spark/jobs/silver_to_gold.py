"""Job PySpark para construir a camada Gold em Star Schema."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import Settings
from src.observability.metrics_collector import build_pipeline_execution_metric
from src.observability.pipeline_monitor import record_pipeline_metric
from src.utils.logger import configure_logging, get_logger

LOGGER = get_logger(__name__)
ENTITY_NAMES = ("customers", "products", "campaigns", "orders", "order_items", "web_events")
GOLD_TABLES = (
    "dim_customer",
    "dim_product",
    "dim_campaign",
    "dim_date",
    "fct_sales",
    "fct_web_events",
)
MAX_SURROGATE_KEY = 9_223_372_036_854_775_807


@dataclass(frozen=True, slots=True)
class GoldJobResult:
    table_name: str
    source_entities: tuple[str, ...]
    target_path: Path
    records_in: int
    records_out: int
    partition_columns: tuple[str, ...]
    notes: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Lê dados da camada silver e materializa dimensões e fatos na camada gold."
    )
    parser.add_argument("--silver-dir", type=Path, default=settings.silver_data_path)
    parser.add_argument("--gold-dir", type=Path, default=settings.gold_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.pipeline_runs_report_path / "silver_to_gold_report.md",
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
    parser.add_argument("--app-name", default="silver-to-gold-job")
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

    builder = SparkSession.builder.appName(resolved_app_name).master(resolved_master)
    for key, value in active_settings.spark_conf.items():
        if key in {"spark.app.name", "spark.master"}:
            continue
        builder = builder.config(key, value)

    return builder.getOrCreate()


def run_silver_to_gold(
    settings: Settings | None = None,
    silver_dir: Path | None = None,
    gold_dir: Path | None = None,
    report_path: Path | None = None,
    observability_json_path: Path | None = None,
    observability_markdown_path: Path | None = None,
    app_name: str | None = None,
    master: str | None = None,
    spark: SparkSession | None = None,
) -> list[GoldJobResult]:
    active_settings = settings or Settings()
    source_root = (silver_dir or active_settings.silver_data_path).resolve()
    target_root = (gold_dir or active_settings.gold_data_path).resolve()
    markdown_report_path = (
        report_path or active_settings.pipeline_runs_report_path / "silver_to_gold_report.md"
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
        raise FileNotFoundError(f"Diretório silver não encontrado: {source_root}")

    target_root.mkdir(parents=True, exist_ok=True)
    markdown_report_path.parent.mkdir(parents=True, exist_ok=True)

    own_spark_session = spark is None
    spark_session = spark or build_spark_session(
        settings=active_settings,
        app_name=app_name or "silver-to-gold-job",
        master=master,
    )

    started_at = datetime.now(timezone.utc)
    results: list[GoldJobResult] = []
    generated_paths: list[Path] = []
    invalid_records = 0

    try:
        silver_frames = _read_silver_entities(spark_session, source_root)
        invalid_records = _calculate_invalid_input_records(silver_frames)
        dim_customer = _build_dim_customer(silver_frames["customers"])
        dim_product = _build_dim_product(silver_frames["products"])
        dim_campaign = _build_dim_campaign(silver_frames["campaigns"])
        dim_date = _build_dim_date(silver_frames["orders"], silver_frames["web_events"])
        fct_sales = _build_fct_sales(
            silver_frames=silver_frames,
            dim_customer=dim_customer,
            dim_product=dim_product,
            dim_campaign=dim_campaign,
            dim_date=dim_date,
        )
        fct_web_events = _build_fct_web_events(
            silver_frames=silver_frames,
            dim_customer=dim_customer,
            dim_campaign=dim_campaign,
            dim_date=dim_date,
        )

        gold_datasets = {
            "dim_customer": (
                dim_customer,
                ("customers",),
                (),
                "Dimensão de clientes com surrogate key determinística por customer_id.",
            ),
            "dim_product": (
                dim_product,
                ("products",),
                (),
                "Dimensão de produtos com categoria padronizada e chave baseada em product_id.",
            ),
            "dim_campaign": (
                dim_campaign,
                ("campaigns",),
                (),
                "Dimensão de campanhas para atribuição analítica de receita e eventos.",
            ),
            "dim_date": (
                dim_date,
                ("orders", "web_events"),
                ("year", "month"),
                "Dimensão de datas compartilhada entre vendas e eventos digitais.",
            ),
            "fct_sales": (
                fct_sales,
                ("orders", "order_items", "customers", "products", "web_events"),
                ("date_key",),
                "Fato de vendas por item de pedido, usando somente linhas válidas da Silver.",
            ),
            "fct_web_events": (
                fct_web_events,
                ("web_events", "customers", "campaigns"),
                ("date_key",),
                "Fato de eventos web com campanha opcional e customer_key obrigatório.",
            ),
        }

        for (
            table_name,
            (dataset, source_entities, partition_columns, notes),
        ) in gold_datasets.items():
            target_path = target_root / table_name
            dataset = dataset.cache()
            records_out = dataset.count()
            records_in = _calculate_input_rows(table_name, silver_frames)

            LOGGER.info(
                "Materializando '%s': entrada=%s, saída=%s",
                table_name,
                records_in,
                records_out,
            )

            _write_dataset(dataset, target_path, partition_columns)
            dataset.unpersist()
            generated_paths.append(target_path)

            results.append(
                GoldJobResult(
                    table_name=table_name,
                    source_entities=source_entities,
                    target_path=target_path,
                    records_in=records_in,
                    records_out=records_out,
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
            invalid_records=invalid_records,
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
            invalid_records=invalid_records,
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


def _read_silver_entities(spark: SparkSession, silver_root: Path) -> dict[str, DataFrame]:
    dataframes: dict[str, DataFrame] = {}
    for entity_name in ENTITY_NAMES:
        entity_path = silver_root / entity_name
        if not entity_path.exists():
            raise FileNotFoundError(f"Dataset silver não encontrado: {entity_path}")
        dataframes[entity_name] = spark.read.parquet(str(entity_path))
    return dataframes


def _surrogate_key(*column_names: str) -> F.Column:
    normalized_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("__null__"))
        for column_name in column_names
    ]
    return F.pmod(F.xxhash64(*normalized_columns), F.lit(MAX_SURROGATE_KEY))


def _build_dim_customer(customers_df: DataFrame) -> DataFrame:
    return (
        customers_df.filter(F.col("customer_id").isNotNull() & (F.trim(F.col("customer_id")) != ""))
        .dropDuplicates(["customer_id"])
        .withColumn("customer_key", _surrogate_key("customer_id"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .select(
            "customer_key",
            "customer_id",
            "customer_name",
            "email",
            "city",
            "state",
            "country",
            "created_at",
        )
    )


def _build_dim_product(products_df: DataFrame) -> DataFrame:
    return (
        products_df.filter(F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != ""))
        .dropDuplicates(["product_id"])
        .withColumn("product_key", _surrogate_key("product_id"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .select(
            "product_key",
            "product_id",
            "product_name",
            "category",
            "unit_price",
        )
    )


def _build_dim_campaign(campaigns_df: DataFrame) -> DataFrame:
    return (
        campaigns_df.filter(F.col("campaign_id").isNotNull() & (F.trim(F.col("campaign_id")) != ""))
        .dropDuplicates(["campaign_id"])
        .withColumn("campaign_key", _surrogate_key("campaign_id"))
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
        .withColumn("budget", F.col("budget").cast("double"))
        .select(
            "campaign_key",
            "campaign_id",
            "campaign_name",
            "channel",
            "start_date",
            "end_date",
            "budget",
        )
    )


def _build_dim_date(orders_df: DataFrame, web_events_df: DataFrame) -> DataFrame:
    order_dates = orders_df.select(F.to_date("order_date").alias("full_date")).filter(
        F.col("full_date").isNotNull()
    )
    event_dates = web_events_df.select(F.to_date("event_timestamp").alias("full_date")).filter(
        F.col("full_date").isNotNull()
    )

    return (
        order_dates.union(event_dates)
        .distinct()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
        )
    )


def _build_fct_sales(
    silver_frames: dict[str, DataFrame],
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_campaign: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    valid_orders = (
        silver_frames["orders"]
        .filter(F.coalesce(F.col("is_valid_status"), F.lit(False)))
        .filter(F.col("order_date").isNotNull())
        .select("order_id", "customer_id", "order_date", "payment_method", "order_status")
    )
    valid_items = (
        silver_frames["order_items"]
        .filter(F.coalesce(F.col("is_quantity_valid"), F.lit(False)))
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "gross_amount",
            "discount_amount",
            "net_amount",
        )
    )

    campaign_attribution_window = Window.partitionBy("customer_id", "event_date").orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )
    campaign_attribution = (
        silver_frames["web_events"]
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("campaign_rank", F.row_number().over(campaign_attribution_window))
        .filter(F.col("campaign_rank") == 1)
        .withColumnRenamed("event_date", "sales_date")
        .select("customer_id", "sales_date", "campaign_id")
    )

    customer_lookup = dim_customer.select("customer_id", "customer_key")
    product_lookup = dim_product.select("product_id", "product_key")
    campaign_lookup = dim_campaign.select("campaign_id", "campaign_key")
    date_lookup = dim_date.select("date_key", "full_date")

    sales = (
        valid_items.join(valid_orders, on="order_id", how="inner")
        .join(customer_lookup, on="customer_id", how="inner")
        .join(product_lookup, on="product_id", how="inner")
        .withColumn("sales_date", F.to_date("order_date"))
        .join(date_lookup, F.col("sales_date") == F.col("full_date"), how="inner")
        .drop("full_date")
        .join(campaign_attribution, ["customer_id", "sales_date"], "left")
        .join(campaign_lookup, on="campaign_id", how="left")
        .withColumn("sales_key", _surrogate_key("order_id", "order_item_id"))
        .select(
            "sales_key",
            "order_id",
            "order_item_id",
            "customer_key",
            "product_key",
            "campaign_key",
            "date_key",
            "quantity",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "payment_method",
            "order_status",
        )
    )
    return sales


def _build_fct_web_events(
    silver_frames: dict[str, DataFrame],
    dim_customer: DataFrame,
    dim_campaign: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    valid_events = (
        silver_frames["web_events"]
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .withColumn("event_date", F.to_date("event_timestamp"))
    )

    customer_lookup = dim_customer.select("customer_id", "customer_key")
    campaign_lookup = dim_campaign.select("campaign_id", "campaign_key")
    date_lookup = dim_date.select("date_key", "full_date")

    web_events = (
        valid_events.join(customer_lookup, on="customer_id", how="inner")
        .join(date_lookup, F.col("event_date") == F.col("full_date"), how="inner")
        .drop("full_date")
        .join(campaign_lookup, on="campaign_id", how="left")
        .withColumn("event_key", _surrogate_key("event_id", "session_id", "event_timestamp"))
        .select(
            "event_key",
            "customer_key",
            "campaign_key",
            "date_key",
            "event_type",
            "page",
            "device",
            "session_id",
        )
    )
    return web_events


def _calculate_input_rows(table_name: str, silver_frames: dict[str, DataFrame]) -> int:
    if table_name == "dim_customer":
        return silver_frames["customers"].count()
    if table_name == "dim_product":
        return silver_frames["products"].count()
    if table_name == "dim_campaign":
        return silver_frames["campaigns"].count()
    if table_name == "dim_date":
        return silver_frames["orders"].count() + silver_frames["web_events"].count()
    if table_name == "fct_sales":
        return silver_frames["order_items"].count()
    if table_name == "fct_web_events":
        return silver_frames["web_events"].count()
    raise ValueError(f"Tabela gold desconhecida: {table_name}")


def _calculate_invalid_input_records(silver_frames: dict[str, DataFrame]) -> int:
    invalid_customers = (
        silver_frames["customers"]
        .filter(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""))
        .count()
    )
    invalid_products = (
        silver_frames["products"]
        .filter(F.col("product_id").isNull() | (F.trim(F.col("product_id")) == ""))
        .count()
    )
    invalid_campaigns = (
        silver_frames["campaigns"]
        .filter(
            F.col("campaign_id").isNull()
            | (F.trim(F.col("campaign_id")) == "")
            | F.col("start_date").isNull()
            | F.col("end_date").isNull()
        )
        .count()
    )
    invalid_orders = (
        silver_frames["orders"]
        .filter(
            (~F.coalesce(F.col("is_valid_status"), F.lit(False))) | F.col("order_date").isNull()
        )
        .count()
    )
    invalid_order_items = (
        silver_frames["order_items"]
        .filter(
            (~F.coalesce(F.col("is_quantity_valid"), F.lit(False)))
            | F.col("net_amount").isNull()
            | (F.col("net_amount") < 0)
        )
        .count()
    )
    invalid_web_events = (
        silver_frames["web_events"]
        .filter(
            F.col("customer_id").isNull()
            | F.col("event_type").isNull()
            | F.col("event_timestamp").isNull()
        )
        .count()
    )

    return (
        invalid_customers
        + invalid_products
        + invalid_campaigns
        + invalid_orders
        + invalid_order_items
        + invalid_web_events
    )


def _write_dataset(
    dataframe: DataFrame,
    target_path: Path,
    partition_columns: tuple[str, ...],
) -> None:
    writer = dataframe.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(target_path))


def _write_markdown_report(
    report_path: Path,
    results: list[GoldJobResult],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    duration_seconds = round((finished_at - started_at).total_seconds(), 2)
    total_input = sum(result.records_in for result in results)
    total_output = sum(result.records_out for result in results)

    lines = [
        "# Silver to Gold Report",
        "",
        f"- Started at: `{started_at.isoformat()}`",
        f"- Finished at: `{finished_at.isoformat()}`",
        f"- Duration seconds: `{duration_seconds}`",
        f"- Total tables: `{len(results)}`",
        f"- Total input rows considered: `{total_input}`",
        f"- Total output rows written: `{total_output}`",
        "",
        "| Table | Source Entities | Records In | Records Out | Partition Columns | Notes |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]

    for result in results:
        partition_description = (
            ", ".join(result.partition_columns) if result.partition_columns else "-"
        )
        source_description = ", ".join(result.source_entities)
        lines.append(
            f"| {result.table_name} | `{source_description}` | {result.records_in} | "
            f"{result.records_out} | `{partition_description}` | {result.notes} |"
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _record_observability(
    settings: Settings,
    results: list[GoldJobResult],
    invalid_records: int,
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
    entity_metrics = [
        {
            "table": result.table_name,
            "records_in": result.records_in,
            "records_out": result.records_out,
            "invalid_records": "-",
            "notes": result.notes,
        }
        for result in results
    ]

    metric = build_pipeline_execution_metric(
        job_name="silver_to_gold",
        started_at=started_at,
        finished_at=finished_at,
        source_layer="silver",
        target_layer="gold",
        records_in=total_records_in,
        records_out=total_records_out,
        invalid_records=invalid_records,
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
        LOGGER.exception("Falha ao registrar observabilidade do job silver_to_gold.")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    started_at = time.perf_counter()
    run_silver_to_gold(
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        report_path=args.report_path,
        observability_json_path=args.observability_json_path,
        observability_markdown_path=args.observability_markdown_path,
        app_name=args.app_name,
        master=args.master,
    )
    elapsed_seconds = round(time.perf_counter() - started_at, 2)
    LOGGER.info("Job silver_to_gold finalizado em %s segundos", elapsed_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
