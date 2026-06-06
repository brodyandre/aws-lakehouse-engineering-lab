"""Benchmark local para comparar estratégias Spark não otimizadas e otimizadas."""

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
from src.utils.logger import configure_logging, get_logger
from src.utils.spark import create_spark_session

LOGGER = get_logger(__name__)
REQUIRED_GOLD_TABLES = (
    "dim_customer",
    "dim_product",
    "dim_campaign",
    "dim_date",
    "fct_sales",
)


@dataclass(frozen=True, slots=True)
class QueryBenchmarkResult:
    query_name: str
    row_count: int
    duration_seconds: float


@dataclass(frozen=True, slots=True)
class BenchmarkVariantResult:
    variant_name: str
    started_at: datetime
    finished_at: datetime
    total_duration_seconds: float
    query_results: tuple[QueryBenchmarkResult, ...]
    applied_techniques: tuple[str, ...]
    notes: str
    shuffle_partitions: int
    adaptive_query_execution: bool
    auto_broadcast_threshold: int
    scale_factor: int


@dataclass(frozen=True, slots=True)
class SparkOptimizationBenchmarkResult:
    source_layer: str
    source_path: str
    unoptimized: BenchmarkVariantResult
    optimized: BenchmarkVariantResult

    @property
    def improvement_percentage(self) -> float:
        baseline = self.unoptimized.total_duration_seconds
        if baseline <= 0:
            return 0.0
        improvement = ((baseline - self.optimized.total_duration_seconds) / baseline) * 100
        return round(improvement, 2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description=(
            "Compara uma execução Spark não otimizada com uma versão otimizada usando "
            "dados da camada Gold."
        )
    )
    parser.add_argument("--gold-dir", type=Path, default=settings.gold_data_path)
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.pipeline_runs_report_path / "spark_optimization_benchmark.md",
    )
    parser.add_argument("--app-name-prefix", default="spark-optimization-benchmark")
    parser.add_argument("--master", default=settings.spark.master)
    parser.add_argument("--remote", default=settings.spark.remote)
    parser.add_argument("--scale-factor", type=int, default=1)
    parser.add_argument("--unoptimized-shuffle-partitions", type=int, default=200)
    parser.add_argument(
        "--optimized-shuffle-partitions",
        type=int,
        default=max(settings.spark.shuffle_partitions, 4),
    )
    return parser.parse_args(argv)


def build_spark_session(
    settings: Settings | None = None,
    app_name: str | None = None,
    master: str | None = None,
    remote: str | None = None,
    shuffle_partitions: int | None = None,
    adaptive_query_execution: bool | None = None,
    auto_broadcast_threshold: int | None = None,
) -> SparkSession:
    active_settings = settings or Settings()
    resolved_shuffle_partitions = (
        shuffle_partitions
        if shuffle_partitions is not None
        else active_settings.spark.shuffle_partitions
    )
    resolved_aqe = (
        adaptive_query_execution
        if adaptive_query_execution is not None
        else active_settings.spark.adaptive_query_execution
    )

    extra_conf = {
        "spark.sql.shuffle.partitions": str(max(resolved_shuffle_partitions, 1)),
        "spark.sql.adaptive.enabled": str(resolved_aqe).lower(),
    }
    if auto_broadcast_threshold is not None:
        extra_conf["spark.sql.autoBroadcastJoinThreshold"] = str(auto_broadcast_threshold)

    return create_spark_session(
        settings=active_settings,
        app_name=app_name,
        master=master,
        remote=remote,
        extra_conf=extra_conf,
    )


def run_benchmark(
    settings: Settings | None = None,
    gold_dir: Path | None = None,
    report_path: Path | None = None,
    app_name_prefix: str = "spark-optimization-benchmark",
    master: str | None = None,
    remote: str | None = None,
    scale_factor: int = 1,
    unoptimized_shuffle_partitions: int = 200,
    optimized_shuffle_partitions: int = 8,
) -> SparkOptimizationBenchmarkResult:
    active_settings = settings or Settings()
    source_root = (gold_dir or active_settings.gold_data_path).resolve()
    markdown_report_path = (
        report_path or active_settings.pipeline_runs_report_path / "spark_optimization_benchmark.md"
    ).resolve()
    markdown_report_path.parent.mkdir(parents=True, exist_ok=True)

    if scale_factor <= 0:
        raise ValueError("scale_factor deve ser maior que zero.")
    _validate_gold_inputs(source_root)

    unoptimized_result = _execute_variant(
        settings=active_settings,
        gold_dir=source_root,
        app_name=f"{app_name_prefix}-unoptimized",
        master=master,
        remote=remote,
        scale_factor=scale_factor,
        shuffle_partitions=unoptimized_shuffle_partitions,
        adaptive_query_execution=False,
        auto_broadcast_threshold=-1,
        variant_name="unoptimized",
    )
    optimized_result = _execute_variant(
        settings=active_settings,
        gold_dir=source_root,
        app_name=f"{app_name_prefix}-optimized",
        master=master,
        remote=remote,
        scale_factor=scale_factor,
        shuffle_partitions=optimized_shuffle_partitions,
        adaptive_query_execution=True,
        auto_broadcast_threshold=50 * 1024 * 1024,
        variant_name="optimized",
    )

    result = SparkOptimizationBenchmarkResult(
        source_layer="gold",
        source_path=str(source_root),
        unoptimized=unoptimized_result,
        optimized=optimized_result,
    )
    write_benchmark_report(result, markdown_report_path)
    LOGGER.info("Relatório de benchmark gerado em %s", markdown_report_path)
    return result


def _execute_variant(
    settings: Settings,
    gold_dir: Path,
    app_name: str,
    master: str | None,
    remote: str | None,
    scale_factor: int,
    shuffle_partitions: int,
    adaptive_query_execution: bool,
    auto_broadcast_threshold: int,
    variant_name: str,
) -> BenchmarkVariantResult:
    spark = build_spark_session(
        settings=settings,
        app_name=app_name,
        master=master,
        remote=remote,
        shuffle_partitions=shuffle_partitions,
        adaptive_query_execution=adaptive_query_execution,
        auto_broadcast_threshold=auto_broadcast_threshold,
    )

    started_at = datetime.now(timezone.utc)
    started_perf = time.perf_counter()
    cached_frames: list[DataFrame] = []

    try:
        if variant_name == "optimized":
            frames = _load_gold_tables_optimized(
                spark=spark,
                gold_dir=gold_dir,
                scale_factor=scale_factor,
                shuffle_partitions=shuffle_partitions,
            )
            cached_frames.extend(frames.values())
            query_frames = _build_optimized_queries(**frames)
            applied_techniques = (
                "column pruning nas leituras",
                "cache do fato reaproveitado em quatro agregações",
                "broadcast join para dimensões pequenas",
                "repartition do fato por date_key",
                "coalesce(1) para resultados analíticos pequenos",
                "Adaptive Query Execution habilitado",
                f"shuffle partitions ajustado para {shuffle_partitions}",
            )
            notes = (
                "Versão otimizada com reutilização controlada de memória, joins mais baratos "
                "e leitura apenas das colunas necessárias."
            )
        else:
            frames = _load_gold_tables_unoptimized(
                spark=spark,
                gold_dir=gold_dir,
                scale_factor=scale_factor,
            )
            query_frames = _build_unoptimized_queries(**frames)
            applied_techniques = (
                "sem cache",
                "sem broadcast",
                "joins diretos no fato",
                "leitura completa das tabelas",
                "shuffle partitions amplo",
                "sem repartition/coalesce planejados",
                "Adaptive Query Execution desabilitado",
            )
            notes = (
                "Versão de referência com plano mais ingênuo para destacar o efeito das "
                "otimizações locais."
            )

        query_results: list[QueryBenchmarkResult] = []
        for query_name, query_frame in query_frames.items():
            query_started = time.perf_counter()
            row_count = len(query_frame.collect())
            duration_seconds = round(time.perf_counter() - query_started, 4)
            LOGGER.info(
                "[%s] Query '%s' concluída em %s s com %s linhas",
                variant_name,
                query_name,
                duration_seconds,
                row_count,
            )
            query_results.append(
                QueryBenchmarkResult(
                    query_name=query_name,
                    row_count=row_count,
                    duration_seconds=duration_seconds,
                )
            )

        finished_at = datetime.now(timezone.utc)
        total_duration_seconds = round(time.perf_counter() - started_perf, 4)
        return BenchmarkVariantResult(
            variant_name=variant_name,
            started_at=started_at,
            finished_at=finished_at,
            total_duration_seconds=total_duration_seconds,
            query_results=tuple(query_results),
            applied_techniques=applied_techniques,
            notes=notes,
            shuffle_partitions=shuffle_partitions,
            adaptive_query_execution=adaptive_query_execution,
            auto_broadcast_threshold=auto_broadcast_threshold,
            scale_factor=scale_factor,
        )
    finally:
        for frame in cached_frames:
            try:
                frame.unpersist()
            except Exception:
                LOGGER.debug("Frame já não estava persistido.", exc_info=True)
        spark.stop()


def _load_gold_tables_unoptimized(
    spark: SparkSession,
    gold_dir: Path,
    scale_factor: int,
) -> dict[str, DataFrame]:
    sales = spark.read.parquet(str(gold_dir / "fct_sales"))
    customers = spark.read.parquet(str(gold_dir / "dim_customer"))
    products = spark.read.parquet(str(gold_dir / "dim_product"))
    campaigns = spark.read.parquet(str(gold_dir / "dim_campaign"))
    dates = spark.read.parquet(str(gold_dir / "dim_date"))
    return {
        "sales": _maybe_scale_sales_fact(spark, sales, scale_factor),
        "customers": customers,
        "products": products,
        "campaigns": campaigns,
        "dates": dates,
    }


def _load_gold_tables_optimized(
    spark: SparkSession,
    gold_dir: Path,
    scale_factor: int,
    shuffle_partitions: int,
) -> dict[str, DataFrame]:
    sales = (
        spark.read.parquet(str(gold_dir / "fct_sales"))
        .select(
            "order_id",
            "customer_key",
            "product_key",
            "campaign_key",
            "date_key",
            "net_amount",
        )
        .filter(F.col("net_amount").isNotNull())
    )
    sales = _maybe_scale_sales_fact(spark, sales, scale_factor)
    sales = sales.repartition(max(shuffle_partitions, 1), "date_key").cache()
    sales.count()

    customers = (
        spark.read.parquet(str(gold_dir / "dim_customer"))
        .select("customer_key", "customer_id", "customer_name")
        .cache()
    )
    customers.count()

    products = (
        spark.read.parquet(str(gold_dir / "dim_product")).select("product_key", "category").cache()
    )
    products.count()

    campaigns = (
        spark.read.parquet(str(gold_dir / "dim_campaign"))
        .select("campaign_key", "campaign_name")
        .cache()
    )
    campaigns.count()

    dates = (
        spark.read.parquet(str(gold_dir / "dim_date"))
        .select("date_key", "year", "month", "month_name")
        .cache()
    )
    dates.count()

    return {
        "sales": sales,
        "customers": customers,
        "products": products,
        "campaigns": campaigns,
        "dates": dates,
    }


def _build_unoptimized_queries(
    sales: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    campaigns: DataFrame,
    dates: DataFrame,
) -> dict[str, DataFrame]:
    return {
        "receita_por_categoria": (
            sales.join(products, on="product_key", how="inner")
            .groupBy("category")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
        ),
        "receita_por_mes": (
            sales.join(dates, on="date_key", how="inner")
            .groupBy("year", "month", "month_name")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy("year", "month")
        ),
        "receita_por_campanha": (
            sales.join(campaigns, on="campaign_key", how="left")
            .withColumn("campaign_name", F.coalesce(F.col("campaign_name"), F.lit("unattributed")))
            .groupBy("campaign_name")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
        ),
        "top_clientes": (
            sales.join(customers, on="customer_key", how="inner")
            .groupBy("customer_id", "customer_name")
            .agg(
                F.round(F.sum("net_amount"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .orderBy(F.desc("total_revenue"), F.desc("order_count"))
            .limit(10)
        ),
    }


def _build_optimized_queries(
    sales: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    campaigns: DataFrame,
    dates: DataFrame,
) -> dict[str, DataFrame]:
    return {
        "receita_por_categoria": (
            sales.join(F.broadcast(products), on="product_key", how="inner")
            .groupBy("category")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
            .coalesce(1)
        ),
        "receita_por_mes": (
            sales.join(F.broadcast(dates), on="date_key", how="inner")
            .groupBy("year", "month", "month_name")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy("year", "month")
            .coalesce(1)
        ),
        "receita_por_campanha": (
            sales.join(F.broadcast(campaigns), on="campaign_key", how="left")
            .withColumn("campaign_name", F.coalesce(F.col("campaign_name"), F.lit("unattributed")))
            .groupBy("campaign_name")
            .agg(F.round(F.sum("net_amount"), 2).alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
            .coalesce(1)
        ),
        "top_clientes": (
            sales.join(F.broadcast(customers), on="customer_key", how="inner")
            .groupBy("customer_id", "customer_name")
            .agg(
                F.round(F.sum("net_amount"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .orderBy(F.desc("total_revenue"), F.desc("order_count"))
            .limit(10)
            .coalesce(1)
        ),
    }


def write_benchmark_report(
    benchmark_result: SparkOptimizationBenchmarkResult,
    report_path: Path,
) -> None:
    report_path.write_text(_render_markdown_report(benchmark_result), encoding="utf-8")


def _render_markdown_report(benchmark_result: SparkOptimizationBenchmarkResult) -> str:
    unoptimized = benchmark_result.unoptimized
    optimized = benchmark_result.optimized

    lines = [
        "# Spark Optimization Benchmark",
        "",
        f"- Source layer: `{benchmark_result.source_layer}`",
        f"- Source path: `{benchmark_result.source_path}`",
        f"- Non-optimized duration: `{unoptimized.total_duration_seconds} s`",
        f"- Optimized duration: `{optimized.total_duration_seconds} s`",
        f"- Improvement percentage: `{benchmark_result.improvement_percentage}%`",
        f"- Scale factor used: `{optimized.scale_factor}`",
        "",
        "## Technical Summary",
        "",
        (
            "A versão otimizada reduz custo de shuffle e leitura ao reaproveitar o fato de vendas "
            "em memória, aplicar broadcast nas dimensões pequenas e limitar a leitura às colunas "
            "necessárias para cada análise."
        ),
        "",
        "## Variant Timing",
        "",
        (
            "| Variant | Started At | Finished At | Duration (s) | "
            "Shuffle Partitions | AQE | Broadcast Threshold |"
        ),
        "| --- | --- | --- | ---: | ---: | --- | ---: |",
        (
            f"| Non-optimized | `{unoptimized.started_at.isoformat()}` | "
            f"`{unoptimized.finished_at.isoformat()}` | {unoptimized.total_duration_seconds} | "
            f"{unoptimized.shuffle_partitions} | {unoptimized.adaptive_query_execution} | "
            f"{unoptimized.auto_broadcast_threshold} |"
        ),
        (
            f"| Optimized | `{optimized.started_at.isoformat()}` | "
            f"`{optimized.finished_at.isoformat()}` | {optimized.total_duration_seconds} | "
            f"{optimized.shuffle_partitions} | {optimized.adaptive_query_execution} | "
            f"{optimized.auto_broadcast_threshold} |"
        ),
        "",
        "## Query Breakdown",
        "",
        "| Query | Non-optimized (s) | Optimized (s) | Result Rows |",
        "| --- | ---: | ---: | ---: |",
    ]

    optimized_by_query = {
        query_result.query_name: query_result for query_result in optimized.query_results
    }
    for unoptimized_query in unoptimized.query_results:
        optimized_query = optimized_by_query[unoptimized_query.query_name]
        lines.append(
            f"| {unoptimized_query.query_name} | {unoptimized_query.duration_seconds} | "
            f"{optimized_query.duration_seconds} | {optimized_query.row_count} |"
        )

    lines.extend(
        [
            "",
            "## Techniques Applied",
            "",
            "### Non-optimized",
            "",
        ]
    )
    for technique in unoptimized.applied_techniques:
        lines.append(f"- {technique}")

    lines.extend(["", "### Optimized", ""])
    for technique in optimized.applied_techniques:
        lines.append(f"- {technique}")

    lines.extend(
        [
            "",
            "## When to Use and When to Avoid",
            "",
            (
                "- `cache`: use quando o mesmo DataFrame será reutilizado em múltiplas ações; "
                "evite quando o dataset é usado uma única vez ou a memória local é limitada."
            ),
            (
                "- `broadcast join`: use para dimensões pequenas e estáveis; evite quando a "
                "dimensão cresce demais e pode pressionar a memória do executor."
            ),
            (
                "- `repartition`: use antes de agregações ou joins amplos para redistribuir "
                "carga; evite reparticionar sem necessidade porque isso cria shuffle extra."
            ),
            (
                "- `coalesce`: use para reduzir partições em saídas pequenas; evite antes de "
                "transformações pesadas, pois pode concentrar trabalho demais em poucas tasks."
            ),
            (
                "- `Adaptive Query Execution`: use por padrão em workloads analíticos; evite "
                "confiar apenas nele sem observar skew, volume e plano físico real."
            ),
            (
                "- `column pruning`: use sempre que a consulta precisa de poucas colunas; evite "
                "leituras completas em tabelas largas quando o objetivo analítico é restrito."
            ),
            "",
            "## Notes",
            "",
            f"- Non-optimized notes: {unoptimized.notes}",
            f"- Optimized notes: {optimized.notes}",
            (
                "- Este benchmark é local. Os tempos variam conforme CPU, memória, disco, "
                "cache do sistema operacional e volume disponível na camada Gold."
            ),
        ]
    )

    return "\n".join(lines) + "\n"


def _maybe_scale_sales_fact(
    spark: SparkSession,
    sales: DataFrame,
    scale_factor: int,
) -> DataFrame:
    if scale_factor == 1:
        return sales
    multiplier_frame = spark.range(scale_factor).select(F.col("id").alias("_benchmark_multiplier"))
    return sales.crossJoin(multiplier_frame).drop("_benchmark_multiplier")


def _validate_gold_inputs(gold_dir: Path) -> None:
    if not gold_dir.exists():
        raise FileNotFoundError(f"Diretório gold não encontrado: {gold_dir}")

    missing_tables = [
        table_name for table_name in REQUIRED_GOLD_TABLES if not (gold_dir / table_name).exists()
    ]
    if missing_tables:
        missing = ", ".join(missing_tables)
        raise FileNotFoundError(
            "O benchmark requer a camada Gold materializada. " f"Tabelas ausentes: {missing}"
        )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    benchmark_result = run_benchmark(
        gold_dir=args.gold_dir,
        report_path=args.report_path,
        app_name_prefix=args.app_name_prefix,
        master=args.master,
        remote=args.remote,
        scale_factor=args.scale_factor,
        unoptimized_shuffle_partitions=args.unoptimized_shuffle_partitions,
        optimized_shuffle_partitions=args.optimized_shuffle_partitions,
    )
    LOGGER.info(
        "Benchmark Spark concluído: baseline=%s s | optimized=%s s | improvement=%s%%",
        benchmark_result.unoptimized.total_duration_seconds,
        benchmark_result.optimized.total_duration_seconds,
        benchmark_result.improvement_percentage,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
