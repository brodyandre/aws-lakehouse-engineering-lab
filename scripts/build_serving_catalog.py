"""Materializa um catálogo DuckDB para consumo via Trino e demos SQL locais."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - dependência opcional fora do ambiente completo
    duckdb = None

from src.config.settings import Settings
from src.utils.logger import configure_logging, get_logger

LOGGER = get_logger(__name__)
GOLD_TABLES = (
    "dim_customer",
    "dim_product",
    "dim_campaign",
    "dim_date",
    "fct_sales",
    "fct_web_events",
)


@dataclass(frozen=True, slots=True)
class ServingTableResult:
    schema_name: str
    table_name: str
    row_count: int
    source: str


@dataclass(frozen=True, slots=True)
class ServingCatalogResult:
    database_path: str
    generated_at: str
    tables: tuple[ServingTableResult, ...]

    @property
    def total_tables(self) -> int:
        return len(self.tables)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = Settings()
    parser = argparse.ArgumentParser(
        description="Cria um catálogo DuckDB local com tabelas Gold e consultas analíticas."
    )
    parser.add_argument("--gold-dir", type=Path, default=settings.gold_data_path)
    parser.add_argument(
        "--database-path",
        type=Path,
        default=settings.serving_data_path / "lakehouse.duckdb",
    )
    parser.add_argument(
        "--analytics-dir",
        type=Path,
        default=settings.project_root / "sql" / "analytics",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=settings.query_report_path / "serving_catalog.md",
    )
    parser.add_argument(
        "--json-path",
        type=Path,
        default=settings.query_report_path / "serving_catalog.json",
    )
    return parser.parse_args(argv)


def _display_path(path: Path, project_root: Path) -> str:
    """Retorna caminho relativo ao projeto quando possível, senão absoluto."""

    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def build_serving_catalog(
    *,
    gold_dir: Path,
    database_path: Path,
    analytics_dir: Path,
    report_path: Path,
    json_path: Path,
) -> ServingCatalogResult:
    if duckdb is None:
        raise ModuleNotFoundError(
            "A dependência 'duckdb' não está instalada. Execute 'make setup-dev' "
            "ou instale os requisitos Python do projeto."
        )

    project_root = Settings().project_root.resolve()
    resolved_gold_dir = gold_dir.resolve()
    resolved_database_path = database_path.resolve()
    resolved_analytics_dir = analytics_dir.resolve()
    resolved_report_path = report_path.resolve()
    resolved_json_path = json_path.resolve()

    if not resolved_gold_dir.exists():
        raise FileNotFoundError(f"Diretório gold não encontrado: {resolved_gold_dir}")
    if not resolved_analytics_dir.exists():
        raise FileNotFoundError(f"Diretório de consultas não encontrado: {resolved_analytics_dir}")

    resolved_database_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_report_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_json_path.parent.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).isoformat()
    table_results: list[ServingTableResult] = []

    with duckdb.connect(str(resolved_database_path)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS gold")
        connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")

        for table_name in GOLD_TABLES:
            parquet_files = sorted(
                path.as_posix() for path in (resolved_gold_dir / table_name).rglob("*.parquet")
            )
            if not parquet_files:
                raise FileNotFoundError(
                    f"Nenhum arquivo Parquet encontrado para gold.{table_name} em "
                    f"{resolved_gold_dir / table_name}"
                )

            LOGGER.info("Materializando tabela gold.%s no catálogo DuckDB.", table_name)
            connection.execute(
                f"CREATE OR REPLACE TABLE gold.{table_name} AS "
                f"SELECT * FROM read_parquet({json.dumps(parquet_files)})"
            )
            row_count = int(
                connection.execute(f"SELECT COUNT(*) FROM gold.{table_name}").fetchone()[0]
            )
            table_results.append(
                ServingTableResult(
                    schema_name="gold",
                    table_name=table_name,
                    row_count=row_count,
                    source=_display_path(resolved_gold_dir / table_name, project_root),
                )
            )

        for sql_file in sorted(resolved_analytics_dir.glob("*.sql")):
            table_name = sql_file.stem
            query_sql = sql_file.read_text(encoding="utf-8").strip().rstrip(";")
            LOGGER.info("Materializando tabela analytics.%s no catálogo DuckDB.", table_name)
            connection.execute(f"CREATE OR REPLACE TABLE analytics.{table_name} AS {query_sql}")
            row_count = int(
                connection.execute(f"SELECT COUNT(*) FROM analytics.{table_name}").fetchone()[0]
            )
            table_results.append(
                ServingTableResult(
                    schema_name="analytics",
                    table_name=table_name,
                    row_count=row_count,
                    source=_display_path(sql_file, project_root),
                )
            )

    result = ServingCatalogResult(
        database_path=_display_path(resolved_database_path, project_root),
        generated_at=generated_at,
        tables=tuple(table_results),
    )
    _write_reports(result=result, report_path=resolved_report_path, json_path=resolved_json_path)
    LOGGER.info("Catálogo DuckDB atualizado em %s", resolved_database_path)
    return result


def _write_reports(
    *,
    result: ServingCatalogResult,
    report_path: Path,
    json_path: Path,
) -> None:
    payload = {
        "summary": {
            "database_path": result.database_path,
            "generated_at": result.generated_at,
            "total_tables": result.total_tables,
        },
        "tables": [asdict(table) for table in result.tables],
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Query Serving Catalog",
        "",
        f"- Database DuckDB: `{result.database_path}`",
        f"- Gerado em: `{result.generated_at}`",
        f"- Total de tabelas materializadas: `{result.total_tables}`",
        "",
        "| Schema | Tabela | Linhas | Origem |",
        "| --- | --- | ---: | --- |",
    ]

    for table in result.tables:
        lines.append(
            f"| {table.schema_name} | {table.table_name} | {table.row_count} | `{table.source}` |"
        )

    lines.extend(
        [
            "",
            "## Exemplos de Consulta no Trino",
            "",
            "```sql",
            "SHOW SCHEMAS FROM lakehouse;",
            "SHOW TABLES FROM lakehouse.analytics;",
            "SELECT * FROM lakehouse.analytics.revenue_by_category LIMIT 10;",
            "SELECT * FROM lakehouse.analytics.campaign_performance LIMIT 10;",
            "```",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    build_serving_catalog(
        gold_dir=args.gold_dir,
        database_path=args.database_path,
        analytics_dir=args.analytics_dir,
        report_path=args.report_path,
        json_path=args.json_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
