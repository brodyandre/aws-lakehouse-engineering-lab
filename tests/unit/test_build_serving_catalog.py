from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

try:
    import pandas as pd

    from scripts.build_serving_catalog import GOLD_TABLES, build_serving_catalog, duckdb
except ModuleNotFoundError:  # pragma: no cover - depende do ambiente local
    pd = None
    GOLD_TABLES = ()
    build_serving_catalog = None
    duckdb = None


@unittest.skipIf(
    pd is None or build_serving_catalog is None or duckdb is None,
    "Dependências opcionais de query serving indisponíveis no ambiente atual.",
)
class BuildServingCatalogTestCase(unittest.TestCase):
    def test_build_serving_catalog_materializes_gold_and_analytics_tables(self) -> None:
        analytics_dir = Path(__file__).resolve().parents[2] / "sql" / "analytics"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gold_dir = tmp_path / "gold"
            database_path = tmp_path / "serving" / "lakehouse.duckdb"
            report_path = tmp_path / "reports" / "query" / "serving_catalog.md"
            json_path = tmp_path / "reports" / "query" / "serving_catalog.json"

            self._write_parquet(
                gold_dir / "dim_customer" / "part-0000.parquet",
                [
                    {
                        "customer_key": 1,
                        "customer_id": "C001",
                        "customer_name": "Ana",
                        "city": "Sao Paulo",
                        "state": "SP",
                    }
                ],
            )
            self._write_parquet(
                gold_dir / "dim_product" / "part-0000.parquet",
                [{"product_key": 10, "category": "electronics"}],
            )
            self._write_parquet(
                gold_dir / "dim_campaign" / "part-0000.parquet",
                [
                    {
                        "campaign_key": 100,
                        "campaign_id": "CMP-01",
                        "campaign_name": "Launch",
                        "channel": "search",
                        "budget": 1200.0,
                    }
                ],
            )
            self._write_parquet(
                gold_dir / "dim_date" / "part-0000.parquet",
                [
                    {
                        "date_key": 20250101,
                        "year": 2025,
                        "month": 1,
                        "month_name": "January",
                    }
                ],
            )
            self._write_parquet(
                gold_dir / "fct_sales" / "part-0000.parquet",
                [
                    {
                        "order_id": "O-001",
                        "date_key": 20250101,
                        "customer_key": 1,
                        "product_key": 10,
                        "campaign_key": 100,
                        "net_amount": 90.0,
                        "gross_amount": 100.0,
                        "discount_amount": 10.0,
                        "quantity": 2,
                    }
                ],
            )
            self._write_parquet(
                gold_dir / "fct_web_events" / "part-0000.parquet",
                [
                    {
                        "campaign_key": 100,
                        "session_id": "S-001",
                        "customer_key": 1,
                        "event_type": "page_view",
                    }
                ],
            )

            result = build_serving_catalog(
                gold_dir=gold_dir,
                database_path=database_path,
                analytics_dir=analytics_dir,
                report_path=report_path,
                json_path=json_path,
            )

            expected_total_tables = len(GOLD_TABLES) + len(list(analytics_dir.glob("*.sql")))

            self.assertTrue(database_path.exists())
            self.assertTrue(report_path.exists())
            self.assertTrue(json_path.exists())
            self.assertEqual(result.total_tables, expected_total_tables)
            self.assertTrue(result.database_path.endswith("lakehouse.duckdb"))

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            markdown = report_path.read_text(encoding="utf-8")

            self.assertEqual(payload["summary"]["total_tables"], expected_total_tables)
            self.assertTrue(all(table["source"] for table in payload["tables"]))
            self.assertIn("analytics", markdown)
            self.assertIn("revenue_by_category", markdown)

    @staticmethod
    def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)


if __name__ == "__main__":
    unittest.main()
