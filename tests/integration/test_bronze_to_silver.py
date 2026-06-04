from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
HAS_JAVA = shutil.which("java") is not None


@unittest.skipUnless(HAS_PYSPARK and HAS_JAVA, "requires pyspark and java")
class BronzeToSilverIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        job_path = Path(__file__).resolve().parents[2] / "spark" / "jobs" / "bronze_to_silver.py"
        spec = importlib.util.spec_from_file_location("bronze_to_silver_job", job_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.module = module

    def test_bronze_to_silver_applies_transformations_and_generates_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_dir = tmp_path / "bronze"
            silver_dir = tmp_path / "silver"
            report_path = tmp_path / "reports" / "bronze_to_silver_report.md"
            observability_json_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.json"
            )
            observability_markdown_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.md"
            )

            spark = self.module.build_spark_session(
                app_name="test-bronze-to-silver",
                master="local[1]",
            )
            try:
                self._write_bronze_sources(spark, bronze_dir)
                results = self.module.run_bronze_to_silver(
                    bronze_dir=bronze_dir,
                    silver_dir=silver_dir,
                    report_path=report_path,
                    observability_json_path=observability_json_path,
                    observability_markdown_path=observability_markdown_path,
                    app_name="test-bronze-to-silver",
                    master="local[1]",
                    spark=spark,
                )

                self.assertEqual(len(results), 6)
                self.assertTrue(report_path.exists())
                self.assertTrue(observability_json_path.exists())
                self.assertTrue(observability_markdown_path.exists())

                customers = spark.read.parquet(str(silver_dir / "customers"))
                self.assertEqual(customers.count(), 2)
                customer_rows = {row.customer_id: row for row in customers.collect()}
                self.assertEqual(customer_rows["CUST-001"].email, "maria@example.com")
                self.assertTrue(customer_rows["CUST-001"].is_email_valid)
                self.assertFalse(customer_rows["CUST-002"].is_email_valid)

                products = spark.read.parquet(str(silver_dir / "products"))
                self.assertEqual(products.count(), 2)
                product_rows = {row.product_id: row for row in products.collect()}
                self.assertEqual(product_rows["PROD-001"].category, "electronics")
                self.assertEqual(product_rows["PROD-002"].unit_price, 0.0)

                campaigns = spark.read.parquet(str(silver_dir / "campaigns"))
                campaign_rows = {row.campaign_id: row for row in campaigns.collect()}
                self.assertEqual(campaign_rows["CAMP-001"].budget, 0.0)
                self.assertIsNotNone(campaign_rows["CAMP-001"].start_date)
                self.assertIsNotNone(campaign_rows["CAMP-001"].end_date)

                orders = spark.read.parquet(str(silver_dir / "orders"))
                order_rows = {row.order_id: row for row in orders.collect()}
                self.assertEqual(order_rows["ORD-001"].order_status, "paid")
                self.assertTrue(order_rows["ORD-001"].is_valid_status)
                self.assertIsNone(order_rows["ORD-002"].order_status)
                self.assertFalse(order_rows["ORD-002"].is_valid_status)

                order_items = spark.read.parquet(str(silver_dir / "order_items"))
                item_rows = {row.order_item_id: row for row in order_items.collect()}
                self.assertEqual(item_rows["ITEM-001"].gross_amount, 100.0)
                self.assertEqual(item_rows["ITEM-001"].net_amount, 95.0)
                self.assertFalse(item_rows["ITEM-002"].is_quantity_valid)

                web_events = spark.read.parquet(str(silver_dir / "web_events"))
                event_rows = {row.event_id: row for row in web_events.collect()}
                self.assertEqual(event_rows["EVT-001"].event_type, "page_view")
                self.assertIsNone(event_rows["EVT-001"].campaign_id)
                self.assertEqual(event_rows["EVT-002"].event_type, "add_to_cart")
                self.assertIsNotNone(event_rows["EVT-002"].event_timestamp)

                report_text = report_path.read_text(encoding="utf-8")
                self.assertIn("# Bronze to Silver Report", report_text)
                self.assertIn("| customers |", report_text)
                self.assertIn("| order_items |", report_text)
                observability_text = observability_markdown_path.read_text(encoding="utf-8")
                observability_json = json.loads(observability_json_path.read_text(encoding="utf-8"))
                self.assertIn("bronze_to_silver", observability_text)
                self.assertEqual(observability_json["summary"]["warning_executions"], 1)
            finally:
                spark.stop()

    def _write_bronze_sources(self, spark, bronze_dir: Path) -> None:
        bronze_dir.mkdir(parents=True, exist_ok=True)

        technical = {
            "ingestion_timestamp": "2024-04-01T10:00:00",
            "processing_date": "2024-04-01",
        }

        self._write_parquet(
            spark,
            bronze_dir / "customers",
            [
                {
                    "customer_id": "CUST-001",
                    "customer_name": "Maria Silva",
                    "email": "MARIA@EXAMPLE.COM",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "country": "Brazil",
                    "created_at": "2024-01-10T10:00:00",
                    "source_file": "customers.csv",
                    **technical,
                },
                {
                    "customer_id": "CUST-002",
                    "customer_name": "Joao Souza",
                    "email": None,
                    "city": "Rio de Janeiro",
                    "state": "RJ",
                    "country": "Brazil",
                    "created_at": "2024-01-11T10:00:00",
                    "source_file": "customers.csv",
                    **technical,
                },
                {
                    "customer_id": " ",
                    "customer_name": "Sem Id",
                    "email": "invalid-email",
                    "city": "Curitiba",
                    "state": "PR",
                    "country": "Brazil",
                    "created_at": "2024-01-12T10:00:00",
                    "source_file": "customers.csv",
                    **technical,
                },
            ],
        )

        self._write_parquet(
            spark,
            bronze_dir / "products",
            [
                {
                    "product_id": "PROD-001",
                    "product_name": "Notebook Nova",
                    "category": " Electronics ",
                    "unit_price": 3500.0,
                    "created_at": "2024-01-01T09:00:00",
                    "source_file": "products.csv",
                    **technical,
                },
                {
                    "product_id": "PROD-002",
                    "product_name": "Mouse Pulse",
                    "category": "Accessories",
                    "unit_price": -10.0,
                    "created_at": "2024-01-01T09:10:00",
                    "source_file": "products.csv",
                    **technical,
                },
                {
                    "product_id": None,
                    "product_name": "Sem Produto",
                    "category": "Unknown",
                    "unit_price": 5.0,
                    "created_at": "2024-01-01T09:20:00",
                    "source_file": "products.csv",
                    **technical,
                },
            ],
        )

        self._write_parquet(
            spark,
            bronze_dir / "campaigns",
            [
                {
                    "campaign_id": "CAMP-001",
                    "campaign_name": "Email Wave 01",
                    "channel": "email",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "budget": -500.0,
                    "source_file": "campaigns.csv",
                    **technical,
                },
                {
                    "campaign_id": "CAMP-002",
                    "campaign_name": "Search Wave 01",
                    "channel": "search",
                    "start_date": "2024-02-01",
                    "end_date": "2024-02-28",
                    "budget": 3000.0,
                    "source_file": "campaigns.csv",
                    **technical,
                },
            ],
        )

        self._write_parquet(
            spark,
            bronze_dir / "orders",
            [
                {
                    "order_id": "ORD-001",
                    "customer_id": "CUST-001",
                    "order_date": "2024-02-01T12:00:00",
                    "payment_method": "credit_card",
                    "order_status": "PAID",
                    "source_file": "orders.csv",
                    **technical,
                },
                {
                    "order_id": "ORD-002",
                    "customer_id": "CUST-002",
                    "order_date": "2024-02-02T12:00:00",
                    "payment_method": "pix",
                    "order_status": "legacy_sync_error",
                    "source_file": "orders.csv",
                    **technical,
                },
            ],
        )

        self._write_parquet(
            spark,
            bronze_dir / "order_items",
            [
                {
                    "order_item_id": "ITEM-001",
                    "order_id": "ORD-001",
                    "product_id": "PROD-001",
                    "quantity": 2,
                    "unit_price": 50.0,
                    "discount_amount": 5.0,
                    "source_file": "order_items.csv",
                    **technical,
                },
                {
                    "order_item_id": "ITEM-002",
                    "order_id": "ORD-002",
                    "product_id": "PROD-002",
                    "quantity": -1,
                    "unit_price": 10.0,
                    "discount_amount": 0.0,
                    "source_file": "order_items.csv",
                    **technical,
                },
            ],
        )

        self._write_parquet(
            spark,
            bronze_dir / "web_events",
            [
                {
                    "event_id": "EVT-001",
                    "customer_id": "CUST-001",
                    "session_id": "SESS-001",
                    "event_type": " PAGE_VIEW ",
                    "page": "/home",
                    "event_timestamp": "2024-02-01T12:05:00",
                    "device": "mobile",
                    "campaign_id": None,
                    "source_file": "web_events.json",
                    **technical,
                },
                {
                    "event_id": "EVT-002",
                    "customer_id": "CUST-002",
                    "session_id": "SESS-002",
                    "event_type": "Add-To-Cart",
                    "page": "/cart",
                    "event_timestamp": "2024-02-02T12:05:00",
                    "device": "desktop",
                    "campaign_id": "CAMP-002",
                    "source_file": "web_events.json",
                    **technical,
                },
            ],
        )

    def _write_parquet(self, spark, target_path: Path, rows: list[dict]) -> None:
        dataframe = spark.createDataFrame(rows)
        dataframe.write.mode("overwrite").parquet(str(target_path))
