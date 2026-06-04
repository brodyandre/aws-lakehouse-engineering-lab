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
class SilverToGoldIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        job_path = Path(__file__).resolve().parents[2] / "spark" / "jobs" / "silver_to_gold.py"
        spec = importlib.util.spec_from_file_location("silver_to_gold_job", job_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.module = module

    def test_silver_to_gold_builds_star_schema_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            silver_dir = tmp_path / "silver"
            gold_dir = tmp_path / "gold"
            report_path = tmp_path / "reports" / "silver_to_gold_report.md"
            observability_json_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.json"
            )
            observability_markdown_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.md"
            )

            spark = self.module.build_spark_session(
                app_name="test-silver-to-gold",
                master="local[1]",
            )
            try:
                self._write_silver_sources(spark, silver_dir)
                results = self.module.run_silver_to_gold(
                    silver_dir=silver_dir,
                    gold_dir=gold_dir,
                    report_path=report_path,
                    observability_json_path=observability_json_path,
                    observability_markdown_path=observability_markdown_path,
                    app_name="test-silver-to-gold",
                    master="local[1]",
                    spark=spark,
                )

                self.assertEqual(len(results), 6)
                self.assertTrue(report_path.exists())
                self.assertTrue(observability_json_path.exists())
                self.assertTrue(observability_markdown_path.exists())

                dim_customer = spark.read.parquet(str(gold_dir / "dim_customer"))
                dim_product = spark.read.parquet(str(gold_dir / "dim_product"))
                dim_campaign = spark.read.parquet(str(gold_dir / "dim_campaign"))
                dim_date = spark.read.parquet(str(gold_dir / "dim_date"))
                fct_sales = spark.read.parquet(str(gold_dir / "fct_sales"))
                fct_web_events = spark.read.parquet(str(gold_dir / "fct_web_events"))

                self.assertEqual(dim_customer.count(), 2)
                self.assertEqual(dim_product.count(), 2)
                self.assertEqual(dim_campaign.count(), 2)
                self.assertEqual(dim_date.count(), 2)
                self.assertEqual(fct_sales.count(), 1)
                self.assertEqual(fct_web_events.count(), 2)

                sales_row = fct_sales.collect()[0]
                self.assertIsNotNone(sales_row.customer_key)
                self.assertIsNotNone(sales_row.product_key)
                self.assertIsNotNone(sales_row.campaign_key)
                self.assertEqual(sales_row.net_amount, 95.0)
                self.assertEqual(sales_row.order_status, "paid")

                web_rows = {row.session_id: row for row in fct_web_events.collect()}
                self.assertIsNotNone(web_rows["SESS-001"].campaign_key)
                self.assertIsNone(web_rows["SESS-002"].campaign_key)

                report_text = report_path.read_text(encoding="utf-8")
                self.assertIn("# Silver to Gold Report", report_text)
                self.assertIn("| dim_customer |", report_text)
                self.assertIn("| fct_sales |", report_text)
                observability_text = observability_markdown_path.read_text(encoding="utf-8")
                observability_json = json.loads(observability_json_path.read_text(encoding="utf-8"))
                self.assertIn("silver_to_gold", observability_text)
                self.assertEqual(observability_json["summary"]["warning_executions"], 1)
            finally:
                spark.stop()

    def _write_silver_sources(self, spark, silver_dir: Path) -> None:
        silver_dir.mkdir(parents=True, exist_ok=True)

        self._write_parquet(
            spark,
            silver_dir / "customers",
            [
                {
                    "customer_id": "CUST-001",
                    "customer_name": "Maria Silva",
                    "email": "maria@example.com",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "country": "Brazil",
                    "created_at": "2024-01-10T10:00:00",
                    "is_email_valid": True,
                    "processing_date": "2024-04-01",
                },
                {
                    "customer_id": "CUST-002",
                    "customer_name": "Joao Souza",
                    "email": None,
                    "city": "Rio de Janeiro",
                    "state": "RJ",
                    "country": "Brazil",
                    "created_at": "2024-01-11T10:00:00",
                    "is_email_valid": False,
                    "processing_date": "2024-04-01",
                },
            ],
        )

        self._write_parquet(
            spark,
            silver_dir / "products",
            [
                {
                    "product_id": "PROD-001",
                    "product_name": "Notebook Nova",
                    "category": "electronics",
                    "unit_price": 50.0,
                    "processing_date": "2024-04-01",
                },
                {
                    "product_id": "PROD-002",
                    "product_name": "Mouse Pulse",
                    "category": "accessories",
                    "unit_price": 10.0,
                    "processing_date": "2024-04-01",
                },
            ],
        )

        self._write_parquet(
            spark,
            silver_dir / "campaigns",
            [
                {
                    "campaign_id": "CAMP-001",
                    "campaign_name": "Email Wave 01",
                    "channel": "email",
                    "start_date": "2024-03-01",
                    "end_date": "2024-03-31",
                    "budget": 1000.0,
                    "processing_date": "2024-04-01",
                },
                {
                    "campaign_id": "CAMP-002",
                    "campaign_name": "Search Wave 01",
                    "channel": "search",
                    "start_date": "2024-03-10",
                    "end_date": "2024-03-25",
                    "budget": 2000.0,
                    "processing_date": "2024-04-01",
                },
            ],
        )

        self._write_parquet(
            spark,
            silver_dir / "orders",
            [
                {
                    "order_id": "ORD-001",
                    "customer_id": "CUST-001",
                    "order_date": "2024-03-15T14:00:00",
                    "payment_method": "credit_card",
                    "order_status": "paid",
                    "is_valid_status": True,
                    "processing_date": "2024-04-01",
                },
                {
                    "order_id": "ORD-002",
                    "customer_id": "CUST-002",
                    "order_date": "2024-03-16T14:00:00",
                    "payment_method": "pix",
                    "order_status": None,
                    "is_valid_status": False,
                    "processing_date": "2024-04-01",
                },
            ],
        )

        self._write_parquet(
            spark,
            silver_dir / "order_items",
            [
                {
                    "order_item_id": "ITEM-001",
                    "order_id": "ORD-001",
                    "product_id": "PROD-001",
                    "quantity": 2,
                    "gross_amount": 100.0,
                    "discount_amount": 5.0,
                    "net_amount": 95.0,
                    "is_quantity_valid": True,
                    "processing_date": "2024-04-01",
                },
                {
                    "order_item_id": "ITEM-002",
                    "order_id": "ORD-002",
                    "product_id": "PROD-002",
                    "quantity": -1,
                    "gross_amount": -10.0,
                    "discount_amount": 0.0,
                    "net_amount": -10.0,
                    "is_quantity_valid": False,
                    "processing_date": "2024-04-01",
                },
            ],
        )

        self._write_parquet(
            spark,
            silver_dir / "web_events",
            [
                {
                    "event_id": "EVT-001",
                    "customer_id": "CUST-001",
                    "session_id": "SESS-001",
                    "event_type": "page_view",
                    "page": "/landing",
                    "event_timestamp": "2024-03-15T10:00:00",
                    "device": "mobile",
                    "campaign_id": "CAMP-001",
                    "processing_date": "2024-04-01",
                },
                {
                    "event_id": "EVT-002",
                    "customer_id": "CUST-002",
                    "session_id": "SESS-002",
                    "event_type": "add_to_cart",
                    "page": "/cart",
                    "event_timestamp": "2024-03-16T10:00:00",
                    "device": "desktop",
                    "campaign_id": None,
                    "processing_date": "2024-04-01",
                },
            ],
        )

    def _write_parquet(self, spark, target_path: Path, rows: list[dict]) -> None:
        dataframe = spark.createDataFrame(rows)
        dataframe.write.mode("overwrite").parquet(str(target_path))
