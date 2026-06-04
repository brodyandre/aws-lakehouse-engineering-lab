from __future__ import annotations

import csv
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
class RawToBronzeIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        job_path = Path(__file__).resolve().parents[2] / "spark" / "jobs" / "raw_to_bronze.py"
        spec = importlib.util.spec_from_file_location("raw_to_bronze_job", job_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.module = module

    def test_raw_to_bronze_writes_parquet_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            raw_dir = tmp_path / "raw"
            bronze_dir = tmp_path / "bronze"
            report_path = tmp_path / "reports" / "raw_to_bronze_report.md"
            observability_json_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.json"
            )
            observability_markdown_path = (
                tmp_path / "reports" / "observability" / "pipeline_metrics.md"
            )
            raw_dir.mkdir(parents=True, exist_ok=True)

            self._write_sample_sources(raw_dir)

            results = self.module.run_raw_to_bronze(
                raw_dir=raw_dir,
                bronze_dir=bronze_dir,
                report_path=report_path,
                observability_json_path=observability_json_path,
                observability_markdown_path=observability_markdown_path,
                app_name="test-raw-to-bronze",
                master="local[1]",
            )

            self.assertEqual(len(results), 6)
            self.assertTrue(report_path.exists())
            self.assertTrue(observability_json_path.exists())
            self.assertTrue(observability_markdown_path.exists())

            verification_spark = self.module.build_spark_session(
                app_name="verify-raw-to-bronze",
                master="local[1]",
            )
            try:
                for entity_name in self.module.ENTITY_SPECS:
                    entity_path = bronze_dir / entity_name
                    self.assertTrue(entity_path.exists(), entity_name)

                    dataframe = verification_spark.read.parquet(str(entity_path))
                    self.assertEqual(dataframe.count(), 1, entity_name)
                    self.assertIn("ingestion_timestamp", dataframe.columns)
                    self.assertIn("source_file", dataframe.columns)
                    self.assertIn("processing_date", dataframe.columns)

                report_text = report_path.read_text(encoding="utf-8")
                self.assertIn("# Raw to Bronze Report", report_text)
                self.assertIn("| customers |", report_text)
                self.assertIn("| web_events |", report_text)
                observability_text = observability_markdown_path.read_text(encoding="utf-8")
                observability_json = json.loads(observability_json_path.read_text(encoding="utf-8"))
                self.assertIn("raw_to_bronze", observability_text)
                self.assertEqual(observability_json["summary"]["total_executions"], 1)
            finally:
                verification_spark.stop()

    def _write_sample_sources(self, raw_dir: Path) -> None:
        self._write_csv(
            raw_dir / "customers.csv",
            [
                "customer_id",
                "customer_name",
                "email",
                "city",
                "state",
                "country",
                "created_at",
            ],
            [
                [
                    "CUST-00001",
                    "Maria Silva",
                    "maria.silva@example.com",
                    "Sao Paulo",
                    "SP",
                    "Brazil",
                    "2024-01-10T10:00:00",
                ]
            ],
        )
        self._write_csv(
            raw_dir / "products.csv",
            ["product_id", "product_name", "category", "unit_price", "created_at"],
            [["PROD-00001", "Notebook Nova", "electronics", "3500.0", "2024-01-01T09:00:00"]],
        )
        self._write_csv(
            raw_dir / "campaigns.csv",
            ["campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget"],
            [["CAMP-00001", "Email Growth Wave 01", "email", "2024-01-01", "2024-01-31", "5000.0"]],
        )
        self._write_csv(
            raw_dir / "orders.csv",
            ["order_id", "customer_id", "order_date", "payment_method", "order_status"],
            [["ORD-000001", "CUST-00001", "2024-02-01T14:20:00", "credit_card", "paid"]],
        )
        self._write_csv(
            raw_dir / "order_items.csv",
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "discount_amount",
            ],
            [["ITEM-0000001", "ORD-000001", "PROD-00001", "1", "3500.0", "0.0"]],
        )

        events = [
            {
                "event_id": "EVT-0000001",
                "customer_id": "CUST-00001",
                "session_id": "SESS-000001",
                "event_type": "page_view",
                "page": "/home",
                "event_timestamp": "2024-02-01T14:10:00",
                "device": "mobile",
                "campaign_id": "CAMP-00001",
            }
        ]
        (raw_dir / "web_events.json").write_text(json.dumps(events, indent=2), encoding="utf-8")

    def _write_csv(self, file_path: Path, headers: list[str], rows: list[list[str]]) -> None:
        with file_path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.writer(file_obj)
            writer.writerow(headers)
            writer.writerows(rows)
