from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
HAS_JAVA = shutil.which("java") is not None


@unittest.skipUnless(HAS_PYSPARK and HAS_JAVA, "requires pyspark and java")
class SilverQualityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        module_path = (
            Path(__file__).resolve().parents[2] / "src" / "quality" / "data_quality_checks.py"
        )
        spec = importlib.util.spec_from_file_location("data_quality_checks_module", module_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.module = module

    def test_silver_quality_detects_failures_and_writes_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            silver_dir = tmp_path / "silver"
            report_path = tmp_path / "reports" / "data_quality_report.md"
            json_path = tmp_path / "reports" / "data_quality_results.json"

            spark = self.module.build_spark_session(
                app_name="test-silver-quality", master="local[1]"
            )
            try:
                self._write_silver_sources(spark, silver_dir)
                results = self.module.run_silver_quality_checks(spark, silver_dir)
                self.module.write_quality_reports(
                    results,
                    report_path,
                    json_path,
                    started_at=datetime.now(timezone.utc),
                    finished_at=datetime.now(timezone.utc),
                )

                result_map = {(result.entity, result.rule_name): result for result in results}
                self.assertFalse(result_map[("customers", "customer_id_not_null")].passed)
                self.assertGreater(
                    result_map[("customers", "customer_id_not_null")].failed_records, 0
                )
                self.assertFalse(result_map[("orders", "order_status_allowed")].passed)
                self.assertFalse(result_map[("order_items", "valid_quantity_positive")].passed)
                self.assertFalse(result_map[("customers", "invalid_email_flagged")].passed)

                self.assertTrue(report_path.exists())
                self.assertTrue(json_path.exists())
                report_text = report_path.read_text(encoding="utf-8")
                report_json = json.loads(json_path.read_text(encoding="utf-8"))

                self.assertIn("# Data Quality Report", report_text)
                self.assertIn("customer_id_not_null", report_text)
                self.assertEqual(report_json["summary"]["total_checks"], len(results))
                self.assertGreater(report_json["summary"]["failed_checks"], 0)
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
                    "is_email_valid": True,
                },
                {
                    "customer_id": None,
                    "customer_name": "Sem ID",
                    "email": "invalid-email",
                    "is_email_valid": True,
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
                },
                {"product_id": None, "product_name": "Produto Sem ID", "category": "other"},
            ],
        )
        self._write_parquet(
            spark,
            silver_dir / "orders",
            [
                {"order_id": "ORD-001", "order_status": "paid", "is_valid_status": True},
                {"order_id": None, "order_status": "legacy", "is_valid_status": False},
            ],
        )
        self._write_parquet(
            spark,
            silver_dir / "order_items",
            [
                {
                    "order_item_id": "ITEM-001",
                    "order_id": "ORD-001",
                    "quantity": 1,
                    "net_amount": 10.0,
                    "is_quantity_valid": True,
                },
                {
                    "order_item_id": "ITEM-002",
                    "order_id": "ORD-001",
                    "quantity": 0,
                    "net_amount": -5.0,
                    "is_quantity_valid": True,
                },
            ],
        )

    def _write_parquet(self, spark, target_path: Path, rows: list[dict]) -> None:
        spark.createDataFrame(rows).write.mode("overwrite").parquet(str(target_path))
