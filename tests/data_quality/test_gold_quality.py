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
class GoldQualityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        module_path = (
            Path(__file__).resolve().parents[2] / "src" / "quality" / "data_quality_checks.py"
        )
        spec = importlib.util.spec_from_file_location(
            "data_quality_checks_module_gold", module_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.module = module

    def test_gold_quality_detects_dimension_and_fact_issues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            gold_dir = tmp_path / "gold"
            report_path = tmp_path / "reports" / "data_quality_report.md"
            json_path = tmp_path / "reports" / "data_quality_results.json"

            spark = self.module.build_spark_session(app_name="test-gold-quality", master="local[1]")
            try:
                self._write_gold_sources(spark, gold_dir)
                results = self.module.run_gold_quality_checks(spark, gold_dir)
                self.module.write_quality_reports(
                    results,
                    report_path,
                    json_path,
                    started_at=datetime.now(timezone.utc),
                    finished_at=datetime.now(timezone.utc),
                )

                result_map = {(result.entity, result.rule_name): result for result in results}
                self.assertFalse(result_map[("dim_customer", "customer_key_not_null")].passed)
                self.assertFalse(result_map[("dim_date", "date_key_unique")].passed)
                self.assertFalse(result_map[("fct_sales", "order_item_id_unique")].passed)
                self.assertFalse(
                    result_map[("fct_sales", "fct_sales_required_dimension_keys")].passed
                )
                self.assertFalse(result_map[("fct_sales", "net_amount_numeric")].passed)

                report_text = report_path.read_text(encoding="utf-8")
                report_json = json.loads(json_path.read_text(encoding="utf-8"))

                self.assertIn("# Data Quality Report", report_text)
                self.assertIn("date_key_unique", report_text)
                self.assertGreater(report_json["summary"]["failed_checks"], 0)
            finally:
                spark.stop()

    def _write_gold_sources(self, spark, gold_dir: Path) -> None:
        gold_dir.mkdir(parents=True, exist_ok=True)

        self._write_parquet(
            spark,
            gold_dir / "dim_customer",
            [
                {"customer_key": 1, "customer_id": "CUST-001"},
                {"customer_key": None, "customer_id": "CUST-002"},
            ],
        )
        self._write_parquet(
            spark,
            gold_dir / "dim_product",
            [
                {"product_key": 10, "product_id": "PROD-001"},
                {"product_key": 20, "product_id": "PROD-002"},
            ],
        )
        self._write_parquet(
            spark,
            gold_dir / "dim_campaign",
            [
                {"campaign_key": 100, "campaign_id": "CAMP-001"},
                {"campaign_key": None, "campaign_id": "CAMP-002"},
            ],
        )
        self._write_parquet(
            spark,
            gold_dir / "dim_date",
            [
                {"date_key": 20240315, "full_date": "2024-03-15"},
                {"date_key": 20240315, "full_date": "2024-03-15"},
            ],
        )
        self._write_parquet(
            spark,
            gold_dir / "fct_sales",
            [
                {
                    "sales_key": 1,
                    "order_item_id": "ITEM-001",
                    "customer_key": 1,
                    "product_key": 10,
                    "net_amount": "95.0",
                },
                {
                    "sales_key": 2,
                    "order_item_id": "ITEM-001",
                    "customer_key": None,
                    "product_key": None,
                    "net_amount": "invalid",
                },
            ],
        )

    def _write_parquet(self, spark, target_path: Path, rows: list[dict]) -> None:
        spark.createDataFrame(rows).write.mode("overwrite").parquet(str(target_path))
