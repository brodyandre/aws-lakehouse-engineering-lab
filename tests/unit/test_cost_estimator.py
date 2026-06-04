from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.finops.cost_estimator import FinOpsParameters, run_cost_estimation


class CostEstimatorTestCase(unittest.TestCase):
    def test_run_cost_estimation_generates_reports_and_small_files_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            raw_dir = tmp_path / "raw"
            bronze_dir = tmp_path / "bronze"
            silver_dir = tmp_path / "silver"
            gold_dir = tmp_path / "gold"

            for layer_dir in (raw_dir, bronze_dir, silver_dir, gold_dir):
                layer_dir.mkdir(parents=True, exist_ok=True)

            (raw_dir / "customers.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
            (bronze_dir / "part-0000.parquet").write_text("x" * 1024, encoding="utf-8")
            (bronze_dir / "part-0001.parquet").write_text("y" * 1024, encoding="utf-8")
            (bronze_dir / "part-0002.parquet").write_text("z" * 1024, encoding="utf-8")
            (bronze_dir / "part-0003.parquet").write_text("k" * 1024, encoding="utf-8")
            (bronze_dir / "part-0004.parquet").write_text("m" * 1024, encoding="utf-8")
            (silver_dir / "part-0000.parquet").write_text("s" * 4096, encoding="utf-8")
            (gold_dir / "part-0000.parquet").write_text("g" * 8192, encoding="utf-8")

            report_path = tmp_path / "reports" / "finops" / "cost_estimation.md"
            json_path = tmp_path / "reports" / "finops" / "cost_estimation.json"

            report = run_cost_estimation(
                raw_dir=raw_dir,
                bronze_dir=bronze_dir,
                silver_dir=silver_dir,
                gold_dir=gold_dir,
                report_path=report_path,
                json_path=json_path,
                parameters=FinOpsParameters(
                    storage_cost_per_gb_month=100.0,
                    athena_cost_per_tb_scanned=1000.0,
                    small_file_threshold_mb=1.0,
                    min_file_count_for_warning=5,
                    small_file_ratio_threshold=0.5,
                ),
            )

            self.assertTrue(report_path.exists())
            self.assertTrue(json_path.exists())

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            markdown = report_path.read_text(encoding="utf-8")

            self.assertEqual(payload["summary"]["total_layers"], 4)
            self.assertGreater(payload["summary"]["total_files"], 0)
            self.assertIn("FinOps Cost Estimation", markdown)
            self.assertGreater(
                report.to_record()["summary"]["total_estimated_s3_storage_cost_usd"], 0.0
            )

            bronze_layer = next(layer for layer in payload["layers"] if layer["layer"] == "bronze")
            self.assertTrue(bronze_layer["has_small_files_problem"])
            self.assertGreater(bronze_layer["estimated_savings_cost_usd"], 0.0)
