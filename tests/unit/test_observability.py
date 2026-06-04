from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.observability.metrics_collector import build_pipeline_execution_metric
from src.observability.pipeline_monitor import record_pipeline_metric


class ObservabilityTestCase(unittest.TestCase):
    def test_build_pipeline_execution_metric_collects_artifacts_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            artifact_dir = tmp_path / "bronze" / "customers"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact_file = artifact_dir / "part-0000.parquet"
            artifact_file.write_text("sample-data", encoding="utf-8")

            started_at = datetime.now(timezone.utc)
            finished_at = started_at + timedelta(seconds=12)

            metric = build_pipeline_execution_metric(
                job_name="raw_to_bronze",
                started_at=started_at,
                finished_at=finished_at,
                source_layer="raw",
                target_layer="bronze",
                records_in=10,
                records_out=10,
                invalid_records=0,
                generated_paths=[artifact_dir],
                entity_metrics=[
                    {
                        "entity": "customers",
                        "records_in": 10,
                        "records_out": 10,
                        "invalid_records": 0,
                    }
                ],
            )

            self.assertEqual(metric.status, "success")
            self.assertEqual(metric.duration_seconds, 12.0)
            self.assertEqual(metric.valid_data_percentage, 100.0)
            self.assertEqual(len(metric.generated_files), 1)
            self.assertGreater(metric.approx_file_size_bytes, 0)

    def test_record_pipeline_metric_persists_json_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            output_dir = tmp_path / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "dataset.parquet"
            output_file.write_text("abc123", encoding="utf-8")

            json_path = tmp_path / "reports" / "observability" / "pipeline_metrics.json"
            markdown_path = tmp_path / "reports" / "observability" / "pipeline_metrics.md"

            base_started_at = datetime.now(timezone.utc)
            first_metric = build_pipeline_execution_metric(
                job_name="raw_to_bronze",
                started_at=base_started_at,
                finished_at=base_started_at + timedelta(seconds=5),
                source_layer="raw",
                target_layer="bronze",
                records_in=10,
                records_out=10,
                invalid_records=0,
                generated_paths=[output_file],
            )
            second_metric = build_pipeline_execution_metric(
                job_name="bronze_to_silver",
                started_at=base_started_at + timedelta(minutes=1),
                finished_at=base_started_at + timedelta(minutes=1, seconds=8),
                source_layer="bronze",
                target_layer="silver",
                records_in=10,
                records_out=9,
                invalid_records=1,
                generated_paths=[output_file],
            )

            record_pipeline_metric(first_metric, json_path=json_path, markdown_path=markdown_path)
            record_pipeline_metric(second_metric, json_path=json_path, markdown_path=markdown_path)

            self.assertTrue(json_path.exists())
            self.assertTrue(markdown_path.exists())

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            markdown = markdown_path.read_text(encoding="utf-8")

            self.assertEqual(payload["summary"]["total_executions"], 2)
            self.assertEqual(payload["summary"]["success_executions"], 1)
            self.assertEqual(payload["summary"]["warning_executions"], 1)
            self.assertIn("Pipeline Observability Metrics", markdown)
            self.assertIn("bronze_to_silver", markdown)
