from __future__ import annotations

import logging
import unittest

from src.config.settings import Settings
from src.utils.logger import configure_logging, get_logger


class SettingsAndLoggerTestCase(unittest.TestCase):
    def test_settings_exposes_expected_paths_and_buckets(self) -> None:
        settings = Settings()

        self.assertTrue(settings.raw_data_path.is_absolute())
        self.assertEqual(settings.raw_data_path.name, "raw")
        self.assertEqual(settings.bronze_data_path.name, "bronze")
        self.assertEqual(settings.silver_data_path.name, "silver")
        self.assertEqual(settings.gold_data_path.name, "gold")
        self.assertEqual(settings.pipeline_runs_report_path.name, "pipeline_runs")
        self.assertEqual(settings.minio.raw_bucket, "raw")
        self.assertEqual(settings.minio.gold_bucket, "gold")

    def test_spark_conf_contains_core_defaults(self) -> None:
        settings = Settings()
        spark_conf = settings.spark_conf

        self.assertEqual(spark_conf["spark.app.name"], "aws-lakehouse-engineering-lab")
        self.assertIn("spark.master", spark_conf)
        self.assertEqual(spark_conf["spark.sql.shuffle.partitions"], "8")
        self.assertEqual(spark_conf["spark.sql.adaptive.enabled"], "true")
        self.assertEqual(spark_conf["spark.sql.session.timeZone"], "UTC")
        self.assertEqual(spark_conf["spark.hadoop.fs.s3a.path.style.access"], "true")

    def test_logger_is_configured_with_project_defaults(self) -> None:
        root_logger = configure_logging(force=True)
        project_logger = get_logger("tests.settings")

        self.assertEqual(root_logger.level, logging.INFO)
        self.assertEqual(project_logger.name, "tests.settings")
        self.assertGreater(len(root_logger.handlers), 0)
        self.assertIsNotNone(root_logger.handlers[0].formatter)


if __name__ == "__main__":
    unittest.main()
