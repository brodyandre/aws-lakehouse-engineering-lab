from __future__ import annotations

import unittest
from pathlib import Path

from src.config.settings import Settings


class ProjectLayoutTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parents[2]
        self.settings = Settings()

    def test_root_files_exist(self) -> None:
        expected_files = [
            "README.md",
            "docker-compose.yml",
            "pyproject.toml",
            "requirements.txt",
        ]

        for file_name in expected_files:
            self.assertTrue((self.project_root / file_name).exists(), file_name)

    def test_layer_paths_are_absolute_and_named(self) -> None:
        for layer in ("raw", "bronze", "silver", "gold"):
            layer_path = self.settings.layer_path(layer)
            self.assertTrue(layer_path.is_absolute())
            self.assertEqual(layer_path.name, layer)

    def test_invalid_layer_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            self.settings.layer_path("platinum")


if __name__ == "__main__":
    unittest.main()
