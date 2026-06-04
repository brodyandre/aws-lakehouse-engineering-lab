from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path

HAS_SYNTHETIC_DATA_DEPS = all(find_spec(module_name) for module_name in ("pandas", "faker"))


@unittest.skipUnless(HAS_SYNTHETIC_DATA_DEPS, "requires pandas and Faker")
class GenerateSyntheticDataTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = importlib.import_module("src.ingestion.generate_synthetic_data")
        cls.pd = importlib.import_module("pandas")

    def test_generate_files_with_consistent_relationships_and_controlled_bad_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            config = self.module.SyntheticDataConfig(
                customers=20,
                products=8,
                campaigns=4,
                orders=12,
                order_items=24,
                web_events=30,
                max_items_per_order=4,
                seed=7,
                output_dir=output_dir,
            )

            output_paths = self.module.generate_synthetic_data(config)

            customers = self.pd.read_csv(output_paths["customers"])
            products = self.pd.read_csv(output_paths["products"])
            campaigns = self.pd.read_csv(output_paths["campaigns"])
            orders = self.pd.read_csv(output_paths["orders"])
            order_items = self.pd.read_csv(output_paths["order_items"])
            web_events = json.loads(output_paths["web_events"].read_text(encoding="utf-8"))

            self.assertEqual(len(customers), 20)
            self.assertEqual(len(products), 8)
            self.assertEqual(len(campaigns), 4)
            self.assertEqual(len(orders), 12)
            self.assertEqual(len(order_items), 24)
            self.assertEqual(len(web_events), 30)

            self.assertTrue(set(orders["customer_id"]).issubset(set(customers["customer_id"])))
            self.assertTrue(set(order_items["order_id"]).issubset(set(orders["order_id"])))
            self.assertTrue(set(order_items["product_id"]).issubset(set(products["product_id"])))

            non_null_campaigns = {
                event["campaign_id"] for event in web_events if event["campaign_id"] is not None
            }
            self.assertTrue(non_null_campaigns.issubset(set(campaigns["campaign_id"])))

            self.assertGreater(customers["email"].isna().sum(), 0)
            self.assertGreater((order_items["quantity"] < 0).sum(), 0)
            self.assertGreater(
                (~orders["order_status"].isin(self.module.VALID_ORDER_STATUSES)).sum(),
                0,
            )
            self.assertGreater(
                sum(1 for event in web_events if event["campaign_id"] is None),
                0,
            )

    def test_invalid_order_item_configuration_raises_value_error(self) -> None:
        config = self.module.SyntheticDataConfig(
            customers=5,
            products=5,
            campaigns=2,
            orders=10,
            order_items=5,
            web_events=10,
            max_items_per_order=2,
            seed=11,
        )

        with self.assertRaises(ValueError):
            config.validate()
