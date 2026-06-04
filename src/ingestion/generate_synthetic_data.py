"""Gera dados sintéticos de e-commerce e marketing para a camada raw."""

from __future__ import annotations

import argparse
import json
import random
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

import pandas as pd
from faker import Faker

from src.config.settings import Settings
from src.utils.logger import configure_logging, get_logger

LOGGER = get_logger(__name__)
DEFAULT_OUTPUT_DIR = Settings().raw_data_path

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "city",
    "state",
    "country",
    "created_at",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "unit_price",
    "created_at",
]
CAMPAIGN_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "channel",
    "start_date",
    "end_date",
    "budget",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "payment_method",
    "order_status",
]
ORDER_ITEM_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
]
WEB_EVENT_COLUMNS = [
    "event_id",
    "customer_id",
    "session_id",
    "event_type",
    "page",
    "event_timestamp",
    "device",
    "campaign_id",
]

LOCATIONS = [
    ("Sao Paulo", "SP", "Brazil"),
    ("Rio de Janeiro", "RJ", "Brazil"),
    ("Belo Horizonte", "MG", "Brazil"),
    ("Curitiba", "PR", "Brazil"),
    ("Porto Alegre", "RS", "Brazil"),
    ("Salvador", "BA", "Brazil"),
    ("Recife", "PE", "Brazil"),
    ("Fortaleza", "CE", "Brazil"),
    ("Goiania", "GO", "Brazil"),
    ("Florianopolis", "SC", "Brazil"),
]
PRODUCT_CATALOG = {
    "electronics": ["Notebook", "Mouse", "Headphone", "Smartphone", "Keyboard"],
    "fashion": ["Jacket", "Sneaker", "T-Shirt", "Backpack", "Jeans"],
    "home": ["Coffee Maker", "Chair", "Desk Lamp", "Mixer", "Organizer"],
    "beauty": ["Perfume", "Serum", "Shampoo", "Lipstick", "Sunscreen"],
    "sports": ["Yoga Mat", "Dumbbell", "Running Belt", "Bike Helmet", "Bottle"],
}
BRANDS = ["Nova", "Aurora", "Pulse", "Vertex", "Orbit", "Prisma", "Atlas", "Nexa"]
CAMPAIGN_CHANNELS = ["email", "social", "search", "affiliate", "display"]
PAYMENT_METHODS = ["credit_card", "pix", "debit_card", "boleto", "wallet"]
VALID_ORDER_STATUSES = ["created", "paid", "shipped", "cancelled", "refunded"]
INVALID_ORDER_STATUSES = ["awaiting_telepathy", "legacy_sync_error", "manual_override"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
EVENT_PAGES = {
    "page_view": ["/home", "/sale", "/search", "/category"],
    "product_view": ["/product/notebook", "/product/sneaker", "/product/perfume"],
    "add_to_cart": ["/cart", "/product/notebook", "/product/sneaker"],
    "checkout_start": ["/checkout"],
    "purchase": ["/thank-you"],
}
DEVICES = ["mobile", "desktop", "tablet"]


@dataclass(slots=True)
class SyntheticDataConfig:
    customers: int = 200
    products: int = 60
    campaigns: int = 12
    orders: int = 400
    order_items: int = 1_000
    web_events: int = 1_500
    max_items_per_order: int = 5
    seed: int = 42
    output_dir: Path = DEFAULT_OUTPUT_DIR

    def validate(self) -> None:
        numeric_fields = {
            "customers": self.customers,
            "products": self.products,
            "campaigns": self.campaigns,
            "orders": self.orders,
            "order_items": self.order_items,
            "web_events": self.web_events,
            "seed": self.seed,
        }
        for field_name, value in numeric_fields.items():
            if value < 0:
                raise ValueError(f"O campo '{field_name}' deve ser maior ou igual a zero.")

        if self.max_items_per_order < 1:
            raise ValueError("O campo 'max_items_per_order' deve ser maior ou igual a 1.")
        if self.orders > 0 and self.customers == 0:
            raise ValueError("Nao e possivel gerar pedidos sem clientes.")
        if self.order_items > 0 and self.products == 0:
            raise ValueError("Nao e possivel gerar itens de pedido sem produtos.")
        if self.orders == 0 and self.order_items > 0:
            raise ValueError("Nao e possivel gerar itens de pedido sem pedidos.")
        if self.orders > 0 and self.order_items < self.orders:
            raise ValueError(
                "A quantidade de order_items deve ser pelo menos igual a quantidade de orders."
            )
        if self.orders * self.max_items_per_order < self.order_items:
            raise ValueError(
                "A quantidade de order_items excede a capacidade maxima "
                "definida por max_items_per_order."
            )
        if self.web_events > 0 and self.customers == 0:
            raise ValueError("Nao e possivel gerar eventos web sem clientes.")


@dataclass(slots=True)
class GeneratedDatasetBundle:
    customers: pd.DataFrame
    products: pd.DataFrame
    campaigns: pd.DataFrame
    orders: pd.DataFrame
    order_items: pd.DataFrame
    web_events: list[dict[str, Any]]


def parse_args(argv: list[str] | None = None) -> SyntheticDataConfig:
    parser = argparse.ArgumentParser(
        description="Gera dados sintéticos de e-commerce e marketing na camada raw."
    )
    parser.add_argument("--customers", type=_non_negative_int, default=200)
    parser.add_argument("--products", type=_non_negative_int, default=60)
    parser.add_argument("--campaigns", type=_non_negative_int, default=12)
    parser.add_argument("--orders", type=_non_negative_int, default=400)
    parser.add_argument("--order-items", dest="order_items", type=_non_negative_int, default=1_000)
    parser.add_argument("--web-events", dest="web_events", type=_non_negative_int, default=1_500)
    parser.add_argument("--max-items-per-order", type=_positive_int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)

    namespace = parser.parse_args(argv)
    return SyntheticDataConfig(
        customers=namespace.customers,
        products=namespace.products,
        campaigns=namespace.campaigns,
        orders=namespace.orders,
        order_items=namespace.order_items,
        web_events=namespace.web_events,
        max_items_per_order=namespace.max_items_per_order,
        seed=namespace.seed,
        output_dir=namespace.output_dir,
    )


def generate_synthetic_data(config: SyntheticDataConfig) -> dict[str, Path]:
    """Gera e persiste os arquivos sintéticos na camada raw."""

    config.validate()
    LOGGER.info("Iniciando geração de dados sintéticos em %s", config.output_dir)

    bundle = build_dataset_bundle(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    output_paths = {
        "customers": config.output_dir / "customers.csv",
        "products": config.output_dir / "products.csv",
        "campaigns": config.output_dir / "campaigns.csv",
        "orders": config.output_dir / "orders.csv",
        "order_items": config.output_dir / "order_items.csv",
        "web_events": config.output_dir / "web_events.json",
    }

    bundle.customers.to_csv(output_paths["customers"], index=False)
    LOGGER.info(
        "Arquivo gerado: %s (%s registros)", output_paths["customers"], len(bundle.customers)
    )

    bundle.products.to_csv(output_paths["products"], index=False)
    LOGGER.info("Arquivo gerado: %s (%s registros)", output_paths["products"], len(bundle.products))

    bundle.campaigns.to_csv(output_paths["campaigns"], index=False)
    LOGGER.info(
        "Arquivo gerado: %s (%s registros)", output_paths["campaigns"], len(bundle.campaigns)
    )

    bundle.orders.to_csv(output_paths["orders"], index=False)
    LOGGER.info("Arquivo gerado: %s (%s registros)", output_paths["orders"], len(bundle.orders))

    bundle.order_items.to_csv(output_paths["order_items"], index=False)
    LOGGER.info(
        "Arquivo gerado: %s (%s registros)", output_paths["order_items"], len(bundle.order_items)
    )

    with output_paths["web_events"].open("w", encoding="utf-8") as file_obj:
        json.dump(bundle.web_events, file_obj, ensure_ascii=False, indent=2)
    LOGGER.info(
        "Arquivo gerado: %s (%s registros)", output_paths["web_events"], len(bundle.web_events)
    )

    return output_paths


def build_dataset_bundle(config: SyntheticDataConfig) -> GeneratedDatasetBundle:
    """Monta os datasets sintéticos em memória sem persistir em disco."""

    config.validate()
    rng = random.Random(config.seed)
    Faker.seed(config.seed)
    faker = Faker("pt_BR")
    now = datetime.now(timezone.utc)

    customers = _generate_customers(config, rng, faker, now)
    products = _generate_products(config, rng, now)
    campaigns = _generate_campaigns(config, rng, now)
    orders = _generate_orders(config, rng, customers, now)
    order_items = _generate_order_items(config, rng, orders, products)
    web_events = _generate_web_events(config, rng, customers, campaigns, now)

    return GeneratedDatasetBundle(
        customers=customers,
        products=products,
        campaigns=campaigns,
        orders=orders,
        order_items=order_items,
        web_events=web_events,
    )


def _generate_customers(
    config: SyntheticDataConfig,
    rng: random.Random,
    faker: Faker,
    now: datetime,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    window_start = now - timedelta(days=720)
    window_end = now - timedelta(days=90)

    for index in range(config.customers):
        customer_id = f"CUST-{index + 1:05d}"
        customer_name = faker.name()
        city, state, country = rng.choice(LOCATIONS)
        created_at = _random_datetime(rng, window_start, window_end).isoformat()
        email = _build_email(customer_name, customer_id)
        records.append(
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "city": city,
                "state": state,
                "country": country,
                "created_at": created_at,
            }
        )

    null_email_indices = _sample_bad_indices(config.customers, rng)
    for index in null_email_indices:
        records[index]["email"] = None
    if null_email_indices:
        LOGGER.info(
            "Emails nulos injetados em %s registro(s) de customers", len(null_email_indices)
        )

    return pd.DataFrame.from_records(records, columns=CUSTOMER_COLUMNS)


def _generate_products(
    config: SyntheticDataConfig,
    rng: random.Random,
    now: datetime,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    window_start = now - timedelta(days=365)
    window_end = now - timedelta(days=30)
    categories = list(PRODUCT_CATALOG)

    for index in range(config.products):
        product_id = f"PROD-{index + 1:05d}"
        category = rng.choice(categories)
        product_name = f"{rng.choice(BRANDS)} {rng.choice(PRODUCT_CATALOG[category])}"
        created_at = _random_datetime(rng, window_start, window_end).isoformat()
        unit_price = round(rng.uniform(19.9, 999.9), 2)
        records.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "unit_price": unit_price,
                "created_at": created_at,
            }
        )

    return pd.DataFrame.from_records(records, columns=PRODUCT_COLUMNS)


def _generate_campaigns(
    config: SyntheticDataConfig,
    rng: random.Random,
    now: datetime,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    start_min = (now - timedelta(days=180)).date()
    start_max = (now - timedelta(days=10)).date()

    for index in range(config.campaigns):
        campaign_id = f"CAMP-{index + 1:05d}"
        start_date = _random_date(rng, start_min, start_max)
        end_date = start_date + timedelta(days=rng.randint(7, 45))
        channel = rng.choice(CAMPAIGN_CHANNELS)
        campaign_name = f"{channel.title()} Growth Wave {index + 1:02d}"
        records.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "channel": channel,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "budget": round(rng.uniform(1_500, 25_000), 2),
            }
        )

    return pd.DataFrame.from_records(records, columns=CAMPAIGN_COLUMNS)


def _generate_orders(
    config: SyntheticDataConfig,
    rng: random.Random,
    customers: pd.DataFrame,
    now: datetime,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    order_start = now - timedelta(days=90)
    customer_ids = customers["customer_id"].tolist()

    for index in range(config.orders):
        order_id = f"ORD-{index + 1:06d}"
        order_date = _random_datetime(rng, order_start, now).isoformat()
        records.append(
            {
                "order_id": order_id,
                "customer_id": rng.choice(customer_ids),
                "order_date": order_date,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "order_status": rng.choice(VALID_ORDER_STATUSES),
            }
        )

    invalid_status_indices = _sample_bad_indices(config.orders, rng)
    for position, index in enumerate(invalid_status_indices):
        records[index]["order_status"] = INVALID_ORDER_STATUSES[
            position % len(INVALID_ORDER_STATUSES)
        ]
    if invalid_status_indices:
        LOGGER.info(
            "Status inválido injetado em %s registro(s) de orders", len(invalid_status_indices)
        )

    return pd.DataFrame.from_records(records, columns=ORDER_COLUMNS)


def _generate_order_items(
    config: SyntheticDataConfig,
    rng: random.Random,
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    order_ids = orders["order_id"].tolist()
    product_rows = products.to_dict(orient="records")
    product_by_id = {product["product_id"]: product for product in product_rows}
    product_ids = list(product_by_id)
    item_distribution = _distribute_order_items(
        total_orders=config.orders,
        total_items=config.order_items,
        max_items_per_order=config.max_items_per_order,
        rng=rng,
    )

    order_item_counter = 1
    for order_index, order_id in enumerate(order_ids):
        item_count = item_distribution[order_index]
        if item_count <= len(product_ids):
            chosen_product_ids = rng.sample(product_ids, k=item_count)
        else:
            chosen_product_ids = [rng.choice(product_ids) for _ in range(item_count)]

        for product_id in chosen_product_ids:
            product = product_by_id[product_id]
            quantity = rng.randint(1, 5)
            discount_ratio = rng.choice([0.0, 0.0, 0.05, 0.1, 0.15])
            discount_amount = round(product["unit_price"] * quantity * discount_ratio, 2)
            records.append(
                {
                    "order_item_id": f"ITEM-{order_item_counter:07d}",
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": product["unit_price"],
                    "discount_amount": discount_amount,
                }
            )
            order_item_counter += 1

    negative_quantity_indices = _sample_bad_indices(config.order_items, rng)
    for index in negative_quantity_indices:
        records[index]["quantity"] = -abs(records[index]["quantity"])
    if negative_quantity_indices:
        LOGGER.info(
            "Quantidade negativa injetada em %s registro(s) de order_items",
            len(negative_quantity_indices),
        )

    return pd.DataFrame.from_records(records, columns=ORDER_ITEM_COLUMNS)


def _generate_web_events(
    config: SyntheticDataConfig,
    rng: random.Random,
    customers: pd.DataFrame,
    campaigns: pd.DataFrame,
    now: datetime,
) -> list[dict[str, Any]]:
    customer_ids = customers["customer_id"].tolist()
    campaign_ids = campaigns["campaign_id"].tolist()
    records: list[dict[str, Any]] = []
    session_count = max(1, config.web_events // 6) if config.web_events else 0
    sessions = [
        {
            "session_id": f"SESS-{rng.getrandbits(48):012x}",
            "customer_id": rng.choice(customer_ids),
        }
        for _ in range(session_count)
    ]
    event_start = now - timedelta(days=90)

    for index in range(config.web_events):
        event_type = rng.choice(EVENT_TYPES)
        session = rng.choice(sessions)
        campaign_id = rng.choice(campaign_ids) if campaign_ids else None
        records.append(
            {
                "event_id": f"EVT-{index + 1:07d}",
                "customer_id": session["customer_id"],
                "session_id": session["session_id"],
                "event_type": event_type,
                "page": rng.choice(EVENT_PAGES[event_type]),
                "event_timestamp": _random_datetime(rng, event_start, now).isoformat(),
                "device": rng.choice(DEVICES),
                "campaign_id": campaign_id,
            }
        )

    missing_campaign_indices = _sample_bad_indices(config.web_events, rng)
    for index in missing_campaign_indices:
        records[index]["campaign_id"] = None
    if missing_campaign_indices:
        LOGGER.info(
            "campaign_id nulo injetado em %s registro(s) de web_events",
            len(missing_campaign_indices),
        )

    return records


def _distribute_order_items(
    total_orders: int,
    total_items: int,
    max_items_per_order: int,
    rng: random.Random,
) -> list[int]:
    if total_orders == 0:
        return []

    counts = [1] * total_orders
    remaining = total_items - total_orders
    available = list(range(total_orders))

    while remaining > 0:
        selected_index = rng.choice(available)
        counts[selected_index] += 1
        remaining -= 1
        if counts[selected_index] >= max_items_per_order:
            available.remove(selected_index)

    return counts


def _sample_bad_indices(total_rows: int, rng: random.Random, ratio: float = 0.03) -> list[int]:
    if total_rows == 0:
        return []
    bad_count = max(1, round(total_rows * ratio))
    bad_count = min(total_rows, bad_count)
    return sorted(rng.sample(range(total_rows), k=bad_count))


def _random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    total_seconds = int((end - start).total_seconds())
    if total_seconds <= 0:
        return start
    offset = rng.randint(0, total_seconds)
    return start + timedelta(seconds=offset)


def _random_date(rng: random.Random, start: date, end: date) -> date:
    total_days = (end - start).days
    if total_days <= 0:
        return start
    offset = rng.randint(0, total_days)
    return start + timedelta(days=offset)


def _build_email(customer_name: str, customer_id: str) -> str:
    normalized_name = _slugify(customer_name).replace("-", ".")
    return f"{normalized_name}.{customer_id.lower()}@example.com"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return "-".join(normalized.lower().split())


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("O valor deve ser maior ou igual a zero.")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("O valor deve ser maior ou igual a um.")
    return parsed


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = parse_args(argv)
    generate_synthetic_data(config)
    LOGGER.info("Geração concluída com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
