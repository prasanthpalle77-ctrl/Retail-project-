"""Generate coherent NovaRetail source data with controlled quality defects."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from faker import Faker

MONEY = Decimal("0.01")


def _money(value: Decimal | float | int | str) -> Decimal:
    return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, ".2f")
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


@dataclass(frozen=True)
class GenerationOptions:
    """Size, reproducibility, and output controls for a synthetic batch."""

    output_root: Path
    seed: int = 42
    reference_time: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    customer_count: int = 50
    product_count: int = 30
    store_count: int = 5
    supplier_count: int = 8
    order_count: int = 100
    include_invalid: bool = True

    def __post_init__(self) -> None:
        positive_counts = {
            "customer_count": self.customer_count,
            "product_count": self.product_count,
            "store_count": self.store_count,
            "supplier_count": self.supplier_count,
            "order_count": self.order_count,
        }
        invalid = [name for name, value in positive_counts.items() if value < 1]
        if invalid:
            raise ValueError(f"Generation counts must be positive: {', '.join(invalid)}")
        if self.reference_time.tzinfo is None:
            raise ValueError("reference_time must be timezone-aware")


@dataclass(frozen=True)
class GenerationReport:
    """Serializable description of a generated source batch."""

    batch_id: str
    output_directory: str
    files: dict[str, str]
    record_counts: dict[str, int]
    injected_issues: tuple[str, ...]
    seed: int
    generated_at: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class RetailDataGenerator:
    """Create referentially coherent retail data and documented invalid records."""

    def __init__(self, options: GenerationOptions) -> None:
        self.options = options
        # A non-cryptographic PRNG is required for reproducible synthetic fixtures.
        self.random = random.Random(options.seed)  # nosec B311
        self.fake = Faker("en_US")
        self.fake.seed_instance(options.seed)

    def generate(self) -> GenerationReport:
        """Generate all configured datasets and return the batch report."""

        batch_id = (
            self.options.reference_time.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
            + f"_seed{self.options.seed}"
        )
        output_dir = self.options.output_root / f"batch_id={batch_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        datasets = self._build_datasets()
        issues = self._inject_invalid_records(datasets) if self.options.include_invalid else []

        csv_sources = {"customers", "products", "stores", "suppliers", "promotions"}
        files: dict[str, str] = {}
        for source_name, records in datasets.items():
            suffix = ".csv" if source_name in csv_sources else ".jsonl"
            target = output_dir / f"{source_name}{suffix}"
            if suffix == ".csv":
                self._write_csv(target, records)
            else:
                self._write_json_lines(target, records)
            files[source_name] = str(target)

        report = GenerationReport(
            batch_id=batch_id,
            output_directory=str(output_dir),
            files=files,
            record_counts={name: len(rows) for name, rows in datasets.items()},
            injected_issues=tuple(issues),
            seed=self.options.seed,
            generated_at=self.options.reference_time.astimezone(UTC).isoformat(),
        )
        self._write_json(output_dir / "generation_report.json", report.as_dict())
        return report

    def _build_datasets(self) -> dict[str, list[dict[str, Any]]]:
        customers = self._customers()
        suppliers = self._suppliers()
        products = self._products(suppliers)
        stores = self._stores()
        promotions = self._promotions(products)
        orders, order_items = self._orders(customers, products, stores)
        payments = self._payments(orders)
        inventory = self._inventory_events(products, stores)
        returns = self._returns(orders, order_items)
        shipments = self._shipments(orders)
        events = self._customer_events(orders, order_items)
        return {
            "customers": customers,
            "products": products,
            "stores": stores,
            "suppliers": suppliers,
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "inventory_events": inventory,
            "returns": returns,
            "promotions": promotions,
            "shipments": shipments,
            "customer_events": events,
        }

    def _customers(self) -> list[dict[str, Any]]:
        rows = []
        for index in range(1, self.options.customer_count + 1):
            created = self.options.reference_time - timedelta(days=self.random.randint(30, 900))
            rows.append(
                {
                    "customer_id": f"C{index:06d}",
                    "first_name": self.fake.first_name(),
                    "last_name": self.fake.last_name(),
                    "email": self.fake.unique.email(),
                    "phone": self.fake.phone_number(),
                    "date_of_birth": self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                    "gender": self.random.choice(["F", "M", "NON_BINARY", "UNDISCLOSED"]),
                    "address": self.fake.street_address(),
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "country": "US",
                    "postal_code": self.fake.postcode(),
                    "loyalty_tier": self.random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"]),
                    "registration_date": created.date(),
                    "customer_status": "ACTIVE",
                    "created_at": created,
                    "updated_at": created,
                    "cdc_operation": "INSERT",
                }
            )
        return rows

    def _suppliers(self) -> list[dict[str, Any]]:
        rows = []
        for index in range(1, self.options.supplier_count + 1):
            created = self.options.reference_time - timedelta(days=self.random.randint(200, 1200))
            rows.append(
                {
                    "supplier_id": f"SUP{index:04d}",
                    "supplier_name": f"{self.fake.company()} Supply",
                    "contact_email": self.fake.company_email(),
                    "country": "US",
                    "supplier_rating": round(self.random.uniform(2.5, 5.0), 2),
                    "lead_time_days": self.random.randint(2, 30),
                    "supplier_status": "ACTIVE",
                    "created_at": created,
                    "updated_at": created,
                    "cdc_operation": "INSERT",
                }
            )
        return rows

    def _products(self, suppliers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        categories = {
            "Electronics": ["Audio", "Computers", "Mobile"],
            "Home": ["Kitchen", "Furniture", "Decor"],
            "Apparel": ["Men", "Women", "Accessories"],
            "Sports": ["Fitness", "Outdoor", "Footwear"],
        }
        rows = []
        for index in range(1, self.options.product_count + 1):
            category = self.random.choice(list(categories))
            cost = _money(self.random.uniform(3, 500))
            price = _money(cost * Decimal(str(self.random.uniform(1.2, 2.4))))
            created = self.options.reference_time - timedelta(days=self.random.randint(10, 700))
            rows.append(
                {
                    "product_id": f"P{index:06d}",
                    "sku": f"NOVA-{index:07d}",
                    "product_name": f"{self.fake.word().title()} {category} Item {index}",
                    "category": category,
                    "subcategory": self.random.choice(categories[category]),
                    "brand": self.random.choice(["Nova", "Orbit", "Summit", "Vertex", "Lumen"]),
                    "supplier_id": self.random.choice(suppliers)["supplier_id"],
                    "unit_cost": cost,
                    "list_price": price,
                    "product_status": "ACTIVE",
                    "launch_date": created.date(),
                    "created_at": created,
                    "updated_at": created,
                    "cdc_operation": "INSERT",
                }
            )
        return rows

    def _stores(self) -> list[dict[str, Any]]:
        regions = ["NORTHEAST", "SOUTHEAST", "MIDWEST", "SOUTHWEST", "WEST"]
        rows = []
        for index in range(1, self.options.store_count + 1):
            opened = self.options.reference_time - timedelta(days=self.random.randint(500, 4000))
            rows.append(
                {
                    "store_id": f"S{index:04d}",
                    "store_name": f"NovaRetail {self.fake.city()} {index}",
                    "store_type": self.random.choice(["MALL", "HIGH_STREET", "OUTLET"]),
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "country": "US",
                    "region": regions[(index - 1) % len(regions)],
                    "opening_date": opened.date(),
                    "store_status": "ACTIVE",
                    "manager_id": f"EMP{index:05d}",
                    "created_at": opened,
                    "updated_at": opened,
                    "cdc_operation": "INSERT",
                }
            )
        return rows

    def _promotions(self, products: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for index in range(1, 6):
            product = self.random.choice(products)
            start = self.options.reference_time.date() - timedelta(days=index * 14)
            rows.append(
                {
                    "promotion_id": f"PROMO{index:04d}",
                    "promotion_name": f"Campaign {index}",
                    "promotion_type": "PERCENTAGE",
                    "start_date": start,
                    "end_date": start + timedelta(days=10),
                    "discount_percentage": self.random.choice([5, 10, 15, 20]),
                    "discount_amount": _money(0),
                    "category": product["category"],
                    "product_id": product["product_id"],
                    "minimum_order_value": _money(25),
                    "promotion_status": "ACTIVE" if index == 1 else "EXPIRED",
                }
            )
        return rows

    def _orders(
        self,
        customers: list[dict[str, Any]],
        products: list[dict[str, Any]],
        stores: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        orders: list[dict[str, Any]] = []
        items: list[dict[str, Any]] = []
        item_index = 1
        for index in range(1, self.options.order_count + 1):
            ordered_at = self.options.reference_time - timedelta(
                days=self.random.randint(1, 180), minutes=self.random.randint(0, 1439)
            )
            order_id = f"O{index:08d}"
            order_items: list[dict[str, Any]] = []
            for _ in range(self.random.randint(1, 4)):
                product = self.random.choice(products)
                quantity = self.random.randint(1, 4)
                unit_price = _money(product["list_price"])
                line_gross = _money(unit_price * quantity)
                discount = _money(line_gross * Decimal(str(self.random.choice([0, 0, 0.05, 0.10]))))
                tax = _money((line_gross - discount) * Decimal("0.08"))
                line_amount = _money(line_gross - discount + tax)
                order_items.append(
                    {
                        "order_item_id": f"OI{item_index:09d}",
                        "order_id": order_id,
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "discount_amount": discount,
                        "tax_amount": tax,
                        "line_amount": line_amount,
                        "created_at": ordered_at,
                        "updated_at": ordered_at,
                    }
                )
                item_index += 1
            gross = _money(sum(row["unit_price"] * row["quantity"] for row in order_items))
            discount = _money(sum(row["discount_amount"] for row in order_items))
            tax = _money(sum(row["tax_amount"] for row in order_items))
            shipping = _money(0 if gross >= 50 else 5.99)
            net = _money(gross + tax + shipping - discount)
            channel = self.random.choice(["STORE", "WEB", "MOBILE", "MARKETPLACE"])
            orders.append(
                {
                    "order_id": order_id,
                    "customer_id": self.random.choice(customers)["customer_id"],
                    "store_id": self.random.choice(stores)["store_id"]
                    if channel == "STORE"
                    else None,
                    "channel": channel,
                    "order_status": self.random.choice(["COMPLETED", "SHIPPED", "DELIVERED"]),
                    "order_timestamp": ordered_at,
                    "currency": "USD",
                    "payment_method": self.random.choice(["CARD", "WALLET", "BANK_TRANSFER"]),
                    "shipping_amount": shipping,
                    "tax_amount": tax,
                    "discount_amount": discount,
                    "gross_amount": gross,
                    "net_amount": net,
                    "created_at": ordered_at,
                    "updated_at": ordered_at,
                    "cdc_operation": "INSERT",
                }
            )
            items.extend(order_items)
        return orders, items

    def _payments(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "payment_id": f"PAY{index:08d}",
                "order_id": order["order_id"],
                "payment_status": "CAPTURED",
                "payment_method": order["payment_method"],
                "payment_amount": order["net_amount"],
                "transaction_reference": f"TXN-{index:012d}",
                "payment_timestamp": order["order_timestamp"] + timedelta(minutes=2),
                "created_at": order["created_at"],
                "updated_at": order["updated_at"],
            }
            for index, order in enumerate(orders, start=1)
        ]

    def _inventory_events(
        self, products: list[dict[str, Any]], stores: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        rows = []
        index = 1
        for store in stores:
            for product in products:
                on_hand = self.random.randint(5, 100)
                rows.append(
                    {
                        "inventory_event_id": f"INV{index:09d}",
                        "product_id": product["product_id"],
                        "store_id": store["store_id"],
                        "event_type": "SNAPSHOT",
                        "quantity_change": 0,
                        "quantity_on_hand": on_hand,
                        "reorder_level": self.random.randint(5, 20),
                        "event_timestamp": self.options.reference_time - timedelta(hours=1),
                        "source_system": "STORE_INVENTORY",
                    }
                )
                index += 1
        return rows

    def _returns(
        self, orders: list[dict[str, Any]], order_items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        order_by_id = {row["order_id"]: row for row in orders}
        candidates = order_items[: max(1, len(order_items) // 10)]
        rows = []
        for index, item in enumerate(candidates, start=1):
            order = order_by_id[item["order_id"]]
            returned_at = order["order_timestamp"] + timedelta(days=self.random.randint(1, 20))
            rows.append(
                {
                    "return_id": f"RET{index:07d}",
                    "order_id": item["order_id"],
                    "order_item_id": item["order_item_id"],
                    "customer_id": order["customer_id"],
                    "product_id": item["product_id"],
                    "return_reason": self.random.choice(
                        ["DAMAGED", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND"]
                    ),
                    "return_status": "REFUNDED",
                    "return_quantity": 1,
                    "refund_amount": _money(item["line_amount"] / item["quantity"]),
                    "return_timestamp": returned_at,
                    "processed_timestamp": returned_at + timedelta(hours=4),
                }
            )
        return rows

    def _shipments(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        shippable = [row for row in orders if row["channel"] != "STORE"]
        for index, order in enumerate(shippable, start=1):
            shipped = order["order_timestamp"] + timedelta(hours=12)
            delivered = shipped + timedelta(days=self.random.randint(1, 7))
            rows.append(
                {
                    "shipment_id": f"SHIP{index:08d}",
                    "order_id": order["order_id"],
                    "carrier": self.random.choice(["NOVA_EXPRESS", "PARCEL_PRO", "FAST_SHIP"]),
                    "shipment_status": "DELIVERED",
                    "shipped_timestamp": shipped,
                    "expected_delivery_timestamp": shipped + timedelta(days=5),
                    "delivered_timestamp": delivered,
                    "shipping_cost": order["shipping_amount"],
                    "tracking_number": f"TRACK{index:012d}",
                }
            )
        return rows

    def _customer_events(
        self, orders: list[dict[str, Any]], order_items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        first_item_by_order: dict[str, dict[str, Any]] = {}
        for item in order_items:
            first_item_by_order.setdefault(item["order_id"], item)

        rows = []
        event_index = 1
        for order in orders:
            product_id = first_item_by_order[order["order_id"]]["product_id"]
            session_id = f"SESSION-{order['order_id']}"
            for event_type, offset in [("PRODUCT_VIEW", -10), ("ADD_TO_CART", -5), ("PURCHASE", 0)]:
                rows.append(
                    {
                        "event_id": f"EVT{event_index:010d}",
                        "session_id": session_id,
                        "customer_id": order["customer_id"],
                        "event_type": event_type,
                        "product_id": product_id,
                        "event_timestamp": order["order_timestamp"] + timedelta(minutes=offset),
                        "page_url": f"/products/{product_id}",
                        "device_type": self.random.choice(["DESKTOP", "MOBILE", "TABLET"]),
                        "browser": self.random.choice(["CHROME", "EDGE", "SAFARI", "FIREFOX"]),
                        "campaign_id": None,
                        "source": "NOVARETAIL_WEB",
                        "ingestion_timestamp": self.options.reference_time,
                    }
                )
                event_index += 1
        return rows

    def _inject_invalid_records(self, datasets: dict[str, list[dict[str, Any]]]) -> list[str]:
        issues: list[str] = []

        datasets["customers"].append(dict(datasets["customers"][0]))
        issues.append("customers: duplicate business key")
        datasets["customers"][1]["email"] = "invalid-email"
        issues.append("customers: invalid email")

        datasets["products"][1]["list_price"] = _money(-5)
        issues.append("products: negative list price")

        datasets["orders"][1]["customer_id"] = "UNKNOWN_CUSTOMER"
        issues.append("orders: broken customer foreign key")
        datasets["orders"][2]["order_status"] = "NOT_A_STATUS"
        issues.append("orders: invalid status")
        datasets["orders"][3]["net_amount"] = _money(datasets["orders"][3]["net_amount"] + 10)
        issues.append("orders: total mismatch")

        datasets["order_items"][1]["quantity"] = -2
        issues.append("order_items: negative quantity")
        datasets["payments"][1]["payment_status"] = "UNKNOWN"
        issues.append("payments: invalid status")
        datasets["inventory_events"][1]["quantity_on_hand"] = -10
        issues.append("inventory_events: negative on-hand quantity")

        if datasets["returns"]:
            datasets["returns"][0]["return_quantity"] = 999
            issues.append("returns: quantity exceeds purchased quantity")
        if len(datasets["shipments"]) > 1:
            shipment = datasets["shipments"][1]
            shipment["delivered_timestamp"] = shipment["shipped_timestamp"] - timedelta(days=1)
            issues.append("shipments: delivered before shipped")

        datasets["customer_events"].append(dict(datasets["customer_events"][0]))
        issues.append("customer_events: duplicate event ID")
        datasets["customer_events"][1]["unexpected_field"] = "schema_drift_demo"
        issues.append("customer_events: unexpected schema field")
        return issues

    @staticmethod
    def _write_csv(path: Path, records: list[dict[str, Any]]) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        key: _json_value(value)
                        if isinstance(value, (date, datetime, Decimal))
                        else value
                        for key, value in record.items()
                    }
                )
        temporary.replace(path)

    @staticmethod
    def _write_json_lines(path: Path, records: list[dict[str, Any]]) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            for record in records:
                stream.write(json.dumps(record, default=_json_value, sort_keys=True) + "\n")
        temporary.replace(path)

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, default=_json_value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
