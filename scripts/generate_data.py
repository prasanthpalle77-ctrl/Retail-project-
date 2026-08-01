"""Command-line entry point for deterministic NovaRetail source data."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from retail_lakehouse.generation import GenerationOptions, RetailDataGenerator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("data/generated"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--products", type=int, default=30)
    parser.add_argument("--stores", type=int, default=5)
    parser.add_argument("--suppliers", type=int, default=8)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--reference-time", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--valid-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reference_time = datetime.fromisoformat(args.reference_time).astimezone(UTC)
    options = GenerationOptions(
        output_root=args.output_root,
        seed=args.seed,
        reference_time=reference_time,
        customer_count=args.customers,
        product_count=args.products,
        store_count=args.stores,
        supplier_count=args.suppliers,
        order_count=args.orders,
        include_invalid=not args.valid_only,
    )
    report = RetailDataGenerator(options).generate()
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
