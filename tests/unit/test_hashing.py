from datetime import UTC, datetime
from decimal import Decimal

import pytest

from retail_lakehouse.utils.hashing import canonical_record, record_hash


def test_hash_is_independent_of_mapping_order() -> None:
    left = {"order_id": "O-1", "amount": Decimal("10.00")}
    right = {"amount": Decimal("10.0"), "order_id": "O-1"}

    assert canonical_record(left) == canonical_record(right)
    assert record_hash(left) == record_hash(right)


def test_naive_and_utc_datetime_hash_identically() -> None:
    naive = {"updated_at": datetime(2026, 1, 1, 12, 0)}
    aware = {"updated_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC)}

    assert record_hash(naive) == record_hash(aware)


def test_unsupported_hash_algorithm_is_clear() -> None:
    with pytest.raises(ValueError, match="Unsupported hash algorithm"):
        record_hash({"id": 1}, algorithm="not-a-hash")
