"""Deterministic special-member and business-key helpers for Gold."""

from retail_lakehouse.utils.hashing import sha256_text

UNKNOWN_BUSINESS_KEY = "__UNKNOWN__"
NOT_APPLICABLE_BUSINESS_KEY = "__NOT_APPLICABLE__"


def dimension_key(dimension_name: str, business_key: str) -> str:
    """Return a stable surrogate for static and special dimension members."""

    return sha256_text(f"novaretail|{dimension_name}|{business_key}")
