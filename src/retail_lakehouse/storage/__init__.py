"""Lakehouse storage primitives."""

from retail_lakehouse.storage.delta import merge_current_state, merge_insert_or_update

__all__ = ["merge_current_state", "merge_insert_or_update"]
