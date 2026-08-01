"""Compact user-facing summaries for copilot responses."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from retail_lakehouse.rag.models import CopilotResponse


def present_response(response: CopilotResponse) -> str:
    """Turn governed evidence into a concise chat answer."""
    if response.refused or response.route != "sql":
        return response.answer
    template_id = _template_id(response)
    if template_id == "data_arrival":
        arrived = sum(row.get("arrival_status") == "ARRIVED" for row in response.rows)
        total = len(response.rows)
        if total and arrived == total:
            return f"All **{total} critical data checks are ARRIVED**. The lakehouse is ready."
        return f"**{arrived} of {total} data checks are ARRIVED.** Review the result table below."
    if template_id == "kpi_summary":
        return f"I found **{len(response.rows)} certified retail KPIs** and their business use."
    if template_id == "daily_sales" and response.rows:
        latest_date = max(row.get("date_key", 0) for row in response.rows)
        latest = [row for row in response.rows if row.get("date_key") == latest_date]
        details = ", ".join(
            f"{row.get('channel')}: net sales {_money(row.get('net_sales'))}, "
            f"AOV {_money(row.get('average_order_value'))}"
            for row in latest
        )
        return f"Latest certified sales for **{latest_date}** — {details}."
    if template_id == "conversion_rate":
        return (
            f"I found **{len(response.rows)} certified conversion-rate rows** by date and device."
        )
    if template_id == "product_returns":
        return f"I found **{len(response.rows)} certified product-return rows**."
    if template_id == "inventory_health":
        return f"I found **{len(response.rows)} certified inventory-health rows**."
    return f"The approved query returned **{len(response.rows)} certified rows**."


def _template_id(response: CopilotResponse) -> str:
    if not response.citations:
        return ""
    prefix = "Certified query: "
    title = response.citations[0].title
    return title[len(prefix) :] if title.startswith(prefix) else ""


def _money(value: Any) -> str:
    try:
        return f"{Decimal(str(value)):,.2f}"
    except Exception:  # pragma: no cover - defensive display fallback
        return str(value)
