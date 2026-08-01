"""Approved SQL catalog and read-only validation for certified Gold analytics."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml

_MUTATION = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|copy|vacuum|optimize)\b",
    re.I,
)
_TABLE_REFERENCE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.I)
_LIMIT = re.compile(r"\blimit\s+(\d+)\b", re.I)
_SET_OPERATION = re.compile(r"\b(union|intersect|except)\b", re.I)


class SqlSafetyError(ValueError):
    """Raised when a catalog or query violates analytics safety controls."""


class SqlExecutor(Protocol):
    """Adapter contract for Spark, Databricks SQL, or a test executor."""

    def __call__(self, sql: str) -> Sequence[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class ApprovedSqlTemplate:
    template_id: str
    description: str
    intent_phrases: tuple[str, ...]
    sql: str
    allowed_tables: frozenset[str]
    max_rows: int


def validate_read_only_sql(sql: str, *, allowed_tables: frozenset[str], max_rows: int = 100) -> str:
    """Validate a single SELECT query against mutation, table, and row controls."""

    normalized = sql.strip()
    without_final_semicolon = normalized[:-1].rstrip() if normalized.endswith(";") else normalized
    if ";" in without_final_semicolon:
        raise SqlSafetyError("Multiple SQL statements are not allowed.")
    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise SqlSafetyError("SQL comments are not allowed.")
    if not re.match(r"^(select|with)\b", without_final_semicolon, re.I):
        raise SqlSafetyError("Only SELECT queries are allowed.")
    if _MUTATION.search(without_final_semicolon):
        raise SqlSafetyError("Mutation and administration statements are forbidden.")
    if _SET_OPERATION.search(without_final_semicolon):
        raise SqlSafetyError("SQL set operations are not allowed.")
    referenced = {match.group(1).lower() for match in _TABLE_REFERENCE.finditer(normalized)}
    allowed = {table.lower() for table in allowed_tables}
    if not referenced:
        raise SqlSafetyError("Query must reference an approved table.")
    unapproved = sorted(referenced - allowed)
    if unapproved:
        raise SqlSafetyError(f"Unapproved tables: {', '.join(unapproved)}")
    limits = [int(match.group(1)) for match in _LIMIT.finditer(normalized)]
    if not limits:
        raise SqlSafetyError("Approved queries must include a LIMIT.")
    if max(limits) > max_rows:
        raise SqlSafetyError(f"LIMIT cannot exceed {max_rows}.")
    return without_final_semicolon


class ApprovedSqlCatalog:
    """Versioned catalog that maps user intent only to pre-reviewed SQL."""

    def __init__(self, templates: tuple[ApprovedSqlTemplate, ...]) -> None:
        self.templates = templates

    @classmethod
    def load(cls, path: Path) -> ApprovedSqlCatalog:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise SqlSafetyError("SQL template catalog must declare version: 1.")
        raw_templates = payload.get("templates")
        if not isinstance(raw_templates, dict) or not raw_templates:
            raise SqlSafetyError("SQL template catalog must contain templates.")
        templates = []
        for template_id, raw in raw_templates.items():
            if not isinstance(raw, dict):
                raise SqlSafetyError(f"Template {template_id} must be a mapping.")
            try:
                phrases = tuple(str(item).lower() for item in raw["intent_phrases"])
                allowed_tables = frozenset(str(item) for item in raw["allowed_tables"])
                max_rows = int(raw.get("max_rows", 100))
                sql = validate_read_only_sql(
                    str(raw["sql"]), allowed_tables=allowed_tables, max_rows=max_rows
                )
                template = ApprovedSqlTemplate(
                    template_id=str(template_id),
                    description=str(raw["description"]),
                    intent_phrases=phrases,
                    sql=sql,
                    allowed_tables=allowed_tables,
                    max_rows=max_rows,
                )
            except (KeyError, TypeError) as error:
                raise SqlSafetyError(f"Template {template_id} is incomplete.") from error
            if not phrases or not allowed_tables:
                raise SqlSafetyError(f"Template {template_id} needs intents and tables.")
            templates.append(template)
        return cls(tuple(templates))

    def match(self, question: str) -> ApprovedSqlTemplate | None:
        normalized = question.lower()
        matches = [
            (
                sum(len(phrase.split()) for phrase in item.intent_phrases if phrase in normalized),
                item,
            )
            for item in self.templates
        ]
        score, template = max(matches, key=lambda pair: (pair[0], pair[1].template_id))
        return template if score else None

    def execute(
        self, template: ApprovedSqlTemplate, executor: SqlExecutor
    ) -> tuple[dict[str, Any], ...]:
        sql = validate_read_only_sql(
            template.sql, allowed_tables=template.allowed_tables, max_rows=template.max_rows
        )
        rows = executor(sql)
        return tuple(dict(row) for row in rows[: template.max_rows])
