"""Repeatable offline evaluation for routing, retrieval, citations, and refusal."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from retail_lakehouse.rag.copilot import RetailCopilot


@dataclass(frozen=True)
class EvaluationResult:
    case_id: str
    passed: bool
    expected_route: str
    actual_route: str
    citation_count: int
    refused: bool


def evaluate(copilot: RetailCopilot, path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("Evaluation set must declare version: 1.")
    results = []
    for case in payload.get("cases", []):
        response = copilot.ask(str(case["question"]))
        expected_route = str(case["expected_route"])
        expected_refused = bool(case.get("expected_refused", False))
        expected_source = case.get("expected_source_uri")
        source_match = expected_source is None or any(
            item.source_uri == expected_source for item in response.citations
        )
        passed = (
            response.route == expected_route
            and response.refused == expected_refused
            and source_match
            and (response.refused or bool(response.citations))
        )
        results.append(
            EvaluationResult(
                case_id=str(case["case_id"]),
                passed=passed,
                expected_route=expected_route,
                actual_route=response.route,
                citation_count=len(response.citations),
                refused=response.refused,
            )
        )
    passed_count = sum(result.passed for result in results)
    return {
        "total": len(results),
        "passed": passed_count,
        "pass_rate": passed_count / len(results) if results else 0.0,
        "results": [asdict(result) for result in results],
    }
