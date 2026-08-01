"""Deterministic routing across governed documents and approved Gold SQL."""

from __future__ import annotations

import json
import time
from dataclasses import replace

from retail_lakehouse.rag.audit import QueryAuditLog
from retail_lakehouse.rag.guardrails import contains_prompt_injection, redact_pii
from retail_lakehouse.rag.index import LexicalIndex
from retail_lakehouse.rag.models import Citation, CopilotResponse
from retail_lakehouse.rag.sql import ApprovedSqlCatalog, SqlExecutor

_FALLBACK = (
    "I do not have enough authorized evidence to answer that question. "
    "Try asking about a documented retail policy, runbook, KPI, or certified metric."
)


class RetailCopilot:
    """Local-first copilot that never invents document or numerical evidence."""

    def __init__(
        self,
        index: LexicalIndex,
        sql_catalog: ApprovedSqlCatalog,
        *,
        sql_executor: SqlExecutor | None = None,
        audit_log: QueryAuditLog | None = None,
    ) -> None:
        self.index = index
        self.sql_catalog = sql_catalog
        self.sql_executor = sql_executor
        self.audit_log = audit_log

    def ask(
        self,
        question: str,
        *,
        allowed_security: frozenset[str] = frozenset({"public", "internal"}),
    ) -> CopilotResponse:
        started = time.perf_counter()
        if contains_prompt_injection(question):
            response = CopilotResponse(
                answer="I cannot follow requests that attempt to override system safeguards.",
                route="guardrail",
                refused=True,
                refusal_reason="prompt_injection",
            )
        elif template := self.sql_catalog.match(question):
            response = self._answer_sql(template)
        else:
            response = self._answer_documents(question, allowed_security)
        if self.audit_log:
            audit_id = self.audit_log.record(
                question, response, latency_ms=(time.perf_counter() - started) * 1000
            )
            response = replace(response, audit_id=audit_id)
        return response

    def _answer_sql(self, template: object) -> CopilotResponse:
        # The catalog is the only source of this object.
        from retail_lakehouse.rag.sql import ApprovedSqlTemplate

        if not isinstance(template, ApprovedSqlTemplate):
            raise TypeError("Unexpected SQL template type.")
        if self.sql_executor is None:
            return CopilotResponse(
                answer=(
                    "This is a certified analytics question, but no Gold SQL executor "
                    "is connected. "
                    "Start the copilot with Spark or Databricks SQL connectivity and try again."
                ),
                route="sql",
                sql=template.sql,
                refused=True,
                refusal_reason="analytics_executor_unavailable",
            )
        rows = self.sql_catalog.execute(template, self.sql_executor)
        citation = Citation(
            citation_id="SQL1",
            title=f"Certified query: {template.template_id}",
            section=template.description,
            source_uri="gold://" + ",".join(sorted(template.allowed_tables)),
            evidence=template.sql,
        )
        if not rows:
            answer = "The approved query returned no certified rows. [SQL1]"
        else:
            answer = "Certified Gold result: " + json.dumps(rows, default=str) + " [SQL1]"
        return CopilotResponse(
            answer=answer,
            route="sql",
            citations=(citation,),
            sql=template.sql,
            rows=rows,
        )

    def _answer_documents(self, question: str, allowed_security: frozenset[str]) -> CopilotResponse:
        results = self.index.search(
            question, top_k=3, allowed_security=allowed_security, minimum_score=0.45
        )
        if not results:
            return CopilotResponse(
                answer=_FALLBACK,
                route="documents",
                refused=True,
                refusal_reason="insufficient_evidence",
            )
        citations = tuple(
            Citation(
                citation_id=f"D{position}",
                title=result.chunk.document.title,
                section=result.chunk.section,
                source_uri=result.chunk.document.source_uri,
                evidence=redact_pii(result.chunk.text),
            )
            for position, result in enumerate(results, start=1)
        )
        answer = " ".join(
            f"{citation.evidence} [{citation.citation_id}]" for citation in citations[:2]
        )
        return CopilotResponse(answer=answer, route="documents", citations=citations)
