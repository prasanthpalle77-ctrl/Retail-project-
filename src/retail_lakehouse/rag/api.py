"""Optional FastAPI delivery surface; core copilot logic has no web dependency."""

from __future__ import annotations

import importlib
from typing import Any

from retail_lakehouse.rag.copilot import RetailCopilot


def create_app(
    copilot: RetailCopilot,
    *,
    allowed_security: frozenset[str] = frozenset({"public", "internal"}),
) -> Any:
    """Create an API application when the optional RAG dependencies are installed."""

    try:
        fastapi = importlib.import_module("fastapi")
    except ImportError as error:
        raise RuntimeError("Install the RAG dependencies with: pip install -e .[rag]") from error

    app = fastapi.FastAPI(title="NovaRetail Data Copilot", version="1.0.0")

    @app.get("/health")  # type: ignore[untyped-decorator]
    def health() -> dict[str, str]:
        return {"status": "ok", "provider": "deterministic_local"}

    @app.post("/ask")  # type: ignore[untyped-decorator]
    def ask(request: dict[str, Any]) -> dict[str, Any]:
        question = str(request.get("question", "")).strip()
        if not 2 <= len(question) <= 2000:
            raise fastapi.HTTPException(status_code=422, detail="question length must be 2-2000")
        return copilot.ask(question, allowed_security=allowed_security).to_dict()

    return app
