"""Citation-grounded retail knowledge and analytics copilot."""

from retail_lakehouse.rag.copilot import RetailCopilot
from retail_lakehouse.rag.documents import load_documents
from retail_lakehouse.rag.index import LexicalIndex
from retail_lakehouse.rag.models import CopilotResponse
from retail_lakehouse.rag.sql import ApprovedSqlCatalog

__all__ = [
    "ApprovedSqlCatalog",
    "CopilotResponse",
    "LexicalIndex",
    "RetailCopilot",
    "load_documents",
]
