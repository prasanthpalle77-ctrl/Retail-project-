"""NovaRetail Copilot chat interface for Databricks Apps."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import streamlit as st
from databricks import sql as databricks_sql
from databricks.sdk.core import Config

from retail_lakehouse.rag import (
    ApprovedSqlCatalog,
    LexicalIndex,
    RetailCopilot,
    load_documents,
    present_response,
)

ROOT = Path(__file__).resolve().parent
CATALOG = os.getenv("NOVARETAIL_CATALOG", "novaretail_dev")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SUGGESTIONS = (
    "Has the data arrived?",
    "Show all KPIs and their meanings",
    "What are the latest net sales and average order value by channel?",
    "What is the latest stockout rate?",
    "How many days can I return an unopened item?",
)


def execute_sql(sql: str) -> Sequence[Mapping[str, Any]]:
    """Run approved read-only SQL through the app service principal."""
    if not WAREHOUSE_ID:
        raise RuntimeError("The SQL warehouse resource is not configured.")
    if not _IDENTIFIER.fullmatch(CATALOG):
        raise RuntimeError("The configured catalog name is invalid.")
    config = Config()
    hostname = config.host.removeprefix("https://").removeprefix("http://")
    with (
        databricks_sql.connect(
            server_hostname=hostname,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            credentials_provider=lambda: config.authenticate,
            _use_arrow_native_complex_types=False,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"USE CATALOG `{CATALOG}`")
        cursor.execute("USE SCHEMA `gold`")
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


@st.cache_resource
def build_copilot() -> RetailCopilot:
    """Load governed documents and approved SQL once per app process."""
    return RetailCopilot(
        LexicalIndex(load_documents(ROOT / "data" / "documents")),
        ApprovedSqlCatalog.load(ROOT / "configs" / "rag_sql_templates.yml"),
        sql_executor=execute_sql if WAREHOUSE_ID else None,
    )


def render_response(payload: dict[str, Any]) -> None:
    """Render one assistant message with optional evidence details."""
    st.markdown(str(payload["content"]))
    response = payload.get("response", {})
    rows = response.get("rows") or []
    if rows:
        with st.expander(f"View certified data ({len(rows)} rows)"):
            st.dataframe(rows, use_container_width=True, hide_index=True)
    citations = response.get("citations") or []
    if citations:
        with st.expander(f"View sources ({len(citations)})"):
            for citation in citations:
                st.markdown(f"**{citation['title']}** — {citation['section']}")
                st.caption(str(citation["source_uri"]))
                st.code(str(citation["evidence"]), language="sql" if response.get("sql") else None)
    if response.get("sql"):
        with st.expander("View approved SQL"):
            st.code(str(response["sql"]), language="sql")


def submit_question(question: str) -> None:
    """Ask the governed copilot and append both chat messages."""
    st.session_state.messages.append({"role": "user", "content": question})
    try:
        response = build_copilot().ask(question)
        assistant = {
            "role": "assistant",
            "content": present_response(response),
            "response": response.to_dict(),
        }
    except Exception as error:
        assistant = {
            "role": "assistant",
            "content": (
                "I could not reach the certified data service. "
                f"Please try again shortly. Technical detail: `{type(error).__name__}`"
            ),
            "response": {},
        }
    st.session_state.messages.append(assistant)


st.set_page_config(
    page_title="NovaRetail Copilot",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp { background: linear-gradient(140deg, #f7f9ff 0%, #ffffff 45%, #f1f7ff 100%); }
    [data-testid="stSidebar"] { background: #111827; }
    [data-testid="stSidebar"] * { color: #f9fafb; }
    .nr-hero { padding: 1.2rem 1.4rem; border-radius: 18px; color: white;
      background: linear-gradient(115deg, #16213e 0%, #275dad 62%, #16a085 100%);
      box-shadow: 0 12px 28px rgba(39, 93, 173, .18); margin-bottom: 1rem; }
    .nr-hero h1 { margin: 0; font-size: 2rem; }
    .nr-hero p { margin: .35rem 0 0; color: #e8f1ff; }
    .nr-badge { display:inline-block; padding:.25rem .6rem; border-radius:999px;
      background:#dcfce7; color:#166534; font-size:.78rem; font-weight:700; }
    [data-testid="stChatMessage"] { border: 1px solid #e5e7eb; border-radius: 16px;
      padding: .45rem .7rem; background: rgba(255,255,255,.88); }
    .stButton > button { border-radius: 999px; border-color: #bfd1ef; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.title("NovaRetail")
    st.caption("Retail Intelligence Copilot")
    st.markdown('<span class="nr-badge">● Governed & read-only</span>', unsafe_allow_html=True)
    st.divider()
    st.write("**Connected catalog**")
    st.code(CATALOG, language=None)
    st.write("**Evidence routes**")
    st.write("• Certified Gold KPIs\n\n• Data-arrival monitoring\n\n• Governed policy documents")
    st.divider()
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

st.markdown(
    """
    <div class="nr-hero">
      <h1>Retail Intelligence Copilot</h1>
      <p>Ask about sales, KPIs, inventory, data arrival, returns, and operating procedures.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if "messages" not in st.session_state:
    st.session_state.messages = []

if not st.session_state.messages:
    st.info("Ask a question below or choose a suggested question to begin.")
    columns = st.columns(2)
    selected: str | None = None
    for index, suggestion in enumerate(SUGGESTIONS):
        if columns[index % 2].button(
            suggestion, key=f"suggestion-{index}", use_container_width=True
        ):
            selected = suggestion
else:
    selected = None

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            render_response(message)
        else:
            st.markdown(str(message["content"]))

question = st.chat_input("Ask the Retail Intelligence Copilot...")
prompt = question or selected
if prompt:
    submit_question(prompt.strip())
    st.rerun()
