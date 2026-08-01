from retail_lakehouse.rag.models import Citation, CopilotResponse
from retail_lakehouse.rag.presentation import present_response


def _citation(template_id: str) -> Citation:
    return Citation(
        "SQL1", f"Certified query: {template_id}", "Certified", "gold://table", "SELECT"
    )


def test_arrival_presentation_reports_all_checks() -> None:
    response = CopilotResponse(
        answer="raw",
        route="sql",
        citations=(_citation("data_arrival"),),
        rows=(
            {"arrival_status": "ARRIVED"},
            {"arrival_status": "ARRIVED"},
        ),
    )

    assert (
        present_response(response)
        == "All **2 critical data checks are ARRIVED**. The lakehouse is ready."
    )


def test_daily_sales_presentation_uses_latest_date() -> None:
    response = CopilotResponse(
        answer="raw",
        route="sql",
        citations=(_citation("daily_sales"),),
        rows=(
            {
                "date_key": 20251230,
                "channel": "WEB",
                "net_sales": "1000.5",
                "average_order_value": "250.0",
            },
            {
                "date_key": 20251229,
                "channel": "STORE",
                "net_sales": "900",
                "average_order_value": "225",
            },
        ),
    )

    answer = present_response(response)

    assert "20251230" in answer
    assert "WEB: net sales 1,000.50, AOV 250.00" in answer


def test_document_and_refusal_answers_are_preserved() -> None:
    document = CopilotResponse(answer="Document answer [D1]", route="documents")
    refusal = CopilotResponse(answer="Cannot answer", route="documents", refused=True)

    assert present_response(document) == "Document answer [D1]"
    assert present_response(refusal) == "Cannot answer"
