from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.quote import QuoteExplanationResponse, QuoteResponse


class StubQuoteReadService:
    def get_quote(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteResponse:
        assert county_id == "harris"
        assert tax_year == 2026
        assert account_number == "1001001001001"
        return QuoteResponse(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
            parcel_id=uuid4(),
            address="101 Main St, Houston, TX 77002",
            current_notice_value=350000,
            defensible_value_point=320000,
            expected_tax_savings_point=975,
            protest_recommendation="file_protest",
            explanation_bullets=["Comparable evidence supports a lower value."],
        )

    def get_explanation(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteExplanationResponse:
        assert county_id == "harris"
        assert tax_year == 2026
        assert account_number == "1001001001001"
        return QuoteExplanationResponse(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
            explanation_json={"basis": "market_and_equity"},
            explanation_bullets=["Comparable evidence supports a lower value."],
        )


def test_get_quote_uses_quote_read_service(monkeypatch) -> None:
    monkeypatch.setattr("app.api.quote.QuoteReadService", StubQuoteReadService)

    from app.api.quote import get_quote

    response = get_quote("harris", 2026, "1001001001001")

    assert response.defensible_value_point == 320000
    assert response.protest_recommendation == "file_protest"


def test_quote_routes_return_public_safe_payload(monkeypatch) -> None:
    monkeypatch.setattr("app.api.quote.QuoteReadService", StubQuoteReadService)

    client = TestClient(app)
    response = client.get("/quote/harris/2026/1001001001001")

    assert response.status_code == 200
    payload = response.json()
    assert payload["county_id"] == "harris"
    assert payload["defensible_value_point"] == 320000
    assert payload["protest_recommendation"] == "file_protest"


def test_quote_explanation_route_returns_explanation(monkeypatch) -> None:
    monkeypatch.setattr("app.api.quote.QuoteReadService", StubQuoteReadService)

    client = TestClient(app)
    response = client.get("/quote/harris/2026/1001001001001/explanation")

    assert response.status_code == 200
    payload = response.json()
    assert payload["explanation_json"]["basis"] == "market_and_equity"
    assert payload["explanation_bullets"] == ["Comparable evidence supports a lower value."]


@pytest.mark.parametrize(
    ("path", "expected_detail"),
    [
        ("/quote/harris/2026/missing", "missing"),
        ("/quote/harris/2026/missing/explanation", "missing"),
    ],
)
def test_quote_routes_map_missing_quote_rows_to_404(monkeypatch, path: str, expected_detail: str) -> None:
    class MissingQuoteService:
        def get_quote(self, *, county_id: str, tax_year: int, account_number: str) -> QuoteResponse:
            raise LookupError("missing")

        def get_explanation(
            self, *, county_id: str, tax_year: int, account_number: str
        ) -> QuoteExplanationResponse:
            raise LookupError("missing")

    monkeypatch.setattr("app.api.quote.QuoteReadService", MissingQuoteService)

    client = TestClient(app)
    response = client.get(path)

    assert response.status_code == 404
    assert response.json()["detail"] == expected_detail


def test_quote_read_service_raises_when_missing(monkeypatch) -> None:
    from tests.unit.test_search_services import connection_factory

    monkeypatch.setattr("app.services.quote_read.get_connection", connection_factory([]))

    from app.services.quote_read import QuoteReadService

    with pytest.raises(LookupError):
        QuoteReadService().get_quote(county_id="harris", tax_year=2026, account_number="missing")
