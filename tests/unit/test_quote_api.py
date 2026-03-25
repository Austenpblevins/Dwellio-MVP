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
            requested_tax_year=tax_year,
            served_tax_year=tax_year,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
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
            requested_tax_year=tax_year,
            served_tax_year=tax_year,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
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
    assert payload["requested_tax_year"] == 2026
    assert payload["served_tax_year"] == 2026
    assert payload["tax_year_fallback_applied"] is False


def test_quote_explanation_route_returns_explanation(monkeypatch) -> None:
    monkeypatch.setattr("app.api.quote.QuoteReadService", StubQuoteReadService)

    client = TestClient(app)
    response = client.get("/quote/harris/2026/1001001001001/explanation")

    assert response.status_code == 200
    payload = response.json()
    assert payload["explanation_json"]["basis"] == "market_and_equity"
    assert payload["explanation_bullets"] == ["Comparable evidence supports a lower value."]
    assert payload["served_tax_year"] == 2026


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


def test_quote_read_service_returns_exact_year_without_fallback(monkeypatch) -> None:
    from tests.unit.test_search_services import connection_factory

    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "requested_tax_year": 2026,
            "served_tax_year": 2026,
            "tax_year_fallback_applied": False,
            "tax_year_fallback_reason": None,
            "data_freshness_label": "current_year",
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "defensible_value_point": 320000,
            "explanation_json": {},
            "explanation_bullets": [],
        }
    ]
    monkeypatch.setattr("app.services.quote_read.get_connection", connection_factory(rows))

    from app.services.quote_read import QuoteReadService

    response = QuoteReadService().get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.tax_year == 2026
    assert response.requested_tax_year == 2026
    assert response.served_tax_year == 2026
    assert response.tax_year_fallback_applied is False


def test_quote_read_service_applies_prior_year_fallback(monkeypatch) -> None:
    from tests.unit.test_search_services import connection_factory

    rows = [
        {
            "county_id": "harris",
            "tax_year": 2025,
            "requested_tax_year": 2026,
            "served_tax_year": 2025,
            "tax_year_fallback_applied": True,
            "tax_year_fallback_reason": "requested_year_unavailable",
            "data_freshness_label": "prior_year_fallback",
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "defensible_value_point": 315000,
            "explanation_json": {},
            "explanation_bullets": [],
        }
    ]
    monkeypatch.setattr("app.services.quote_read.get_connection", connection_factory(rows))

    from app.services.quote_read import QuoteReadService

    response = QuoteReadService().get_quote(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert response.tax_year == 2025
    assert response.requested_tax_year == 2026
    assert response.served_tax_year == 2025
    assert response.tax_year_fallback_applied is True
    assert response.tax_year_fallback_reason == "requested_year_unavailable"
    assert response.data_freshness_label == "prior_year_fallback"


def test_quote_read_service_queries_nearest_prior_year(monkeypatch) -> None:
    from tests.unit.test_search_services import FakeConnection
    from contextlib import contextmanager

    connection = FakeConnection(
        [
            {
                "county_id": "harris",
                "tax_year": 2025,
                "requested_tax_year": 2026,
                "served_tax_year": 2025,
                "tax_year_fallback_applied": True,
                "tax_year_fallback_reason": "requested_year_unavailable",
                "data_freshness_label": "prior_year_fallback",
                "account_number": "1001001001001",
                "parcel_id": uuid4(),
                "address": "101 Main St, Houston, TX 77002",
                "defensible_value_point": 315000,
                "explanation_json": {},
                "explanation_bullets": [],
            }
        ]
    )

    @contextmanager
    def _connection():
        yield connection

    monkeypatch.setattr("app.services.quote_read.get_connection", _connection)

    from app.services.quote_read import QuoteReadService

    QuoteReadService().get_quote(county_id="harris", tax_year=2026, account_number="1001001001001")

    sql, params = connection.cursor_instance.execute_calls[0]
    assert "tax_year <= %s" in sql
    assert "ORDER BY tax_year DESC, valuation_created_at DESC NULLS LAST" in sql
    assert params[-3:] == ("harris", "1001001001001", 2026)
