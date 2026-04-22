from __future__ import annotations

import json
from collections import deque
from contextlib import contextmanager
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.models.lead import LeadCreateRequest, LeadCreateResponse
from app.services.lead_capture import LeadCaptureService


class FakeCursor:
    def __init__(self, fetchone_results: list[dict[str, object] | None]) -> None:
        self._fetchone_results = deque(fetchone_results)
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((query, params))

    def fetchone(self) -> dict[str, object] | None:
        if not self._fetchone_results:
            return None
        return self._fetchone_results.popleft()


class FakeConnection:
    def __init__(self, fetchone_results: list[dict[str, object] | None]) -> None:
        self.cursor_instance = FakeCursor(fetchone_results)
        self.commit_calls = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1


def connection_sequence_factory(connections: list[FakeConnection]):
    remaining = deque(connections)

    @contextmanager
    def _connection():
        if not remaining:
            raise AssertionError("Unexpected extra database connection.")
        yield remaining.popleft()

    return _connection


def test_lead_capture_service_persists_attribution_and_quote_context(monkeypatch) -> None:
    parcel_id = uuid4()
    lead_id = uuid4()
    parcel_connection = FakeConnection(
        [{"parcel_id": parcel_id, "tax_year": 2025, "property_type_code": "sfr"}]
    )
    quote_connection = FakeConnection(
        [
            {
                "parcel_id": parcel_id,
                "tax_year": 2025,
                "protest_recommendation": "file_protest",
                "expected_tax_savings_point": 975.0,
                "defensible_value_point": 320000.0,
            }
        ]
    )
    insert_connection = FakeConnection([{"lead_id": lead_id}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory(
            [
                parcel_connection,
                quote_connection,
                insert_connection,
            ]
        ),
    )

    response = LeadCaptureService().create_lead(
        LeadCreateRequest(
            county_id="harris",
            tax_year=2026,
            account_number="1001001001001",
            owner_name="Alex Example",
            email="alex@example.com",
            phone="7135550101",
            consent_to_contact=True,
            source_channel="quote_funnel",
            anonymous_session_id="anon-123",
            funnel_stage="quote",
            utm_source="google",
            utm_medium="cpc",
            utm_campaign="spring",
            utm_term="property tax",
            utm_content="hero",
        )
    )

    assert response.status == "accepted"
    assert response.lead_id == lead_id
    assert response.context_status == "quote_ready"
    assert response.quote_ready is True
    assert response.county_supported is True
    assert response.property_supported is True
    assert response.parcel_id == parcel_id
    assert response.requested_tax_year == 2026
    assert response.served_tax_year == 2025
    assert response.tax_year_fallback_applied is True
    assert response.tax_year_fallback_reason == "requested_year_unavailable"

    insert_sql, insert_params = insert_connection.cursor_instance.execute_calls[0]
    assert "INSERT INTO leads" in insert_sql
    assert insert_params == (
        parcel_id,
        "harris",
        2026,
        "1001001001001",
        "Alex Example",
        "alex@example.com",
        "7135550101",
        "quote_funnel",
        True,
    )

    event_sql, event_params = insert_connection.cursor_instance.execute_calls[1]
    assert "INSERT INTO lead_events" in event_sql
    assert event_params is not None
    assert event_params[1] == "lead_submitted"
    event_payload = json.loads(event_params[2])
    assert event_payload["anonymous_session_id"] == "anon-123"
    assert event_payload["funnel_stage"] == "quote"
    assert event_payload["utm"]["source"] == "google"
    assert event_payload["quote_context"]["status"] == "quote_ready"
    assert event_payload["quote_context"]["requested_tax_year"] == 2026
    assert event_payload["quote_context"]["served_tax_year"] == 2025
    assert event_payload["quote_context"]["tax_year_fallback_applied"] is True
    assert event_payload["quote_context"]["property_type_code"] == "sfr"
    assert event_payload["quote_context"]["protest_recommendation"] == "file_protest"
    assert insert_connection.commit_calls == 1


def test_lead_capture_service_accepts_unsupported_county_without_quote_lookup(monkeypatch) -> None:
    lead_id = uuid4()
    insert_connection = FakeConnection([{"lead_id": lead_id}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory([insert_connection]),
    )

    response = LeadCaptureService().create_lead(
        LeadCreateRequest(
            county_id="dallas",
            tax_year=2026,
            account_number="1001001001001",
            email="alex@example.com",
        )
    )

    assert response.context_status == "unsupported_county"
    assert response.county_supported is False
    assert response.property_supported is None
    assert response.quote_ready is False
    assert response.parcel_id is None
    assert len(insert_connection.cursor_instance.execute_calls) == 2

    event_payload = json.loads(insert_connection.cursor_instance.execute_calls[1][1][2])
    assert event_payload["quote_context"]["status"] == "unsupported_county"
    assert event_payload["quote_context"]["county_supported"] is False


def test_lead_capture_service_marks_unsupported_property_type(monkeypatch) -> None:
    parcel_id = uuid4()
    lead_id = uuid4()
    parcel_connection = FakeConnection(
        [{"parcel_id": parcel_id, "tax_year": 2026, "property_type_code": "condo"}]
    )
    insert_connection = FakeConnection([{"lead_id": lead_id}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory([parcel_connection, insert_connection]),
    )

    response = LeadCaptureService().create_lead(
        LeadCreateRequest(
            county_id="harris",
            tax_year=2026,
            account_number="1001001001002",
        )
    )

    assert response.context_status == "unsupported_property_type"
    assert response.property_supported is False
    assert response.quote_ready is False
    assert response.parcel_id == parcel_id


def test_lead_capture_service_marks_missing_quote_ready_row(monkeypatch) -> None:
    parcel_id = uuid4()
    lead_id = uuid4()
    parcel_connection = FakeConnection(
        [{"parcel_id": parcel_id, "tax_year": 2026, "property_type_code": "sfr"}]
    )
    quote_connection = FakeConnection([None])
    insert_connection = FakeConnection([{"lead_id": lead_id}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory(
            [
                parcel_connection,
                quote_connection,
                insert_connection,
            ]
        ),
    )

    response = LeadCaptureService().create_lead(
        LeadCreateRequest(
            county_id="harris",
            tax_year=2026,
            account_number="1001001001003",
        )
    )

    assert response.context_status == "missing_quote_ready_row"
    assert response.county_supported is True
    assert response.property_supported is True
    assert response.quote_ready is False
    assert response.parcel_id == parcel_id


def test_lead_route_returns_structured_contract_response(monkeypatch) -> None:
    lead_id = uuid4()

    class StubLeadCaptureService:
        def create_lead(self, request: LeadCreateRequest) -> LeadCreateResponse:
            assert request.county_id == "harris"
            assert request.anonymous_session_id == "anon-123"
            return LeadCreateResponse(
                lead_id=lead_id,
                context_status="quote_ready",
                county_supported=True,
                property_supported=True,
                quote_ready=True,
                parcel_id=uuid4(),
                requested_tax_year=2026,
                served_tax_year=2026,
                tax_year_fallback_applied=False,
                tax_year_fallback_reason=None,
            )

    monkeypatch.setattr("app.api.leads.LeadCaptureService", StubLeadCaptureService)

    client = TestClient(app)
    response = client.post(
        "/lead",
        json={
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "email": "alex@example.com",
            "anonymous_session_id": "anon-123",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["lead_id"] == str(lead_id)
    assert payload["context_status"] == "quote_ready"
    assert payload["quote_ready"] is True


def test_canonical_public_routes_remain_registered() -> None:
    route_paths = {route.path for route in app.routes}

    assert "/healthz" in route_paths
    assert "/search" in route_paths
    assert "/search/autocomplete" in route_paths
    assert "/parcel/{county_id}/{tax_year}/{account_number}" in route_paths
    assert "/quote/instant/{county_id}/{tax_year}/{account_number}" in route_paths
    assert "/quote/{county_id}/{tax_year}/{account_number}" in route_paths
    assert "/quote/{county_id}/{tax_year}/{account_number}/explanation" in route_paths
    assert "/lead" in route_paths
