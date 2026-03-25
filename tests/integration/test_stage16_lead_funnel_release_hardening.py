from __future__ import annotations

import json
from collections import deque
from contextlib import contextmanager
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


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


def test_lead_route_persists_attribution_payload_evidence(monkeypatch) -> None:
    parcel_id = uuid4()
    lead_id = uuid4()
    parcel_connection = FakeConnection(
        [{"parcel_id": parcel_id, "tax_year": 2026, "property_type_code": "sfr"}]
    )
    quote_connection = FakeConnection(
        [
            {
                "parcel_id": parcel_id,
                "tax_year": 2026,
                "protest_recommendation": "file_protest",
                "expected_tax_savings_point": 975.0,
                "defensible_value_point": 320000.0,
            }
        ]
    )
    insert_connection = FakeConnection([{"lead_id": lead_id}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory([parcel_connection, quote_connection, insert_connection]),
    )

    client = TestClient(app)
    response = client.post(
        "/lead",
        json={
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "owner_name": "Alex Example",
            "email": "alex@example.com",
            "source_channel": "web_quote_funnel",
            "anonymous_session_id": "release-anon-1",
                "funnel_stage": "quote_gate",
                "utm_source": "google",
                "utm_medium": "cpc",
                "utm_campaign": "release-hardening",
                "consent_to_contact": True,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["context_status"] == "quote_ready"
    assert payload["quote_ready"] is True
    assert payload["lead_id"] == str(lead_id)

    event_sql, event_params = insert_connection.cursor_instance.execute_calls[1]
    assert "INSERT INTO lead_events" in event_sql
    assert event_params is not None
    event_payload = json.loads(event_params[2])
    assert event_payload["anonymous_session_id"] == "release-anon-1"
    assert event_payload["funnel_stage"] == "quote_gate"
    assert event_payload["utm"] == {
        "source": "google",
        "medium": "cpc",
        "campaign": "release-hardening",
        "term": None,
        "content": None,
    }
    assert event_payload["quote_context"]["status"] == "quote_ready"
    assert event_payload["quote_context"]["county_id"] == "harris"
    assert event_payload["quote_context"]["account_number"] == "1001001001001"


def test_lead_route_accepts_unsupported_and_missing_quote_contexts(monkeypatch) -> None:
    unsupported_insert = FakeConnection([{"lead_id": uuid4()}])
    missing_parcel_id = uuid4()
    missing_quote_parcel_connection = FakeConnection(
        [{"parcel_id": missing_parcel_id, "tax_year": 2026, "property_type_code": "sfr"}]
    )
    missing_quote_connection = FakeConnection([None])
    missing_quote_insert = FakeConnection([{"lead_id": uuid4()}])

    monkeypatch.setattr(
        "app.services.lead_capture.get_connection",
        connection_sequence_factory(
            [
                unsupported_insert,
                missing_quote_parcel_connection,
                missing_quote_connection,
                missing_quote_insert,
            ]
        ),
    )

    client = TestClient(app)

    unsupported_response = client.post(
        "/lead",
        json={
            "county_id": "dallas",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "email": "alex@example.com",
        },
    )
    assert unsupported_response.status_code == 200
    unsupported_payload = unsupported_response.json()
    assert unsupported_payload["context_status"] == "unsupported_county"
    assert unsupported_payload["county_supported"] is False
    assert unsupported_payload["quote_ready"] is False

    missing_quote_response = client.post(
        "/lead",
        json={
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001002",
            "email": "alex@example.com",
        },
    )
    assert missing_quote_response.status_code == 200
    missing_quote_payload = missing_quote_response.json()
    assert missing_quote_payload["context_status"] == "missing_quote_ready_row"
    assert missing_quote_payload["county_supported"] is True
    assert missing_quote_payload["property_supported"] is True
    assert missing_quote_payload["quote_ready"] is False
    assert missing_quote_payload["parcel_id"] == str(missing_parcel_id)
