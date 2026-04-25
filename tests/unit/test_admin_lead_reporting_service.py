from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import date

from app.services.admin_lead_reporting import AdminLeadReportingService


class FakeCursor:
    def __init__(self, results: list[object]) -> None:
        self._results = deque(results)
        self.execute_calls: list[tuple[str, list[object] | None]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        normalized_params = list(params) if params is not None else None
        self.execute_calls.append((query, normalized_params))

    def fetchone(self):
        result = self._results.popleft()
        assert isinstance(result, dict) or result is None
        return result

    def fetchall(self):
        result = self._results.popleft()
        assert isinstance(result, list)
        return result


class FakeConnection:
    def __init__(self, results: list[object]) -> None:
        self.cursor_instance = FakeCursor(results)

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def connection_factory(connection: FakeConnection):
    @contextmanager
    def _connection():
        yield connection

    return _connection


def test_admin_lead_reporting_service_lists_leads_with_duplicate_filter(monkeypatch) -> None:
    connection = FakeConnection(
        [
            [
                {
                    "lead_id": "lead-1",
                    "lead_event_id": "event-1",
                    "event_created_at": "2026-04-24T12:00:00+00:00",
                    "county_id": "harris",
                    "account_number": "1001001001001",
                    "requested_tax_year": 2026,
                    "served_tax_year": 2025,
                    "demand_bucket": "quote_ready_demand",
                    "context_status": "quote_ready",
                    "source_channel": "web_quote_funnel",
                    "owner_name": "Alex Example",
                    "tax_year_fallback_applied": True,
                    "tax_year_fallback_reason": "requested_year_unavailable",
                    "email_present": True,
                    "phone_present": False,
                    "contact_consent_to_contact": True,
                    "duplicate_group_key": "harris|1001001001001|2026",
                    "duplicate_group_size": 2,
                }
            ],
            {
                "total_count": 1,
                "quote_ready_count": 1,
                "reachable_unquoted_count": 0,
                "unsupported_county_count": 0,
                "unsupported_property_count": 0,
                "fallback_applied_count": 1,
                "duplicate_group_count": 1,
            },
            [{"demand_bucket": "quote_ready_demand", "lead_count": 1}],
            [
                {
                    "duplicate_group_key": "harris|1001001001001|2026",
                    "latest_lead_id": "lead-1",
                    "county_id": "harris",
                    "account_number": "1001001001001",
                    "requested_tax_year": 2026,
                    "lead_count": 2,
                    "latest_submitted_at": "2026-04-24T12:00:00+00:00",
                    "latest_demand_bucket": "quote_ready_demand",
                    "fallback_present": True,
                    "demand_bucket_count": 1,
                }
            ],
        ]
    )
    monkeypatch.setattr(
        "app.services.admin_lead_reporting.get_connection",
        connection_factory(connection),
    )

    response = AdminLeadReportingService().list_leads(
        county_id="harris",
        requested_tax_year=2026,
        demand_bucket="quote_ready_demand",
        duplicate_only=True,
        quote_ready_only=True,
        submitted_from=date(2026, 4, 1),
        submitted_to=date(2026, 4, 30),
        limit=25,
    )

    assert response.access_scope == "internal"
    assert response.kpi_summary.quote_ready_count == 1
    assert response.kpi_summary.duplicate_group_count == 1
    assert response.leads[0].duplicate_group_size == 2
    assert response.duplicate_groups[0].lead_count == 2

    first_query, first_params = connection.cursor_instance.execute_calls[0]
    assert "county_id = %s" in first_query
    assert "requested_tax_year = %s" in first_query
    assert "demand_bucket = %s" in first_query
    assert "quote_ready = %s" in first_query
    assert "duplicate_group_size > 1" in first_query
    assert first_params == [
        "harris",
        2026,
        "quote_ready_demand",
        True,
        date(2026, 4, 1),
        date(2026, 4, 30),
        25,
    ]


def test_admin_lead_reporting_service_returns_detail_and_duplicate_peers(monkeypatch) -> None:
    connection = FakeConnection(
        [
            {
                "lead_id": "lead-1",
                "lead_event_id": "event-1",
                "event_created_at": "2026-04-24T12:00:00+00:00",
                "county_id": "harris",
                "account_number": "1001001001001",
                "requested_tax_year": 2026,
                "served_tax_year": 2025,
                "demand_bucket": "quote_ready_demand",
                "context_status": "quote_ready",
                "source_channel": "web_quote_funnel",
                "owner_name": "Alex Example",
                "email": "alex@example.com",
                "phone": "7135550101",
                "tax_year_fallback_applied": True,
                "tax_year_fallback_reason": "requested_year_unavailable",
                "email_present": True,
                "phone_present": True,
                "contact_consent_to_contact": True,
                "duplicate_group_key": "harris|1001001001001|2026",
                "duplicate_group_size": 2,
                "county_supported": True,
                "property_supported": True,
                "quote_ready": True,
                "parcel_id": "parcel-1",
                "property_type_code": "sfr",
                "protest_recommendation": "file_protest",
                "expected_tax_savings_point": 975.0,
                "defensible_value_point": 320000.0,
                "anonymous_session_id": "anon-1",
                "funnel_stage": "quote_gate",
                "utm_payload": {"source": "google", "campaign": "spring"},
                "raw_event_payload": {"quote_context": {"status": "quote_ready"}},
            },
            [
                {
                    "lead_id": "lead-0",
                    "event_created_at": "2026-04-23T12:00:00+00:00",
                    "demand_bucket": "reachable_unquoted_demand",
                    "context_status": "missing_quote_ready_row",
                    "served_tax_year": 2026,
                    "tax_year_fallback_applied": False,
                    "source_channel": "web_quote_funnel",
                }
            ],
        ]
    )
    monkeypatch.setattr(
        "app.services.admin_lead_reporting.get_connection",
        connection_factory(connection),
    )

    response = AdminLeadReportingService().get_lead_detail(lead_id="lead-1")

    assert response.lead.lead_id == "lead-1"
    assert response.quote_context.demand_bucket == "quote_ready_demand"
    assert response.attribution.utm_source == "google"
    assert response.duplicate_peers[0].context_status == "missing_quote_ready_row"

    first_query, first_params = connection.cursor_instance.execute_calls[0]
    second_query, second_params = connection.cursor_instance.execute_calls[1]
    assert "lead_id = %s" in first_query
    assert first_params == ["lead-1"]
    assert "duplicate_group_key = %s" in second_query
    assert second_params == ["harris|1001001001001|2026", "lead-1"]
