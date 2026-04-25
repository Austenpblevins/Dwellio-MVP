from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.models.admin import (
    AdminLeadAttributionSnapshot,
    AdminLeadContactSnapshot,
    AdminLeadDetail,
    AdminLeadListResponse,
    AdminLeadQuoteContextSnapshot,
    AdminLeadReportingKpiSummary,
    AdminLeadSummary,
)


def test_admin_lead_routes_require_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/leads")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_lead_detail_route_requires_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/leads/lead-1")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_leads_route_returns_internal_payload(monkeypatch) -> None:
    def stub_get_admin_leads(
        *,
        county_id: str | None = None,
        requested_tax_year: int | None = None,
        served_tax_year: int | None = None,
        demand_bucket: str | None = None,
        fallback_applied: bool | None = None,
        source_channel: str | None = None,
        duplicate_only: bool = False,
        quote_ready_only: bool = False,
        submitted_from=None,
        submitted_to=None,
        limit: int = 50,
    ) -> AdminLeadListResponse:
        assert county_id == "harris"
        assert requested_tax_year == 2026
        assert demand_bucket == "quote_ready_demand"
        assert duplicate_only is True
        assert quote_ready_only is False
        assert limit == 25
        return AdminLeadListResponse(
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            demand_bucket=demand_bucket,
            duplicate_only=duplicate_only,
            quote_ready_only=quote_ready_only,
            limit=limit,
            kpi_summary=AdminLeadReportingKpiSummary(
                total_count=2,
                quote_ready_count=2,
                duplicate_group_count=1,
            ),
            leads=[
                AdminLeadSummary(
                    lead_id="lead-1",
                    lead_event_id="event-1",
                    county_id="harris",
                    account_number="1001001001001",
                    requested_tax_year=2026,
                    served_tax_year=2025,
                    demand_bucket="quote_ready_demand",
                    context_status="quote_ready",
                    duplicate_group_key="harris|1001001001001|2026",
                    duplicate_group_size=2,
                )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_leads", stub_get_admin_leads)

    client = TestClient(app)
    response = client.get(
        "/admin/leads",
        params={
            "county_id": "harris",
            "requested_tax_year": 2026,
            "demand_bucket": "quote_ready_demand",
            "duplicate_only": True,
            "limit": 25,
        },
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["kpi_summary"]["quote_ready_count"] == 2
    assert payload["leads"][0]["duplicate_group_size"] == 2


def test_admin_lead_detail_maps_missing_lead_to_404(monkeypatch) -> None:
    def stub_get_admin_lead_detail(_: str) -> AdminLeadDetail:
        raise LookupError("Missing lead lead-missing.")

    monkeypatch.setattr("app.api.routes.admin.get_admin_lead_detail", stub_get_admin_lead_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/leads/lead-missing",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Missing lead lead-missing."


def test_admin_lead_detail_route_returns_internal_payload(monkeypatch) -> None:
    def stub_get_admin_lead_detail(lead_id: str) -> AdminLeadDetail:
        assert lead_id == "lead-1"
        return AdminLeadDetail(
            lead=AdminLeadSummary(
                lead_id=lead_id,
                lead_event_id="event-1",
                county_id="harris",
                account_number="1001001001001",
                requested_tax_year=2026,
                served_tax_year=2025,
                demand_bucket="quote_ready_demand",
                context_status="quote_ready",
                duplicate_group_key="harris|1001001001001|2026",
                duplicate_group_size=2,
            ),
            contact=AdminLeadContactSnapshot(
                owner_name="Alex Example",
                email="alex@example.com",
                email_present=True,
                phone_present=False,
                consent_to_contact=True,
            ),
            quote_context=AdminLeadQuoteContextSnapshot(
                context_status="quote_ready",
                demand_bucket="quote_ready_demand",
                county_supported=True,
                property_supported=True,
                quote_ready=True,
                requested_tax_year=2026,
                served_tax_year=2025,
                tax_year_fallback_applied=True,
                tax_year_fallback_reason="requested_year_unavailable",
            ),
            attribution=AdminLeadAttributionSnapshot(
                anonymous_session_id="anon-1",
                funnel_stage="quote_gate",
                utm_source="google",
            ),
            raw_event_payload={"quote_context": {"status": "quote_ready"}},
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_lead_detail", stub_get_admin_lead_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/leads/lead-1",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["quote_context"]["tax_year_fallback_applied"] is True
    assert payload["attribution"]["utm_source"] == "google"
