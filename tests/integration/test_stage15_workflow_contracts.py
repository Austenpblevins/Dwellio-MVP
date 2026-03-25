from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.models.case import CaseMutationResult
from app.models.lead import LeadCreateResponse

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "stage15_workflow_samples.json"


def load_workflow_samples() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text())


def test_lead_route_accepts_fixture_contract() -> None:
    lead_id = uuid4()

    def stub_create_lead(request) -> LeadCreateResponse:
        assert request.county_id == "harris"
        assert request.tax_year == 2026
        assert request.account_number == "1001001001001"
        assert request.owner_name == "Alex Example"
        assert request.email == "alex@example.com"
        assert request.phone == "7135550101"
        assert request.consent_to_contact is True
        assert request.source_channel == "web_quote_funnel"
        assert request.anonymous_session_id == "fixture-anon-1"
        assert request.funnel_stage == "quote_gate"
        assert request.utm_source == "google"
        assert request.utm_medium == "cpc"
        assert request.utm_campaign == "stage15_fixture"
        return LeadCreateResponse(
            lead_id=lead_id,
            context_status="quote_ready",
            county_supported=True,
            property_supported=True,
            quote_ready=True,
            parcel_id=None,
            requested_tax_year=2026,
            served_tax_year=2026,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
        )

    from app.api.routes import leads as lead_routes

    original_create_lead = lead_routes.create_lead
    lead_routes.create_lead = stub_create_lead
    client = TestClient(app)
    try:
        payload = load_workflow_samples()["lead_request"]
        response = client.post("/lead", json=payload)
    finally:
        lead_routes.create_lead = original_create_lead

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert response.json()["lead_id"] == str(lead_id)
    assert response.json()["context_status"] == "quote_ready"


def test_admin_case_and_packet_routes_accept_fixture_contracts(monkeypatch) -> None:
    samples = load_workflow_samples()

    def stub_create_case(request) -> CaseMutationResult:
        assert str(request.client_id) == samples["case_create_request"]["client_id"]
        assert str(request.parcel_id) == samples["case_create_request"]["parcel_id"]
        assert request.tax_year == 2026
        return CaseMutationResult(
            action="create_protest_case",
            protest_case_id="case-fixture-1",
            message="Created protest case.",
        )

    def stub_create_packet(request) -> CaseMutationResult:
        assert str(request.protest_case_id) == samples["packet_create_request"]["protest_case_id"]
        assert request.packet_type == "informal"
        assert request.items[0].section_code == "value_summary"
        assert request.comp_sets[0].items[0].comp_role == "supporting"
        return CaseMutationResult(
            action="create_evidence_packet",
            evidence_packet_id="packet-fixture-1",
            message="Created evidence packet.",
        )

    monkeypatch.setattr("app.api.routes.admin.post_admin_case", stub_create_case)
    monkeypatch.setattr("app.api.routes.admin.post_admin_packet", stub_create_packet)

    client = TestClient(app)

    case_response = client.post(
        "/admin/cases",
        json=samples["case_create_request"],
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )
    packet_response = client.post(
        "/admin/packets",
        json=samples["packet_create_request"],
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert case_response.status_code == 200
    assert case_response.json()["action"] == "create_protest_case"
    assert case_response.json()["access_scope"] == "internal"

    assert packet_response.status_code == 200
    assert packet_response.json()["action"] == "create_evidence_packet"
    assert packet_response.json()["access_scope"] == "internal"
