from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.models.case import CaseMutationResult

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "stage15_workflow_samples.json"


def load_workflow_samples() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text())


def test_lead_route_currently_returns_501_for_unwired_workflow() -> None:
    client = TestClient(app)
    payload = load_workflow_samples()["lead_request"]

    response = client.post("/lead", json=payload)

    assert response.status_code == 501
    assert response.json()["detail"] == "Wire to lead creation workflow"


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
