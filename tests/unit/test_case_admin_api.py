from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.models.case import (
    AdminCaseDetail,
    AdminCaseListResponse,
    AdminCaseNote,
    AdminCaseStatusHistoryEntry,
    AdminCaseSummary,
    AdminEvidenceCompSet,
    AdminEvidenceCompSetItem,
    AdminEvidencePacketDetail,
    AdminEvidencePacketItem,
    AdminEvidencePacketListResponse,
    AdminEvidencePacketSummary,
    CaseMutationResult,
)


def test_admin_case_routes_require_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/cases")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_packet_routes_require_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/packets")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_case_detail_requires_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/cases/case-1")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_packet_detail_requires_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/packets/packet-1")

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_cases_route_returns_internal_payload(monkeypatch) -> None:
    def stub_get_cases(
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        case_status: str | None = None,
        limit: int = 50,
    ) -> AdminCaseListResponse:
        assert county_id == "harris"
        assert tax_year == 2026
        assert case_status == "active"
        assert limit == 25
        return AdminCaseListResponse(
            county_id=county_id,
            tax_year=tax_year,
            case_status=case_status,
            cases=[
                AdminCaseSummary(
                    protest_case_id="case-1",
                    county_id="harris",
                    parcel_id=str(uuid4()),
                    account_number="1001001001001",
                    tax_year=2026,
                    case_status="active",
                    workflow_status_code="packet_review",
                    address="101 Main St, Houston, TX 77002",
                    owner_name="A. Example",
                    client_id=str(uuid4()),
                    client_name="Alex Example",
                    representation_agreement_id=str(uuid4()),
                    valuation_run_id=str(uuid4()),
                    packet_count=1,
                    note_count=2,
                    hearing_count=0,
                    recommendation_code="file_protest",
                    expected_tax_savings_point=975.0,
                )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_cases", stub_get_cases)

    client = TestClient(app)
    response = client.get(
        "/admin/cases",
        params={"county_id": "harris", "tax_year": 2026, "case_status": "active", "limit": 25},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["cases"][0]["workflow_status_code"] == "packet_review"


def test_admin_case_detail_maps_missing_case_to_404(monkeypatch) -> None:
    def stub_detail(_: str) -> AdminCaseDetail:
        raise LookupError("Missing protest case case-missing.")

    monkeypatch.setattr("app.api.routes.admin.get_admin_case_detail", stub_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/cases/case-missing",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Missing protest case case-missing."


def test_admin_case_note_route_uses_internal_mutation(monkeypatch) -> None:
    def stub_note(case_id: str, request) -> CaseMutationResult:
        assert case_id == "case-1"
        assert request.note_code == "review"
        return CaseMutationResult(
            action="add_case_note",
            protest_case_id=case_id,
            message="Added note.",
        )

    monkeypatch.setattr("app.api.routes.admin.post_admin_case_note", stub_note)

    client = TestClient(app)
    response = client.post(
        "/admin/cases/case-1/notes",
        json={"note_text": "Need comp review", "note_code": "review", "author_label": "Analyst"},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    assert response.json()["action"] == "add_case_note"


def test_admin_packet_routes_return_internal_payload(monkeypatch) -> None:
    def stub_packets(
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        packet_status: str | None = None,
        limit: int = 50,
    ) -> AdminEvidencePacketListResponse:
        assert county_id == "harris"
        assert tax_year == 2026
        assert packet_status == "draft"
        assert limit == 20
        return AdminEvidencePacketListResponse(
            county_id=county_id,
            tax_year=tax_year,
            packet_status=packet_status,
            packets=[
                AdminEvidencePacketSummary(
                    evidence_packet_id="packet-1",
                    protest_case_id="case-1",
                    county_id="harris",
                    parcel_id=str(uuid4()),
                    account_number="1001001001001",
                    tax_year=2026,
                    packet_type="informal",
                    packet_status="draft",
                    valuation_run_id=str(uuid4()),
                    address="101 Main St, Houston, TX 77002",
                    case_status="active",
                    item_count=2,
                    comp_set_count=1,
                )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_packets", stub_packets)

    client = TestClient(app)
    response = client.get(
        "/admin/packets",
        params={"county_id": "harris", "tax_year": 2026, "packet_status": "draft", "limit": 20},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["packets"][0]["comp_set_count"] == 1


def test_admin_packet_detail_route_returns_comp_sets(monkeypatch) -> None:
    def stub_packet_detail(packet_id: str) -> AdminEvidencePacketDetail:
        assert packet_id == "packet-1"
        return AdminEvidencePacketDetail(
            packet=AdminEvidencePacketSummary(
                evidence_packet_id=packet_id,
                protest_case_id="case-1",
                county_id="harris",
                parcel_id=str(uuid4()),
                account_number="1001001001001",
                tax_year=2026,
                packet_type="informal",
                packet_status="draft",
                address="101 Main St, Houston, TX 77002",
                case_status="active",
                item_count=1,
                comp_set_count=1,
            ),
            items=[
                AdminEvidencePacketItem(
                    evidence_packet_item_id="item-1",
                    item_type="section",
                    section_code="value_summary",
                    title="Value summary",
                    body_text="Ready for analyst review.",
                    source_basis="quote_read_model",
                    display_order=10,
                )
            ],
            comp_sets=[
                AdminEvidenceCompSet(
                    evidence_comp_set_id="set-1",
                    basis_type="equity",
                    set_label="Equity support set",
                    items=[
                        AdminEvidenceCompSetItem(
                            evidence_comp_set_item_id="comp-1",
                            parcel_sale_id=str(uuid4()),
                            parcel_id=str(uuid4()),
                            comp_role="supporting",
                            comp_rank=1,
                            rationale_text="Neighborhood peer",
                        )
                    ],
                )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_packet_detail", stub_packet_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/packets/packet-1",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["comp_sets"][0]["basis_type"] == "equity"
    assert payload["items"][0]["section_code"] == "value_summary"


def test_admin_case_detail_route_returns_notes_history_and_packets(monkeypatch) -> None:
    def stub_case_detail(case_id: str) -> AdminCaseDetail:
        assert case_id == "case-1"
        return AdminCaseDetail(
            case=AdminCaseSummary(
                protest_case_id=case_id,
                county_id="harris",
                parcel_id=str(uuid4()),
                account_number="1001001001001",
                tax_year=2026,
                case_status="active",
                workflow_status_code="packet_review",
                address="101 Main St, Houston, TX 77002",
                owner_name="A. Example",
            ),
            notes=[
                AdminCaseNote(
                    case_note_id="note-1",
                    note_text="Packet needs valuation summary copy.",
                    note_code="review",
                    author_label="Analyst",
                )
            ],
            assignments=[],
            hearings=[],
            status_history=[
                AdminCaseStatusHistoryEntry(
                    case_status_history_id="history-1",
                    workflow_status_code="packet_review",
                    case_status="active",
                    reason_text="packet rows assembled",
                )
            ],
            packets=[
                AdminEvidencePacketSummary(
                    evidence_packet_id="packet-1",
                    protest_case_id=case_id,
                    county_id="harris",
                    parcel_id=str(uuid4()),
                    account_number="1001001001001",
                    tax_year=2026,
                    packet_type="informal",
                    packet_status="draft",
                    item_count=1,
                    comp_set_count=1,
                )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_admin_case_detail", stub_case_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/cases/case-1",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["notes"][0]["note_code"] == "review"
    assert payload["status_history"][0]["workflow_status_code"] == "packet_review"
    assert payload["packets"][0]["packet_type"] == "informal"
