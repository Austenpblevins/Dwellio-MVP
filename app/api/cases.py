from __future__ import annotations

from app.models.case import (
    AdminCaseDetail,
    AdminCaseListResponse,
    AdminEvidencePacketDetail,
    AdminEvidencePacketListResponse,
    CaseMutationResult,
    EvidencePacketCreate,
    ProtestCaseCreate,
    ProtestCaseNoteCreate,
    ProtestCaseStatusUpdate,
)
from app.services.case_ops import CaseOpsService


def get_admin_cases(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    case_status: str | None = None,
    limit: int = 50,
) -> AdminCaseListResponse:
    return CaseOpsService().list_cases(
        county_id=county_id,
        tax_year=tax_year,
        case_status=case_status,
        limit=limit,
    )


def get_admin_case_detail(protest_case_id: str) -> AdminCaseDetail:
    return CaseOpsService().get_case_detail(protest_case_id=protest_case_id)


def post_admin_case(request: ProtestCaseCreate) -> CaseMutationResult:
    return CaseOpsService().create_case(request=request)


def post_admin_case_note(
    protest_case_id: str,
    request: ProtestCaseNoteCreate,
) -> CaseMutationResult:
    return CaseOpsService().add_case_note(protest_case_id=protest_case_id, request=request)


def post_admin_case_status(
    protest_case_id: str,
    request: ProtestCaseStatusUpdate,
) -> CaseMutationResult:
    return CaseOpsService().update_case_status(protest_case_id=protest_case_id, request=request)


def get_admin_packets(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    packet_status: str | None = None,
    limit: int = 50,
) -> AdminEvidencePacketListResponse:
    return CaseOpsService().list_packets(
        county_id=county_id,
        tax_year=tax_year,
        packet_status=packet_status,
        limit=limit,
    )


def get_admin_packet_detail(evidence_packet_id: str) -> AdminEvidencePacketDetail:
    return CaseOpsService().get_packet_detail(evidence_packet_id=evidence_packet_id)


def post_admin_packet(request: EvidencePacketCreate) -> CaseMutationResult:
    return CaseOpsService().create_packet(request=request)
