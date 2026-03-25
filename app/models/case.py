from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import Field

from app.models.common import DwellioBaseModel, JsonDict


class ProtestCaseCreate(DwellioBaseModel):
    client_id: UUID
    parcel_id: UUID
    tax_year: int
    valuation_run_id: UUID | None = None
    representation_agreement_id: UUID | None = None
    appraisal_district_id: UUID | None = None
    case_status: str = "created"
    workflow_status_code: str | None = "active"


class ProtestCaseNoteCreate(DwellioBaseModel):
    note_text: str = Field(min_length=1)
    note_code: str = "general"
    author_label: str | None = None


class ProtestCaseStatusUpdate(DwellioBaseModel):
    case_status: str = Field(min_length=1)
    workflow_status_code: str | None = None
    reason_text: str | None = None
    changed_by: str | None = None


class EvidencePacketItemCreate(DwellioBaseModel):
    item_type: str = "section"
    section_code: str
    title: str
    body_text: str | None = None
    source_basis: str | None = None
    display_order: int = 100
    metadata_json: JsonDict = Field(default_factory=dict)


class EvidenceCompSetItemCreate(DwellioBaseModel):
    parcel_sale_id: UUID | None = None
    parcel_id: UUID | None = None
    comp_role: str = "supporting"
    comp_rank: int | None = None
    rationale_text: str | None = None
    adjustment_summary_json: JsonDict = Field(default_factory=dict)


class EvidenceCompSetCreate(DwellioBaseModel):
    basis_type: str
    set_label: str
    notes: str | None = None
    metadata_json: JsonDict = Field(default_factory=dict)
    items: list[EvidenceCompSetItemCreate] = Field(default_factory=list)


class EvidencePacketCreate(DwellioBaseModel):
    protest_case_id: UUID
    packet_type: str = "informal"
    packet_status: str = "draft"
    valuation_run_id: UUID | None = None
    storage_path: str | None = None
    packet_json: JsonDict = Field(default_factory=dict)
    items: list[EvidencePacketItemCreate] = Field(default_factory=list)
    comp_sets: list[EvidenceCompSetCreate] = Field(default_factory=list)


class CaseMutationResult(DwellioBaseModel):
    access_scope: str = "internal"
    action: str
    protest_case_id: str | None = None
    evidence_packet_id: str | None = None
    message: str


class AdminCaseSummary(DwellioBaseModel):
    protest_case_id: str
    county_id: str
    parcel_id: str
    account_number: str
    tax_year: int
    case_status: str
    workflow_status_code: str | None = None
    address: str | None = None
    owner_name: str | None = None
    client_id: str | None = None
    client_name: str | None = None
    representation_agreement_id: str | None = None
    valuation_run_id: str | None = None
    packet_count: int = 0
    note_count: int = 0
    hearing_count: int = 0
    latest_outcome_code: str | None = None
    outcome_date: datetime | None = None
    recommendation_code: str | None = None
    expected_tax_savings_point: float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AdminCaseNote(DwellioBaseModel):
    case_note_id: str
    note_text: str
    note_code: str = "general"
    author_label: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AdminCaseAssignment(DwellioBaseModel):
    case_assignment_id: str
    assignee_name: str
    assignee_role: str
    assignment_status: str
    assigned_at: datetime | None = None
    due_at: datetime | None = None
    active_flag: bool
    metadata_json: JsonDict = Field(default_factory=dict)


class AdminHearingSummary(DwellioBaseModel):
    hearing_id: str
    hearing_type_code: str
    hearing_status: str
    scheduled_at: datetime | None = None
    location_text: str | None = None
    hearing_reference: str | None = None
    result_summary: str | None = None


class AdminCaseStatusHistoryEntry(DwellioBaseModel):
    case_status_history_id: str
    workflow_status_code: str | None = None
    case_status: str
    reason_text: str | None = None
    changed_by: str | None = None
    created_at: datetime | None = None


class AdminCaseListResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str | None = None
    tax_year: int | None = None
    case_status: str | None = None
    cases: list[AdminCaseSummary]


class AdminEvidencePacketItem(DwellioBaseModel):
    evidence_packet_item_id: str
    item_type: str
    section_code: str
    title: str
    body_text: str | None = None
    source_basis: str | None = None
    display_order: int
    metadata_json: JsonDict = Field(default_factory=dict)


class AdminEvidenceCompSetItem(DwellioBaseModel):
    evidence_comp_set_item_id: str
    parcel_sale_id: str | None = None
    parcel_id: str | None = None
    comp_role: str
    comp_rank: int | None = None
    rationale_text: str | None = None
    adjustment_summary_json: JsonDict = Field(default_factory=dict)


class AdminEvidenceCompSet(DwellioBaseModel):
    evidence_comp_set_id: str
    basis_type: str
    set_label: str
    notes: str | None = None
    metadata_json: JsonDict = Field(default_factory=dict)
    items: list[AdminEvidenceCompSetItem] = Field(default_factory=list)


class AdminEvidencePacketSummary(DwellioBaseModel):
    evidence_packet_id: str
    protest_case_id: str | None = None
    county_id: str
    parcel_id: str
    account_number: str
    tax_year: int
    packet_type: str
    packet_status: str
    valuation_run_id: str | None = None
    address: str | None = None
    case_status: str | None = None
    item_count: int = 0
    comp_set_count: int = 0
    generated_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AdminEvidencePacketListResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str | None = None
    tax_year: int | None = None
    packet_status: str | None = None
    packets: list[AdminEvidencePacketSummary]


class AdminEvidencePacketDetail(DwellioBaseModel):
    access_scope: str = "internal"
    packet: AdminEvidencePacketSummary
    items: list[AdminEvidencePacketItem]
    comp_sets: list[AdminEvidenceCompSet]


class AdminCaseDetail(DwellioBaseModel):
    access_scope: str = "internal"
    case: AdminCaseSummary
    notes: list[AdminCaseNote]
    assignments: list[AdminCaseAssignment]
    hearings: list[AdminHearingSummary]
    status_history: list[AdminCaseStatusHistoryEntry]
    packets: list[AdminEvidencePacketSummary]
