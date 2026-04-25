from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.admin import (
    get_admin_lead_detail,
    get_admin_leads,
    get_completeness_issues,
    get_county_onboarding_contract,
    get_county_year_readiness,
    get_import_batch_detail,
    get_import_batches,
    get_scalability_bottleneck_review,
    get_search_inspection,
    get_source_files,
    get_tax_assignment_issues,
    get_validation_results,
    post_manual_import_register,
    post_publish_import_batch,
    post_retry_import_batch_maintenance,
    post_rollback_import_batch,
)
from app.api.cases import (
    get_admin_case_detail,
    get_admin_cases,
    get_admin_packet_detail,
    get_admin_packets,
    post_admin_case,
    post_admin_case_note,
    post_admin_case_status,
    post_admin_packet,
)
from app.api.deps.admin_auth import require_admin_access
from app.models.admin import (
    AdminLeadDetail,
    AdminLeadListResponse,
    AdminCompletenessIssuesResponse,
    AdminCountyOnboardingContract,
    AdminCountyYearReadinessDashboard,
    AdminImportBatchActionRequest,
    AdminImportBatchDetail,
    AdminImportBatchListResponse,
    AdminManualImportRequest,
    AdminMutationResult,
    AdminScalabilityBottleneckReview,
    AdminSearchInspectResponse,
    AdminSourceFilesResponse,
    AdminTaxAssignmentIssuesResponse,
    AdminValidationResultsResponse,
)
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

router = APIRouter(dependencies=[Depends(require_admin_access)])


@router.get(
    "/admin/leads",
    response_model=AdminLeadListResponse,
)
def admin_leads_endpoint(
    county_id: Annotated[str | None, Query()] = None,
    requested_tax_year: Annotated[int | None, Query()] = None,
    served_tax_year: Annotated[int | None, Query()] = None,
    demand_bucket: Annotated[str | None, Query()] = None,
    fallback_applied: Annotated[bool | None, Query()] = None,
    source_channel: Annotated[str | None, Query()] = None,
    duplicate_only: Annotated[bool, Query()] = False,
    quote_ready_only: Annotated[bool, Query()] = False,
    submitted_from: Annotated[date | None, Query()] = None,
    submitted_to: Annotated[date | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AdminLeadListResponse:
    return get_admin_leads(
        county_id=county_id,
        requested_tax_year=requested_tax_year,
        served_tax_year=served_tax_year,
        demand_bucket=demand_bucket,
        fallback_applied=fallback_applied,
        source_channel=source_channel,
        duplicate_only=duplicate_only,
        quote_ready_only=quote_ready_only,
        submitted_from=submitted_from,
        submitted_to=submitted_to,
        limit=limit,
    )


@router.get(
    "/admin/leads/{lead_id}",
    response_model=AdminLeadDetail,
)
def admin_lead_detail_endpoint(lead_id: str) -> AdminLeadDetail:
    try:
        return get_admin_lead_detail(lead_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/admin/readiness/{county_id}",
    response_model=AdminCountyYearReadinessDashboard,
)
def county_year_readiness_endpoint(
    county_id: str,
    tax_years: Annotated[list[int], Query(min_length=1)],
) -> AdminCountyYearReadinessDashboard:
    return get_county_year_readiness(county_id=county_id, tax_years=tax_years)


@router.get(
    "/admin/onboarding/{county_id}",
    response_model=AdminCountyOnboardingContract,
)
def county_onboarding_contract_endpoint(
    county_id: str,
    tax_years: Annotated[list[int], Query(min_length=1)],
    current_tax_year: Annotated[int | None, Query()] = None,
) -> AdminCountyOnboardingContract:
    return get_county_onboarding_contract(
        county_id=county_id,
        tax_years=tax_years,
        current_tax_year=current_tax_year,
    )


@router.get(
    "/admin/scalability/{county_id}",
    response_model=AdminScalabilityBottleneckReview,
)
def county_scalability_bottleneck_endpoint(
    county_id: str,
    tax_years: Annotated[list[int], Query(min_length=1)],
    limit: Annotated[int, Query(ge=1, le=25)] = 5,
) -> AdminScalabilityBottleneckReview:
    return get_scalability_bottleneck_review(
        county_id=county_id,
        tax_years=tax_years,
        limit=limit,
    )


@router.get(
    "/admin/search/inspect",
    response_model=AdminSearchInspectResponse,
)
def search_inspection_endpoint(
    query: Annotated[str, Query(min_length=3)],
    limit: Annotated[int, Query(ge=1, le=25)] = 10,
) -> AdminSearchInspectResponse:
    return get_search_inspection(query, limit=limit)


@router.get(
    "/admin/ops/import-batches",
    response_model=AdminImportBatchListResponse,
)
def import_batches_endpoint(
    county_id: Annotated[str, Query(min_length=1)],
    tax_year: Annotated[int | None, Query()] = None,
    dataset_type: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AdminImportBatchListResponse:
    return get_import_batches(
        county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        limit=limit,
    )


@router.get(
    "/admin/ops/import-batches/{import_batch_id}",
    response_model=AdminImportBatchDetail,
)
def import_batch_detail_endpoint(import_batch_id: str) -> AdminImportBatchDetail:
    return get_import_batch_detail(import_batch_id)


@router.get(
    "/admin/ops/validation/{import_batch_id}",
    response_model=AdminValidationResultsResponse,
)
def validation_results_endpoint(
    import_batch_id: str,
    severity: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminValidationResultsResponse:
    return get_validation_results(
        import_batch_id,
        severity=severity,
        limit=limit,
    )


@router.get(
    "/admin/ops/source-files",
    response_model=AdminSourceFilesResponse,
)
def source_files_endpoint(
    county_id: Annotated[str, Query(min_length=1)],
    tax_year: Annotated[int | None, Query()] = None,
    dataset_type: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminSourceFilesResponse:
    return get_source_files(
        county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        limit=limit,
    )


@router.get(
    "/admin/ops/completeness/{county_id}/{tax_year}",
    response_model=AdminCompletenessIssuesResponse,
)
def completeness_issues_endpoint(
    county_id: str,
    tax_year: int,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminCompletenessIssuesResponse:
    return get_completeness_issues(county_id, tax_year=tax_year, limit=limit)


@router.get(
    "/admin/ops/tax-assignment/{county_id}/{tax_year}",
    response_model=AdminTaxAssignmentIssuesResponse,
)
def tax_assignment_issues_endpoint(
    county_id: str,
    tax_year: int,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminTaxAssignmentIssuesResponse:
    return get_tax_assignment_issues(county_id, tax_year=tax_year, limit=limit)


@router.post(
    "/admin/ops/manual-import/register",
    response_model=AdminMutationResult,
)
def manual_import_register_endpoint(
    request: AdminManualImportRequest,
) -> AdminMutationResult:
    return post_manual_import_register(request)


@router.post(
    "/admin/ops/import-batches/{import_batch_id}/publish",
    response_model=AdminMutationResult,
)
def publish_import_batch_endpoint(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    return post_publish_import_batch(import_batch_id, request)


@router.post(
    "/admin/ops/import-batches/{import_batch_id}/rollback",
    response_model=AdminMutationResult,
)
def rollback_import_batch_endpoint(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    return post_rollback_import_batch(import_batch_id, request)


@router.post(
    "/admin/ops/import-batches/{import_batch_id}/retry-maintenance",
    response_model=AdminMutationResult,
)
def retry_import_batch_maintenance_endpoint(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    return post_retry_import_batch_maintenance(import_batch_id, request)


@router.get(
    "/admin/cases",
    response_model=AdminCaseListResponse,
)
def admin_cases_endpoint(
    county_id: Annotated[str | None, Query()] = None,
    tax_year: Annotated[int | None, Query()] = None,
    case_status: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AdminCaseListResponse:
    return get_admin_cases(
        county_id=county_id,
        tax_year=tax_year,
        case_status=case_status,
        limit=limit,
    )


@router.post(
    "/admin/cases",
    response_model=CaseMutationResult,
)
def admin_case_create_endpoint(request: ProtestCaseCreate) -> CaseMutationResult:
    try:
        return post_admin_case(request)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/admin/cases/{protest_case_id}",
    response_model=AdminCaseDetail,
)
def admin_case_detail_endpoint(protest_case_id: str) -> AdminCaseDetail:
    try:
        return get_admin_case_detail(protest_case_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/admin/cases/{protest_case_id}/notes",
    response_model=CaseMutationResult,
)
def admin_case_note_endpoint(
    protest_case_id: str,
    request: ProtestCaseNoteCreate,
) -> CaseMutationResult:
    try:
        return post_admin_case_note(protest_case_id, request)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/admin/cases/{protest_case_id}/status",
    response_model=CaseMutationResult,
)
def admin_case_status_endpoint(
    protest_case_id: str,
    request: ProtestCaseStatusUpdate,
) -> CaseMutationResult:
    try:
        return post_admin_case_status(protest_case_id, request)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/admin/packets",
    response_model=AdminEvidencePacketListResponse,
)
def admin_packets_endpoint(
    county_id: Annotated[str | None, Query()] = None,
    tax_year: Annotated[int | None, Query()] = None,
    packet_status: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AdminEvidencePacketListResponse:
    return get_admin_packets(
        county_id=county_id,
        tax_year=tax_year,
        packet_status=packet_status,
        limit=limit,
    )


@router.post(
    "/admin/packets",
    response_model=CaseMutationResult,
)
def admin_packet_create_endpoint(request: EvidencePacketCreate) -> CaseMutationResult:
    try:
        return post_admin_packet(request)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/admin/packets/{evidence_packet_id}",
    response_model=AdminEvidencePacketDetail,
)
def admin_packet_detail_endpoint(evidence_packet_id: str) -> AdminEvidencePacketDetail:
    try:
        return get_admin_packet_detail(evidence_packet_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
