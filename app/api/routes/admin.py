from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.admin import (
    get_completeness_issues,
    get_county_year_readiness,
    get_import_batch_detail,
    get_import_batches,
    get_search_inspection,
    get_source_files,
    get_tax_assignment_issues,
    get_validation_results,
    post_manual_import_register,
    post_publish_import_batch,
    post_rollback_import_batch,
)
from app.api.deps.admin_auth import require_admin_access
from app.models.admin import (
    AdminCompletenessIssuesResponse,
    AdminCountyYearReadinessDashboard,
    AdminImportBatchActionRequest,
    AdminImportBatchDetail,
    AdminImportBatchListResponse,
    AdminManualImportRequest,
    AdminMutationResult,
    AdminSearchInspectResponse,
    AdminSourceFilesResponse,
    AdminTaxAssignmentIssuesResponse,
    AdminValidationResultsResponse,
)

router = APIRouter(dependencies=[Depends(require_admin_access)])


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
