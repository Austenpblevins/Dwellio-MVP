from __future__ import annotations

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
from app.services.address_resolver import AddressResolverService
from app.services.admin_ops import AdminOpsService
from app.services.admin_readiness import AdminReadinessService


def get_county_year_readiness(
    county_id: str,
    tax_years: list[int],
) -> AdminCountyYearReadinessDashboard:
    service = AdminReadinessService()
    return service.build_dashboard(county_id=county_id, tax_years=tax_years)


def get_search_inspection(
    query: str,
    *,
    limit: int = 10,
) -> AdminSearchInspectResponse:
    service = AddressResolverService()
    return service.inspect_search(query, limit=limit)


def get_import_batches(
    county_id: str,
    *,
    tax_year: int | None = None,
    dataset_type: str | None = None,
    limit: int = 50,
) -> AdminImportBatchListResponse:
    service = AdminOpsService()
    return service.list_import_batches(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        limit=limit,
    )


def get_import_batch_detail(import_batch_id: str) -> AdminImportBatchDetail:
    service = AdminOpsService()
    return service.get_import_batch_detail(import_batch_id=import_batch_id)


def get_validation_results(
    import_batch_id: str,
    *,
    severity: str | None = None,
    limit: int = 100,
) -> AdminValidationResultsResponse:
    service = AdminOpsService()
    return service.list_validation_results(
        import_batch_id=import_batch_id,
        severity=severity,
        limit=limit,
    )


def get_source_files(
    county_id: str,
    *,
    tax_year: int | None = None,
    dataset_type: str | None = None,
    limit: int = 100,
) -> AdminSourceFilesResponse:
    service = AdminOpsService()
    return service.list_source_files(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        limit=limit,
    )


def get_completeness_issues(
    county_id: str,
    *,
    tax_year: int,
    limit: int = 100,
) -> AdminCompletenessIssuesResponse:
    service = AdminOpsService()
    return service.list_completeness_issues(
        county_id=county_id,
        tax_year=tax_year,
        limit=limit,
    )


def get_tax_assignment_issues(
    county_id: str,
    *,
    tax_year: int,
    limit: int = 100,
) -> AdminTaxAssignmentIssuesResponse:
    service = AdminOpsService()
    return service.list_tax_assignment_issues(
        county_id=county_id,
        tax_year=tax_year,
        limit=limit,
    )


def post_manual_import_register(request: AdminManualImportRequest) -> AdminMutationResult:
    service = AdminOpsService()
    return service.register_manual_import(request=request)


def post_publish_import_batch(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    service = AdminOpsService()
    return service.publish_import_batch(import_batch_id=import_batch_id, request=request)


def post_rollback_import_batch(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    service = AdminOpsService()
    return service.rollback_import_batch(import_batch_id=import_batch_id, request=request)
