from __future__ import annotations

from app.models.admin import (
    AdminCompletenessIssuesResponse,
    AdminCountyOnboardingAction,
    AdminCountyOnboardingContract,
    AdminCountyOnboardingDatasetSnapshot,
    AdminCountyOnboardingPhase,
    AdminCountyOnboardingReadinessSnapshot,
    AdminCountyOnboardingSummary,
    AdminCountyOnboardingValidationYear,
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
from app.services.county_onboarding import CountyOnboardingService


def get_county_year_readiness(
    county_id: str,
    tax_years: list[int],
) -> AdminCountyYearReadinessDashboard:
    service = AdminReadinessService()
    return service.build_dashboard(county_id=county_id, tax_years=tax_years)


def get_county_onboarding_contract(
    county_id: str,
    *,
    tax_years: list[int],
    current_tax_year: int | None = None,
) -> AdminCountyOnboardingContract:
    service = CountyOnboardingService()
    contract = service.build_contract(
        county_id=county_id,
        tax_years=tax_years,
        current_tax_year=current_tax_year,
    )
    return AdminCountyOnboardingContract(
        county_id=contract.county_id,
        current_tax_year=contract.current_tax_year,
        validation_tax_year=contract.validation_tax_year,
        validation_recommended=contract.validation_recommended,
        onboarding_summary=AdminCountyOnboardingSummary(
            overall_status=contract.onboarding_summary.overall_status,
            done_phase_count=contract.onboarding_summary.done_phase_count,
            pending_phase_count=contract.onboarding_summary.pending_phase_count,
            blocked_phase_count=contract.onboarding_summary.blocked_phase_count,
            blocking_phase_codes=list(contract.onboarding_summary.blocking_phase_codes),
            next_phase_code=contract.onboarding_summary.next_phase_code,
            next_blocking_phase_code=contract.onboarding_summary.next_blocking_phase_code,
        ),
        capabilities=[
            {
                "capability_code": capability.capability_code,
                "label": capability.label,
                "status": capability.status,
                "source_datasets": capability.source_datasets,
                "notes": capability.notes,
                "metadata": capability.metadata,
            }
            for capability in contract.capabilities
        ],
        validation_candidates=[
            AdminCountyOnboardingValidationYear(
                tax_year=candidate.tax_year,
                readiness_score=candidate.readiness_score,
                recommended_for_qa=candidate.recommended_for_qa,
                caveats=list(candidate.caveats),
                validation_capabilities=dict(candidate.validation_capabilities),
            )
            for candidate in contract.validation_candidates
        ],
        current_year_snapshot=(
            None
            if contract.current_year_snapshot is None
            else AdminCountyOnboardingReadinessSnapshot(
                tax_year=contract.current_year_snapshot.tax_year,
                datasets=[
                    AdminCountyOnboardingDatasetSnapshot(
                        dataset_type=dataset.dataset_type,
                        access_method=dataset.access_method,
                        availability_status=dataset.availability_status,
                        raw_file_count=dataset.raw_file_count,
                        latest_import_batch_id=dataset.latest_import_batch_id,
                        latest_import_status=dataset.latest_import_status,
                        latest_publish_state=dataset.latest_publish_state,
                        canonical_published=dataset.canonical_published,
                    )
                    for dataset in contract.current_year_snapshot.datasets
                ],
                parcel_summary_ready=contract.current_year_snapshot.parcel_summary_ready,
                search_support_ready=contract.current_year_snapshot.search_support_ready,
                feature_ready=contract.current_year_snapshot.feature_ready,
                comp_ready=contract.current_year_snapshot.comp_ready,
                quote_ready=contract.current_year_snapshot.quote_ready,
            )
        ),
        validation_year_snapshot=(
            None
            if contract.validation_year_snapshot is None
            else AdminCountyOnboardingReadinessSnapshot(
                tax_year=contract.validation_year_snapshot.tax_year,
                datasets=[
                    AdminCountyOnboardingDatasetSnapshot(
                        dataset_type=dataset.dataset_type,
                        access_method=dataset.access_method,
                        availability_status=dataset.availability_status,
                        raw_file_count=dataset.raw_file_count,
                        latest_import_batch_id=dataset.latest_import_batch_id,
                        latest_import_status=dataset.latest_import_status,
                        latest_publish_state=dataset.latest_publish_state,
                        canonical_published=dataset.canonical_published,
                    )
                    for dataset in contract.validation_year_snapshot.datasets
                ],
                parcel_summary_ready=contract.validation_year_snapshot.parcel_summary_ready,
                search_support_ready=contract.validation_year_snapshot.search_support_ready,
                feature_ready=contract.validation_year_snapshot.feature_ready,
                comp_ready=contract.validation_year_snapshot.comp_ready,
                quote_ready=contract.validation_year_snapshot.quote_ready,
            )
        ),
        phases=[
            AdminCountyOnboardingPhase(
                phase_code=phase.phase_code,
                label=phase.label,
                status=phase.status,
                blocking=phase.blocking,
                summary=phase.summary,
                details=list(phase.details),
            )
            for phase in contract.phases
        ],
        recommended_actions=[
            AdminCountyOnboardingAction(
                action_code=action.action_code,
                phase_code=action.phase_code,
                blocking=action.blocking,
                summary=action.summary,
                command_hint=action.command_hint,
            )
            for action in contract.recommended_actions
        ],
    )


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


def post_retry_import_batch_maintenance(
    import_batch_id: str,
    request: AdminImportBatchActionRequest,
) -> AdminMutationResult:
    service = AdminOpsService()
    return service.retry_import_batch_maintenance(import_batch_id=import_batch_id, request=request)
