from __future__ import annotations

from datetime import datetime

from pydantic import Field

from app.models.common import DwellioBaseModel, JsonDict


class AdminCountyYearDatasetReadiness(DwellioBaseModel):
    dataset_type: str
    source_system_code: str
    access_method: str
    availability_status: str
    raw_file_count: int
    latest_import_batch_id: str | None = None
    latest_import_status: str | None = None
    latest_status_reason: str | None = None
    latest_publish_state: str | None = None
    stage_status: str
    blockers: list[str]
    latest_activity_at: datetime | None = None
    freshness_status: str = "unknown"
    freshness_sla_days: int | None = None
    freshness_age_days: int | None = None
    recent_failed_job_count: int = 0
    stale_running_job_count: int = 0
    validation_error_count: int = 0
    validation_regression: bool = False


class AdminCountyYearDerivedReadiness(DwellioBaseModel):
    parcel_summary_ready: bool
    parcel_year_trend_ready: bool = False
    neighborhood_stats_ready: bool = False
    neighborhood_year_trend_ready: bool = False
    instant_quote_subject_ready: bool = False
    instant_quote_neighborhood_stats_ready: bool = False
    instant_quote_segment_stats_ready: bool = False
    instant_quote_asset_ready: bool = False
    instant_quote_ready: bool = False
    instant_quote_refresh_status: str | None = None
    instant_quote_last_refresh_at: datetime | None = None
    instant_quote_last_validated_at: datetime | None = None
    instant_quote_cache_view_row_delta: int | None = None
    instant_quote_tax_rate_basis_year: int | None = None
    instant_quote_tax_rate_basis_reason: str | None = None
    instant_quote_tax_rate_basis_fallback_applied: bool = False
    instant_quote_tax_rate_basis_status: str | None = None
    instant_quote_tax_rate_basis_status_reason: str | None = None
    instant_quote_tax_rate_requested_year_supportable_subject_row_count: int = 0
    instant_quote_tax_rate_basis_supportable_subject_row_count: int = 0
    instant_quote_supported_public_quote_exists: bool = False
    instant_quote_subject_rows_without_usable_neighborhood_stats: int = 0
    instant_quote_subject_rows_without_usable_segment_stats: int = 0
    instant_quote_subject_rows_missing_segment_row: int = 0
    instant_quote_subject_rows_thin_segment_support: int = 0
    instant_quote_subject_rows_unusable_segment_basis: int = 0
    instant_quote_served_neighborhood_only_quote_count: int = 0
    instant_quote_served_supported_neighborhood_only_quote_count: int = 0
    instant_quote_served_unsupported_neighborhood_only_quote_count: int = 0
    search_support_ready: bool
    feature_ready: bool
    comp_ready: bool
    valuation_ready: bool = False
    savings_ready: bool = False
    decision_tree_ready: bool = False
    explanation_ready: bool = False
    recommendation_ready: bool = False
    quote_ready: bool
    parcel_summary_row_count: int
    parcel_year_trend_row_count: int = 0
    neighborhood_stats_row_count: int = 0
    neighborhood_year_trend_row_count: int = 0
    instant_quote_subject_row_count: int = 0
    instant_quote_neighborhood_stats_row_count: int = 0
    instant_quote_segment_stats_row_count: int = 0
    instant_quote_supportable_row_count: int = 0
    instant_quote_supported_neighborhood_stats_row_count: int = 0
    instant_quote_supported_segment_stats_row_count: int = 0
    search_document_row_count: int
    parcel_feature_row_count: int
    comp_pool_row_count: int
    valuation_run_row_count: int = 0
    savings_row_count: int = 0
    decision_tree_row_count: int = 0
    explanation_row_count: int = 0
    recommendation_row_count: int = 0
    quote_row_count: int


class AdminCountyYearOperationalReadiness(DwellioBaseModel):
    quality_score: int = 0
    quality_status: str = "unknown"
    freshness_status: str = "unknown"
    freshness_sla_days: int | None = None
    freshness_age_days: int | None = None
    latest_activity_at: datetime | None = None
    recent_failed_job_count: int = 0
    stale_running_job_count: int = 0
    validation_error_count: int = 0
    validation_regression_count: int = 0
    searchable_ready: bool = False
    alerts: list[str] = Field(default_factory=list)


class AdminCountyYearReadiness(DwellioBaseModel):
    county_id: str
    tax_year: int
    overall_status: str
    readiness_score: int
    trend_label: str
    trend_delta: int | None = None
    tax_year_known: bool
    blockers: list[str]
    datasets: list[AdminCountyYearDatasetReadiness]
    derived: AdminCountyYearDerivedReadiness
    operational: AdminCountyYearOperationalReadiness = Field(
        default_factory=AdminCountyYearOperationalReadiness
    )


class AdminCountyYearReadinessKpiSummary(DwellioBaseModel):
    total_year_count: int = 0
    healthy_year_count: int = 0
    warning_year_count: int = 0
    critical_year_count: int = 0
    stale_year_count: int = 0
    searchable_year_count: int = 0
    failed_job_count: int = 0
    validation_regression_count: int = 0


class AdminCountyYearReadinessDashboard(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_years: list[int]
    readiness_rows: list[AdminCountyYearReadiness]
    kpi_summary: AdminCountyYearReadinessKpiSummary = Field(
        default_factory=AdminCountyYearReadinessKpiSummary
    )


class AdminSearchScoreComponents(DwellioBaseModel):
    basis_rank: int
    address_similarity: float
    search_text_similarity: float
    owner_similarity: float


class AdminSearchInspectCandidate(DwellioBaseModel):
    county_id: str
    tax_year: int | None = None
    account_number: str
    parcel_id: str
    address: str
    situs_zip: str | None = None
    owner_name: str | None = None
    match_basis: str
    match_score: float
    confidence_label: str
    confidence_reasons: list[str]
    matched_fields: list[str]
    score_components: AdminSearchScoreComponents


class AdminSearchInspectResponse(DwellioBaseModel):
    access_scope: str = "internal"
    query: str
    normalized_address_query: str
    normalized_owner_query: str | None = None
    candidates: list[AdminSearchInspectCandidate]


class AdminImportBatchSummary(DwellioBaseModel):
    import_batch_id: str
    county_id: str
    tax_year: int
    dataset_type: str
    source_system_code: str
    source_filename: str | None = None
    file_format: str | None = None
    status: str
    status_reason: str | None = None
    publish_state: str | None = None
    publish_version: str | None = None
    row_count: int | None = None
    error_count: int | None = None
    raw_file_count: int
    validation_result_count: int
    validation_error_count: int
    latest_job_name: str | None = None
    latest_job_stage: str | None = None
    latest_job_status: str | None = None
    latest_job_started_at: datetime | None = None
    latest_job_finished_at: datetime | None = None
    latest_job_error_message: str | None = None
    created_at: datetime | None = None


class AdminImportBatchListResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_year: int | None = None
    dataset_type: str | None = None
    batches: list[AdminImportBatchSummary]


class AdminJobRunSummary(DwellioBaseModel):
    job_run_id: str
    job_name: str
    job_stage: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    row_count: int | None = None
    error_message: str | None = None
    metadata_json: JsonDict = Field(default_factory=dict)


class AdminSourceFileRecord(DwellioBaseModel):
    raw_file_id: str
    import_batch_id: str
    county_id: str
    tax_year: int
    dataset_type: str
    source_system_code: str
    original_filename: str
    storage_path: str
    checksum: str
    mime_type: str | None = None
    size_bytes: int
    file_format: str | None = None
    source_url: str | None = None
    created_at: datetime | None = None


class AdminSourceFilesResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_year: int | None = None
    dataset_type: str | None = None
    source_files: list[AdminSourceFileRecord]


class AdminValidationFinding(DwellioBaseModel):
    validation_result_id: str
    validation_code: str
    message: str
    severity: str
    validation_scope: str
    entity_table: str | None = None
    details_json: JsonDict = Field(default_factory=dict)
    created_at: datetime | None = None


class AdminValidationResultsResponse(DwellioBaseModel):
    access_scope: str = "internal"
    import_batch_id: str
    total_count: int
    error_count: int
    warning_count: int
    info_count: int
    findings: list[AdminValidationFinding]


class AdminImportBatchInspection(DwellioBaseModel):
    status: str
    publish_state: str | None = None
    publish_version: str | None = None
    row_count: int | None = None
    error_count: int | None = None
    raw_file_count: int
    job_run_count: int
    staging_row_count: int
    lineage_record_count: int
    validation_result_count: int
    validation_error_count: int
    parcel_year_snapshot_count: int = 0
    parcel_assessment_count: int = 0
    parcel_exemption_count: int = 0
    taxing_unit_count: int = 0
    tax_rate_count: int = 0
    parcel_tax_assignment_count: int = 0
    effective_tax_rate_count: int = 0
    deed_record_count: int = 0
    deed_party_count: int = 0
    parcel_owner_period_count: int = 0
    current_owner_rollup_count: int = 0


class AdminImportBatchActions(DwellioBaseModel):
    can_publish: bool
    can_rollback: bool
    manual_fallback_supported: bool
    manual_fallback_message: str | None = None


class AdminImportBatchDetail(DwellioBaseModel):
    access_scope: str = "internal"
    batch: AdminImportBatchSummary
    inspection: AdminImportBatchInspection
    validation_summary: AdminValidationResultsResponse
    source_files: list[AdminSourceFileRecord]
    job_runs: list[AdminJobRunSummary]
    actions: AdminImportBatchActions


class AdminCompletenessIssue(DwellioBaseModel):
    county_id: str
    parcel_id: str
    tax_year: int
    account_number: str
    completeness_score: float
    public_summary_ready_flag: bool
    admin_review_required: bool
    warning_codes: list[str] = Field(default_factory=list)


class AdminCompletenessIssuesResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_year: int
    issues: list[AdminCompletenessIssue]


class AdminTaxAssignmentIssue(DwellioBaseModel):
    county_id: str
    parcel_id: str
    tax_year: int
    account_number: str
    county_assignment_count: int
    city_assignment_count: int
    school_assignment_count: int
    mud_assignment_count: int
    special_assignment_count: int
    missing_county_assignment: bool
    missing_city_assignment: bool
    missing_school_assignment: bool
    missing_mud_assignment: bool
    conflicting_county_assignment: bool
    conflicting_city_assignment: bool
    conflicting_school_assignment: bool
    conflicting_mud_assignment: bool


class AdminTaxAssignmentIssuesResponse(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_year: int
    issues: list[AdminTaxAssignmentIssue]


class AdminManualImportRequest(DwellioBaseModel):
    county_id: str
    tax_year: int
    dataset_type: str
    source_file_path: str
    source_url: str | None = None
    dry_run: bool = False


class AdminImportBatchActionRequest(DwellioBaseModel):
    county_id: str
    tax_year: int
    dataset_type: str


class AdminMutationResult(DwellioBaseModel):
    access_scope: str = "internal"
    action: str
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str | None = None
    job_run_id: str | None = None
    publish_version: str | None = None
    message: str


class AdminCountyYearWorkflowStep(DwellioBaseModel):
    step_code: str
    phase: str
    title: str
    status: str
    summary: str
    blockers: list[str] = Field(default_factory=list)
    recommended_action: str | None = None
    commands: list[str] = Field(default_factory=list)
    related_dataset_type: str | None = None
    related_import_batch_id: str | None = None


class AdminCountyYearWorkflowValidationCandidate(DwellioBaseModel):
    tax_year: int
    readiness_score: int
    recommended_for_qa: bool
    parcel_summary_ready: bool
    quote_ready: bool
    instant_quote_ready: bool
    caveats: list[str] = Field(default_factory=list)


class AdminCountyYearWorkflow(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_year: int
    overall_status: str
    readiness_score: int
    searchable_ready: bool
    preferred_validation_year: int | None = None
    summary: str
    alerts: list[str] = Field(default_factory=list)
    steps: list[AdminCountyYearWorkflowStep] = Field(default_factory=list)
    validation_candidates: list[AdminCountyYearWorkflowValidationCandidate] = Field(
        default_factory=list
    )
