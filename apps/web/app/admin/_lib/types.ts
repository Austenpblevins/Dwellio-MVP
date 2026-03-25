export type AdminCountyYearDatasetReadiness = {
  dataset_type: string;
  source_system_code: string;
  access_method: string;
  availability_status: string;
  raw_file_count: number;
  latest_import_status: string | null;
  latest_publish_state: string | null;
  stage_status: string;
  blockers: string[];
};

export type AdminCountyYearDerivedReadiness = {
  parcel_summary_ready: boolean;
  parcel_year_trend_ready: boolean;
  neighborhood_stats_ready: boolean;
  neighborhood_year_trend_ready: boolean;
  search_support_ready: boolean;
  feature_ready: boolean;
  comp_ready: boolean;
  valuation_ready: boolean;
  savings_ready: boolean;
  decision_tree_ready: boolean;
  explanation_ready: boolean;
  recommendation_ready: boolean;
  quote_ready: boolean;
  parcel_summary_row_count: number;
  parcel_year_trend_row_count: number;
  neighborhood_stats_row_count: number;
  neighborhood_year_trend_row_count: number;
  search_document_row_count: number;
  parcel_feature_row_count: number;
  comp_pool_row_count: number;
  valuation_run_row_count: number;
  savings_row_count: number;
  decision_tree_row_count: number;
  explanation_row_count: number;
  recommendation_row_count: number;
  quote_row_count: number;
};

export type AdminCountyYearReadiness = {
  county_id: string;
  tax_year: number;
  overall_status: string;
  readiness_score: number;
  trend_label: string;
  trend_delta: number | null;
  tax_year_known: boolean;
  blockers: string[];
  datasets: AdminCountyYearDatasetReadiness[];
  derived: AdminCountyYearDerivedReadiness;
};

export type AdminCountyYearReadinessDashboard = {
  access_scope: string;
  county_id: string;
  tax_years: number[];
  readiness_rows: AdminCountyYearReadiness[];
};

export type AdminImportBatchSummary = {
  import_batch_id: string;
  county_id: string;
  tax_year: number;
  dataset_type: string;
  source_system_code: string;
  source_filename: string;
  file_format: string | null;
  status: string;
  publish_state: string | null;
  publish_version: string | null;
  row_count: number | null;
  error_count: number | null;
  raw_file_count: number;
  validation_result_count: number;
  validation_error_count: number;
  latest_job_name: string | null;
  latest_job_stage: string | null;
  latest_job_status: string | null;
  latest_job_started_at: string | null;
  latest_job_finished_at: string | null;
  latest_job_error_message: string | null;
  created_at: string | null;
};

export type AdminImportBatchListResponse = {
  access_scope: string;
  county_id: string;
  tax_year: number | null;
  dataset_type: string | null;
  batches: AdminImportBatchSummary[];
};

export type AdminValidationFinding = {
  validation_result_id: string;
  validation_code: string;
  message: string;
  severity: string;
  validation_scope: string;
  entity_table: string | null;
  details_json: Record<string, unknown>;
  created_at: string | null;
};

export type AdminValidationResultsResponse = {
  access_scope: string;
  import_batch_id: string;
  total_count: number;
  error_count: number;
  warning_count: number;
  info_count: number;
  findings: AdminValidationFinding[];
};

export type AdminSourceFileRecord = {
  raw_file_id: string;
  import_batch_id: string;
  county_id: string;
  tax_year: number;
  dataset_type: string;
  source_system_code: string;
  original_filename: string;
  storage_path: string;
  checksum: string;
  mime_type: string | null;
  size_bytes: number;
  file_format: string | null;
  source_url: string | null;
  created_at: string | null;
};

export type AdminSourceFilesResponse = {
  access_scope: string;
  county_id: string;
  tax_year: number | null;
  dataset_type: string | null;
  source_files: AdminSourceFileRecord[];
};

export type AdminJobRunSummary = {
  job_run_id: string;
  job_name: string;
  job_stage: string;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  row_count: number | null;
  error_message: string | null;
  metadata_json: Record<string, unknown>;
};

export type AdminImportBatchInspection = {
  status: string;
  publish_state: string | null;
  publish_version: string | null;
  row_count: number | null;
  error_count: number | null;
  raw_file_count: number;
  job_run_count: number;
  staging_row_count: number;
  lineage_record_count: number;
  validation_result_count: number;
  validation_error_count: number;
  parcel_year_snapshot_count: number;
  parcel_assessment_count: number;
  parcel_exemption_count: number;
  taxing_unit_count: number;
  tax_rate_count: number;
  parcel_tax_assignment_count: number;
  effective_tax_rate_count: number;
  deed_record_count: number;
  deed_party_count: number;
  parcel_owner_period_count: number;
  current_owner_rollup_count: number;
};

export type AdminImportBatchActions = {
  can_publish: boolean;
  can_rollback: boolean;
  manual_fallback_supported: boolean;
  manual_fallback_message: string | null;
};

export type AdminImportBatchDetail = {
  access_scope: string;
  batch: AdminImportBatchSummary;
  inspection: AdminImportBatchInspection;
  validation_summary: AdminValidationResultsResponse;
  source_files: AdminSourceFileRecord[];
  job_runs: AdminJobRunSummary[];
  actions: AdminImportBatchActions;
};

export type AdminCompletenessIssue = {
  county_id: string;
  parcel_id: string;
  tax_year: number;
  account_number: string;
  completeness_score: number;
  public_summary_ready_flag: boolean;
  admin_review_required: boolean;
  warning_codes: string[];
};

export type AdminCompletenessIssuesResponse = {
  access_scope: string;
  county_id: string;
  tax_year: number;
  issues: AdminCompletenessIssue[];
};

export type AdminTaxAssignmentIssue = {
  county_id: string;
  parcel_id: string;
  tax_year: number;
  account_number: string;
  county_assignment_count: number;
  city_assignment_count: number;
  school_assignment_count: number;
  mud_assignment_count: number;
  special_assignment_count: number;
  missing_county_assignment: boolean;
  missing_city_assignment: boolean;
  missing_school_assignment: boolean;
  missing_mud_assignment: boolean;
  conflicting_county_assignment: boolean;
  conflicting_city_assignment: boolean;
  conflicting_school_assignment: boolean;
  conflicting_mud_assignment: boolean;
};

export type AdminTaxAssignmentIssuesResponse = {
  access_scope: string;
  county_id: string;
  tax_year: number;
  issues: AdminTaxAssignmentIssue[];
};

export type AdminMutationResult = {
  access_scope: string;
  action: string;
  county_id: string;
  tax_year: number;
  dataset_type: string;
  import_batch_id: string | null;
  job_run_id: string | null;
  publish_version: string | null;
  message: string;
};
