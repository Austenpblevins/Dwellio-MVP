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

export type CaseMutationResult = {
  access_scope: string;
  action: string;
  protest_case_id: string | null;
  evidence_packet_id: string | null;
  message: string;
};

export type AdminCaseSummary = {
  protest_case_id: string;
  county_id: string;
  parcel_id: string;
  account_number: string;
  tax_year: number;
  case_status: string;
  workflow_status_code: string | null;
  address: string | null;
  owner_name: string | null;
  client_id: string | null;
  client_name: string | null;
  representation_agreement_id: string | null;
  valuation_run_id: string | null;
  packet_count: number;
  note_count: number;
  hearing_count: number;
  latest_outcome_code: string | null;
  outcome_date: string | null;
  recommendation_code: string | null;
  expected_tax_savings_point: number | null;
  created_at: string | null;
  updated_at: string | null;
};

export type AdminCaseNote = {
  case_note_id: string;
  note_text: string;
  note_code: string;
  author_label: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type AdminCaseAssignment = {
  case_assignment_id: string;
  assignee_name: string;
  assignee_role: string;
  assignment_status: string;
  assigned_at: string | null;
  due_at: string | null;
  active_flag: boolean;
  metadata_json: Record<string, unknown>;
};

export type AdminHearingSummary = {
  hearing_id: string;
  hearing_type_code: string;
  hearing_status: string;
  scheduled_at: string | null;
  location_text: string | null;
  hearing_reference: string | null;
  result_summary: string | null;
};

export type AdminCaseStatusHistoryEntry = {
  case_status_history_id: string;
  workflow_status_code: string | null;
  case_status: string;
  reason_text: string | null;
  changed_by: string | null;
  created_at: string | null;
};

export type AdminCaseListResponse = {
  access_scope: string;
  county_id: string | null;
  tax_year: number | null;
  case_status: string | null;
  cases: AdminCaseSummary[];
};

export type AdminEvidencePacketSummary = {
  evidence_packet_id: string;
  protest_case_id: string | null;
  county_id: string;
  parcel_id: string;
  account_number: string;
  tax_year: number;
  packet_type: string;
  packet_status: string;
  valuation_run_id: string | null;
  address: string | null;
  case_status: string | null;
  item_count: number;
  comp_set_count: number;
  generated_at: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type AdminEvidencePacketItem = {
  evidence_packet_item_id: string;
  item_type: string;
  section_code: string;
  title: string;
  body_text: string | null;
  source_basis: string | null;
  display_order: number;
  metadata_json: Record<string, unknown>;
};

export type AdminEvidenceCompSetItem = {
  evidence_comp_set_item_id: string;
  parcel_sale_id: string | null;
  parcel_id: string | null;
  comp_role: string;
  comp_rank: number | null;
  rationale_text: string | null;
  adjustment_summary_json: Record<string, unknown>;
};

export type AdminEvidenceCompSet = {
  evidence_comp_set_id: string;
  basis_type: string;
  set_label: string;
  notes: string | null;
  metadata_json: Record<string, unknown>;
  items: AdminEvidenceCompSetItem[];
};

export type AdminEvidencePacketListResponse = {
  access_scope: string;
  county_id: string | null;
  tax_year: number | null;
  packet_status: string | null;
  packets: AdminEvidencePacketSummary[];
};

export type AdminEvidencePacketDetail = {
  access_scope: string;
  packet: AdminEvidencePacketSummary;
  items: AdminEvidencePacketItem[];
  comp_sets: AdminEvidenceCompSet[];
};

export type AdminCaseDetail = {
  access_scope: string;
  case: AdminCaseSummary;
  notes: AdminCaseNote[];
  assignments: AdminCaseAssignment[];
  hearings: AdminHearingSummary[];
  status_history: AdminCaseStatusHistoryEntry[];
  packets: AdminEvidencePacketSummary[];
};

export type AdminLeadReportingKpiSummary = {
  total_count: number;
  quote_ready_count: number;
  reachable_unquoted_count: number;
  unsupported_county_count: number;
  unsupported_property_count: number;
  fallback_applied_count: number;
  duplicate_group_count: number;
};

export type AdminLeadDemandBucketSummary = {
  demand_bucket: string;
  lead_count: number;
};

export type AdminLeadDuplicateGroupSummary = {
  duplicate_group_key: string;
  latest_lead_id: string;
  county_id: string;
  account_number: string;
  requested_tax_year: number;
  lead_count: number;
  latest_submitted_at: string | null;
  latest_demand_bucket: string | null;
  fallback_present: boolean;
  demand_bucket_count: number;
};

export type AdminLeadSummary = {
  lead_id: string;
  lead_event_id: string;
  submitted_at: string | null;
  county_id: string;
  account_number: string;
  requested_tax_year: number;
  served_tax_year: number | null;
  demand_bucket: string;
  context_status: string;
  source_channel: string | null;
  owner_name: string | null;
  fallback_applied: boolean;
  fallback_reason: string | null;
  email_present: boolean;
  phone_present: boolean;
  consent_to_contact: boolean;
  duplicate_group_key: string;
  duplicate_group_size: number;
};

export type AdminLeadListResponse = {
  access_scope: string;
  county_id: string | null;
  requested_tax_year: number | null;
  served_tax_year: number | null;
  demand_bucket: string | null;
  fallback_applied: boolean | null;
  source_channel: string | null;
  duplicate_only: boolean;
  quote_ready_only: boolean;
  submitted_from: string | null;
  submitted_to: string | null;
  limit: number;
  kpi_summary: AdminLeadReportingKpiSummary;
  demand_buckets: AdminLeadDemandBucketSummary[];
  duplicate_groups: AdminLeadDuplicateGroupSummary[];
  leads: AdminLeadSummary[];
};

export type AdminLeadContactSnapshot = {
  owner_name: string | null;
  email: string | null;
  phone: string | null;
  email_present: boolean;
  phone_present: boolean;
  consent_to_contact: boolean;
};

export type AdminLeadQuoteContextSnapshot = {
  context_status: string;
  demand_bucket: string;
  county_supported: boolean;
  property_supported: boolean | null;
  quote_ready: boolean;
  requested_tax_year: number;
  served_tax_year: number | null;
  tax_year_fallback_applied: boolean;
  tax_year_fallback_reason: string | null;
  parcel_id: string | null;
  property_type_code: string | null;
  protest_recommendation: string | null;
  expected_tax_savings_point: number | null;
  defensible_value_point: number | null;
};

export type AdminLeadAttributionSnapshot = {
  anonymous_session_id: string | null;
  funnel_stage: string | null;
  utm_source: string | null;
  utm_medium: string | null;
  utm_campaign: string | null;
  utm_term: string | null;
  utm_content: string | null;
};

export type AdminLeadDuplicatePeer = {
  lead_id: string;
  submitted_at: string | null;
  demand_bucket: string;
  context_status: string;
  served_tax_year: number | null;
  fallback_applied: boolean;
  source_channel: string | null;
};

export type AdminLeadDetail = {
  access_scope: string;
  lead: AdminLeadSummary;
  contact: AdminLeadContactSnapshot;
  quote_context: AdminLeadQuoteContextSnapshot;
  attribution: AdminLeadAttributionSnapshot;
  raw_event_payload: Record<string, unknown>;
  duplicate_peers: AdminLeadDuplicatePeer[];
};
