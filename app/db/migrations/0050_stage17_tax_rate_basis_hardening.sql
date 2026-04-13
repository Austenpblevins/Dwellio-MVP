ALTER TABLE instant_quote_refresh_runs
  ADD COLUMN IF NOT EXISTS tax_rate_quoteable_subject_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS requested_tax_rate_effective_tax_rate_coverage_ratio numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS requested_tax_rate_assignment_coverage_ratio numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_effective_tax_rate_coverage_ratio numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_assignment_coverage_ratio numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_continuity_parcel_match_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_continuity_parcel_gap_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_continuity_parcel_match_ratio numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_continuity_account_number_match_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_warning_codes text[] NOT NULL DEFAULT ARRAY[]::text[];

COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_quoteable_subject_row_count IS 'Current-year quoteable instant-quote cohort size used for tax-rate basis evaluation.';
COMMENT ON COLUMN instant_quote_refresh_runs.requested_tax_rate_effective_tax_rate_coverage_ratio IS 'Coverage ratio of positive effective tax rates for the requested year over the current-year quoteable cohort.';
COMMENT ON COLUMN instant_quote_refresh_runs.requested_tax_rate_assignment_coverage_ratio IS 'Coverage ratio of county+school assignment completeness for the requested year over the current-year quoteable cohort.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_effective_tax_rate_coverage_ratio IS 'Coverage ratio of positive effective tax rates for the selected basis year over the current-year quoteable cohort.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_assignment_coverage_ratio IS 'Coverage ratio of county+school assignment completeness for the selected basis year over the current-year quoteable cohort.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_continuity_parcel_match_row_count IS 'Count of current-year quoteable parcels with same parcel_id continuity into the selected basis year.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_continuity_parcel_gap_row_count IS 'Count of current-year quoteable parcels without same parcel_id continuity into the selected basis year.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_continuity_parcel_match_ratio IS 'Share of current-year quoteable parcels with same parcel_id continuity into the selected basis year.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_continuity_account_number_match_row_count IS 'Diagnostic count of current-year quoteable parcels lacking parcel_id continuity but matching account_number in the selected basis year.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_warning_codes IS 'Deterministic warning codes for the selected tax-rate basis year, including continuity diagnostics.';
