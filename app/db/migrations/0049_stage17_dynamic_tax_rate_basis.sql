ALTER TABLE instant_quote_subject_cache
  ADD COLUMN IF NOT EXISTS effective_tax_rate_basis_year integer REFERENCES tax_years(tax_year),
  ADD COLUMN IF NOT EXISTS effective_tax_rate_basis_reason text,
  ADD COLUMN IF NOT EXISTS effective_tax_rate_basis_fallback_applied boolean NOT NULL DEFAULT false;

ALTER TABLE instant_quote_refresh_runs
  ADD COLUMN IF NOT EXISTS tax_rate_basis_year integer REFERENCES tax_years(tax_year),
  ADD COLUMN IF NOT EXISTS tax_rate_basis_reason text,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_fallback_applied boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS requested_tax_rate_supportable_subject_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_supportable_subject_row_count integer NOT NULL DEFAULT 0;

COMMENT ON COLUMN instant_quote_subject_cache.effective_tax_rate_basis_year IS 'Tax-rate basis year used to populate the quote-year subject cache row.';
COMMENT ON COLUMN instant_quote_subject_cache.effective_tax_rate_basis_reason IS 'Deterministic basis-selection reason code captured during refresh.';
COMMENT ON COLUMN instant_quote_subject_cache.effective_tax_rate_basis_fallback_applied IS 'True when the quote-year row used a prior-year tax-rate basis.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_year IS 'Selected tax-rate basis year used for the county-year refresh.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_reason IS 'Deterministic basis-selection reason code for the county-year refresh.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_fallback_applied IS 'True when the refresh fell back to a prior-year tax-rate basis.';
COMMENT ON COLUMN instant_quote_refresh_runs.requested_tax_rate_supportable_subject_row_count IS 'Supportable current-year subject count if the requested tax year tax rates were used.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_supportable_subject_row_count IS 'Supportable current-year subject count for the selected tax-rate basis year.';
