CREATE TABLE IF NOT EXISTS instant_quote_tax_rate_adoption_statuses (
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  adoption_status text NOT NULL,
  adoption_status_reason text,
  status_source text NOT NULL DEFAULT 'operator_asserted',
  source_note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (county_id, tax_year),
  CONSTRAINT instant_quote_tax_rate_adoption_statuses_status_check CHECK (
    adoption_status IN (
      'current_year_unofficial_or_proposed_rates',
      'current_year_final_adopted_rates'
    )
  )
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_tax_rate_adoption_statuses_lookup
  ON instant_quote_tax_rate_adoption_statuses(county_id, tax_year);

ALTER TABLE instant_quote_subject_cache
  ADD COLUMN IF NOT EXISTS effective_tax_rate_basis_status text,
  ADD COLUMN IF NOT EXISTS effective_tax_rate_basis_status_reason text;

ALTER TABLE instant_quote_refresh_runs
  ADD COLUMN IF NOT EXISTS tax_rate_basis_status text,
  ADD COLUMN IF NOT EXISTS tax_rate_basis_status_reason text;

COMMENT ON TABLE instant_quote_tax_rate_adoption_statuses IS 'Internal county-year tax-rate adoption truth used to distinguish same-year unofficial/proposed rates from same-year final adopted rates for instant-quote refreshes.';
COMMENT ON COLUMN instant_quote_tax_rate_adoption_statuses.adoption_status IS 'Internal same-year adoption truth. Prior-year adopted status is inferred by refresh when the selected basis year precedes the quote year.';
COMMENT ON COLUMN instant_quote_tax_rate_adoption_statuses.adoption_status_reason IS 'Operator or pipeline note describing why the same-year adoption status was asserted.';
COMMENT ON COLUMN instant_quote_tax_rate_adoption_statuses.status_source IS 'Internal provenance for the asserted same-year adoption status.';
COMMENT ON COLUMN instant_quote_tax_rate_adoption_statuses.source_note IS 'Optional supporting note or evidence pointer for the asserted same-year adoption status.';
COMMENT ON COLUMN instant_quote_subject_cache.effective_tax_rate_basis_status IS 'Internal basis-status classification for the selected tax-rate basis used on this quote-year row.';
COMMENT ON COLUMN instant_quote_subject_cache.effective_tax_rate_basis_status_reason IS 'Deterministic explanation for the internal basis-status classification on this quote-year row.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_status IS 'Internal basis-status classification for the selected tax-rate basis used on this county-year refresh.';
COMMENT ON COLUMN instant_quote_refresh_runs.tax_rate_basis_status_reason IS 'Deterministic explanation for the internal basis-status classification on this county-year refresh.';
