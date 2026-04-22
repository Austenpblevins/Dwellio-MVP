CREATE TABLE IF NOT EXISTS instant_quote_tax_profile (
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  profile_version text NOT NULL,
  generated_at timestamptz NOT NULL DEFAULT now(),
  source_data_cutoff_at timestamptz,
  assessment_basis_value numeric(14,2),
  assessment_basis_source_value_type text,
  assessment_basis_source_year integer REFERENCES tax_years(tax_year),
  assessment_basis_source_reason text,
  market_value numeric(14,2),
  appraised_value numeric(14,2),
  capped_value numeric(14,2),
  certified_value numeric(14,2),
  notice_value numeric(14,2),
  homestead_flag boolean NOT NULL DEFAULT false,
  over65_flag boolean,
  disabled_flag boolean,
  disabled_veteran_flag boolean,
  freeze_flag boolean,
  raw_exemption_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  normalized_exemption_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  exemption_profile_quality_score integer NOT NULL DEFAULT 0,
  tax_unit_assignment_complete_flag boolean NOT NULL DEFAULT false,
  tax_rate_complete_flag boolean NOT NULL DEFAULT false,
  tax_profile_status text NOT NULL DEFAULT 'unsupported',
  tax_profile_quality_score integer NOT NULL DEFAULT 0,
  cap_gap_value numeric(14,2),
  cap_gap_pct numeric(8,6),
  homestead_cap_binding_flag boolean,
  total_exemption_flag boolean NOT NULL DEFAULT false,
  near_total_exemption_flag boolean NOT NULL DEFAULT false,
  marginal_model_type text NOT NULL DEFAULT 'unsupported_tax_profile',
  marginal_tax_rate_total numeric(10,8),
  marginal_tax_rate_school numeric(10,8),
  marginal_tax_rate_non_school numeric(10,8),
  marginal_rate_basis text,
  savings_limited_by_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  affected_unit_mask text,
  opportunity_vs_savings_state text,
  profile_warning_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  fallback_tax_profile_used_flag boolean NOT NULL DEFAULT false,
  PRIMARY KEY (parcel_id, tax_year, profile_version)
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_tax_profile_scope
  ON instant_quote_tax_profile(county_id, tax_year, profile_version);

CREATE INDEX IF NOT EXISTS idx_instant_quote_tax_profile_status
  ON instant_quote_tax_profile(county_id, tax_year, profile_version, tax_profile_status);

COMMENT ON TABLE instant_quote_tax_profile IS 'Stage 4 summary-first parcel-year tax profile used for internal V5 savings translation and quality classification without changing the public quote path yet.';
COMMENT ON COLUMN instant_quote_tax_profile.profile_version IS 'Internal tax-profile version so additive materialization can evolve independently of public quote versioning.';
COMMENT ON COLUMN instant_quote_tax_profile.source_data_cutoff_at IS 'County-year refresh timestamp used as the source-data cutoff for the materialized profile row.';
COMMENT ON COLUMN instant_quote_tax_profile.tax_profile_status IS 'Internal V5 status band derived from tax-profile quality and blocker states.';
COMMENT ON COLUMN instant_quote_tax_profile.tax_profile_quality_score IS '0-100 internal quality score used to classify whether the parcel-year profile is standard, constrained, opportunity-only, or unsupported.';
COMMENT ON COLUMN instant_quote_tax_profile.savings_limited_by_codes IS 'Internal limiters that explain why savings translation should be constrained, opportunity-only, or unsupported.';
COMMENT ON COLUMN instant_quote_tax_profile.profile_warning_codes IS 'Internal warning rollup for the materialized profile row, combining parcel, refresh, and county capability limitation signals.';
