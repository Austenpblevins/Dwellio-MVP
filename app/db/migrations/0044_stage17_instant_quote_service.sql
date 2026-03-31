CREATE TABLE IF NOT EXISTS instant_quote_neighborhood_stats (
  instant_quote_neighborhood_stat_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  neighborhood_code text NOT NULL,
  property_type_code text NOT NULL DEFAULT 'sfr',
  parcel_count integer NOT NULL DEFAULT 0,
  trimmed_parcel_count integer NOT NULL DEFAULT 0,
  excluded_parcel_count integer NOT NULL DEFAULT 0,
  p10_assessed_psf numeric,
  p25_assessed_psf numeric,
  p50_assessed_psf numeric,
  p75_assessed_psf numeric,
  p90_assessed_psf numeric,
  mean_assessed_psf numeric,
  median_assessed_psf numeric,
  stddev_assessed_psf numeric,
  coefficient_of_variation numeric,
  support_level text,
  support_threshold_met boolean NOT NULL DEFAULT false,
  trim_method_code text NOT NULL DEFAULT 'trim_p05_p95',
  last_refresh_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(county_id, tax_year, neighborhood_code, property_type_code)
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_neighborhood_stats_lookup
  ON instant_quote_neighborhood_stats(county_id, tax_year, neighborhood_code, property_type_code);

CREATE TABLE IF NOT EXISTS instant_quote_segment_stats (
  instant_quote_segment_stat_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  neighborhood_code text NOT NULL,
  property_type_code text NOT NULL DEFAULT 'sfr',
  size_bucket text NOT NULL,
  age_bucket text NOT NULL,
  parcel_count integer NOT NULL DEFAULT 0,
  trimmed_parcel_count integer NOT NULL DEFAULT 0,
  excluded_parcel_count integer NOT NULL DEFAULT 0,
  p10_assessed_psf numeric,
  p25_assessed_psf numeric,
  p50_assessed_psf numeric,
  p75_assessed_psf numeric,
  p90_assessed_psf numeric,
  mean_assessed_psf numeric,
  median_assessed_psf numeric,
  stddev_assessed_psf numeric,
  coefficient_of_variation numeric,
  support_level text,
  support_threshold_met boolean NOT NULL DEFAULT false,
  trim_method_code text NOT NULL DEFAULT 'trim_p05_p95',
  last_refresh_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(county_id, tax_year, neighborhood_code, property_type_code, size_bucket, age_bucket)
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_segment_stats_lookup
  ON instant_quote_segment_stats(
    county_id,
    tax_year,
    neighborhood_code,
    property_type_code,
    size_bucket,
    age_bucket
  );

CREATE TABLE IF NOT EXISTS instant_quote_request_logs (
  instant_quote_request_log_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id uuid NOT NULL UNIQUE,
  quote_version text NOT NULL,
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE SET NULL,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  basis_code text NOT NULL,
  supported boolean NOT NULL DEFAULT false,
  support_blocker_code text,
  target_psf numeric,
  subject_assessed_psf numeric,
  target_psf_segment_component numeric,
  target_psf_neighborhood_component numeric,
  equity_value_estimate numeric,
  reduction_estimate_raw numeric,
  reduction_estimate_display numeric,
  savings_estimate_raw numeric,
  savings_estimate_display numeric,
  public_savings_range_low numeric,
  public_savings_range_high numeric,
  public_estimate_bucket text,
  subject_percentile numeric,
  confidence_score numeric,
  confidence_label text,
  neighborhood_sample_count integer,
  segment_sample_count integer,
  tax_rate_source_method text,
  fallback_tier text,
  unsupported_reason text,
  explanation_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  latency_ms integer,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_request_logs_lookup
  ON instant_quote_request_logs(county_id, tax_year, account_number, created_at DESC);

DROP VIEW IF EXISTS instant_quote_subject_view;

CREATE VIEW instant_quote_subject_view AS
WITH assessment_basis AS (
  SELECT
    pa.parcel_id,
    pa.tax_year,
    pa.land_value,
    pa.improvement_value,
    COALESCE(
      pa.certified_value,
      pa.appraised_value,
      pa.assessed_value,
      pa.market_value,
      pa.notice_value
    ) AS assessment_basis_value
  FROM parcel_assessments pa
),
tax_rate_basis AS (
  SELECT
    petrv.parcel_id,
    petrv.tax_year,
    petrv.effective_tax_rate,
    petrv.source_method
  FROM parcel_effective_tax_rate_view petrv
)
SELECT
  psv.parcel_id,
  psv.county_id,
  psv.tax_year,
  psv.account_number,
  psv.address,
  psv.situs_address,
  psv.situs_city,
  psv.situs_state,
  psv.situs_zip,
  psv.neighborhood_code,
  psv.school_district_name,
  psv.property_type_code,
  psv.property_class_code,
  psv.living_area_sf,
  psv.year_built,
  psv.assessed_value,
  psv.capped_value,
  psv.notice_value,
  ab.land_value,
  ab.improvement_value,
  ab.assessment_basis_value,
  trb.effective_tax_rate,
  trb.source_method AS effective_tax_rate_source_method,
  psv.completeness_score,
  psv.public_summary_ready_flag,
  psv.homestead_flag,
  psv.over65_flag,
  psv.disabled_flag,
  psv.disabled_veteran_flag,
  psv.freeze_flag,
  psv.exemption_type_codes,
  psv.warning_codes,
  CASE
    WHEN psv.living_area_sf IS NULL OR psv.living_area_sf <= 0 THEN NULL
    WHEN psv.living_area_sf < 1400 THEN 'lt_1400'
    WHEN psv.living_area_sf < 1700 THEN '1400_1699'
    WHEN psv.living_area_sf < 2000 THEN '1700_1999'
    WHEN psv.living_area_sf < 2400 THEN '2000_2399'
    WHEN psv.living_area_sf < 2900 THEN '2400_2899'
    WHEN psv.living_area_sf < 3500 THEN '2900_3499'
    ELSE '3500_plus'
  END AS size_bucket,
  CASE
    WHEN psv.year_built IS NULL THEN 'unknown'
    WHEN psv.year_built < 1970 THEN 'pre_1970'
    WHEN psv.year_built < 1990 THEN '1970_1989'
    WHEN psv.year_built < 2005 THEN '1990_2004'
    WHEN psv.year_built < 2015 THEN '2005_2014'
    ELSE '2015_plus'
  END AS age_bucket,
  CASE
    WHEN psv.property_type_code IS DISTINCT FROM 'sfr' THEN 'unsupported_property_type'
    WHEN psv.living_area_sf IS NULL OR psv.living_area_sf <= 0 THEN 'missing_living_area'
    WHEN ab.assessment_basis_value IS NULL OR ab.assessment_basis_value <= 0 THEN 'missing_assessment_basis'
    WHEN psv.neighborhood_code IS NULL OR btrim(psv.neighborhood_code) = '' THEN 'missing_neighborhood_code'
    WHEN trb.effective_tax_rate IS NULL OR trb.effective_tax_rate <= 0 THEN 'missing_effective_tax_rate'
    ELSE NULL
  END AS support_blocker_code,
  CASE
    WHEN psv.living_area_sf IS NULL OR psv.living_area_sf <= 0 THEN NULL
    WHEN ab.assessment_basis_value IS NULL OR ab.assessment_basis_value <= 0 THEN NULL
    ELSE ab.assessment_basis_value / psv.living_area_sf
  END AS subject_assessed_psf
FROM parcel_summary_view psv
LEFT JOIN assessment_basis ab
  ON ab.parcel_id = psv.parcel_id
 AND ab.tax_year = psv.tax_year
LEFT JOIN tax_rate_basis trb
  ON trb.parcel_id = psv.parcel_id
 AND trb.tax_year = psv.tax_year;

COMMENT ON VIEW instant_quote_subject_view IS 'Instant-quote subject read model derived from parcel_summary_view plus parcel assessment/tax basis fields, preserving the public parcel-year contract while adding additive instant-quote buckets and blockers.';
COMMENT ON TABLE instant_quote_neighborhood_stats IS 'Assessment-equity neighborhood statistics for the additive instant quote service. This table is separate from sale-based neighborhood_stats.';
COMMENT ON TABLE instant_quote_segment_stats IS 'Assessment-equity size and age bucket statistics for the additive instant quote service.';
COMMENT ON TABLE instant_quote_request_logs IS 'Best-effort request and observability log for the additive public instant quote endpoint.';
