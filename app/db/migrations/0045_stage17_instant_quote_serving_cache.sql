CREATE TABLE IF NOT EXISTS instant_quote_subject_cache (
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  address text,
  situs_address text,
  situs_city text,
  situs_state text,
  situs_zip text,
  neighborhood_code text,
  school_district_name text,
  property_type_code text,
  property_class_code text,
  living_area_sf numeric,
  year_built integer,
  assessed_value numeric,
  capped_value numeric,
  notice_value numeric,
  land_value numeric,
  improvement_value numeric,
  assessment_basis_value numeric,
  effective_tax_rate numeric,
  effective_tax_rate_source_method text,
  completeness_score numeric,
  public_summary_ready_flag boolean,
  homestead_flag boolean,
  over65_flag boolean,
  disabled_flag boolean,
  disabled_veteran_flag boolean,
  freeze_flag boolean,
  exemption_type_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  warning_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  size_bucket text,
  age_bucket text,
  support_blocker_code text,
  subject_assessed_psf numeric,
  refreshed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (parcel_id, tax_year)
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_subject_cache_lookup
  ON instant_quote_subject_cache(county_id, account_number, tax_year DESC);

CREATE INDEX IF NOT EXISTS idx_instant_quote_subject_cache_neighborhood
  ON instant_quote_subject_cache(county_id, tax_year, neighborhood_code, property_type_code);

CREATE INDEX IF NOT EXISTS idx_instant_quote_subject_cache_segment
  ON instant_quote_subject_cache(
    county_id,
    tax_year,
    neighborhood_code,
    property_type_code,
    size_bucket,
    age_bucket
  );

CREATE INDEX IF NOT EXISTS idx_instant_quote_subject_cache_support
  ON instant_quote_subject_cache(county_id, tax_year, support_blocker_code);

COMMENT ON TABLE instant_quote_subject_cache IS 'Indexed instant-quote subject serving cache populated from instant_quote_subject_view during refresh to keep the public lookup path read-model driven and fast.';
