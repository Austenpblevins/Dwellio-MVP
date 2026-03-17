CREATE TABLE IF NOT EXISTS parcel_improvements (
  parcel_improvement_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  living_area_sf numeric,
  year_built integer,
  effective_year_built integer,
  effective_age numeric,
  bedrooms integer,
  full_baths numeric,
  half_baths numeric,
  stories numeric,
  quality_code text,
  condition_code text,
  garage_spaces numeric,
  pool_flag boolean,
  UNIQUE(parcel_id, tax_year)
);
CREATE TABLE IF NOT EXISTS parcel_lands (
  parcel_land_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  land_sf numeric,
  land_acres numeric,
  frontage_sf numeric,
  depth_sf numeric,
  UNIQUE(parcel_id, tax_year)
);
CREATE TABLE IF NOT EXISTS parcel_assessments (
  parcel_assessment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  land_value numeric,
  improvement_value numeric,
  market_value numeric,
  assessed_value numeric,
  capped_value numeric,
  appraised_value numeric,
  exemption_value_total numeric,
  notice_value numeric,
  certified_value numeric,
  prior_year_market_value numeric,
  prior_year_assessed_value numeric,
  homestead_flag boolean,
  UNIQUE(parcel_id, tax_year)
);
CREATE TABLE IF NOT EXISTS parcel_exemptions (
  parcel_exemption_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  exemption_type_code text,
  exemption_amount numeric,
  UNIQUE(parcel_id, tax_year, exemption_type_code)
);
