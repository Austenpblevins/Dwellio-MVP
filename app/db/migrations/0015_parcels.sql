CREATE TABLE IF NOT EXISTS property_type_codes (
  property_type_code text PRIMARY KEY,
  label text NOT NULL,
  is_residential boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS parcels (
  parcel_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  appraisal_district_id uuid REFERENCES appraisal_districts(appraisal_district_id),
  tax_year integer REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  cad_property_id text,
  geo_account_number text,
  quick_ref_id text,
  situs_address text,
  situs_city text,
  situs_state text DEFAULT 'TX',
  situs_zip text,
  owner_name text,
  property_type_code text REFERENCES property_type_codes(property_type_code),
  property_class_code text,
  neighborhood_code text,
  subdivision_name text,
  school_district_name text,
  latitude numeric,
  longitude numeric,
  geom geometry(MultiPolygon, 4326),
  active_flag boolean NOT NULL DEFAULT true,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (county_id, account_number)
);
