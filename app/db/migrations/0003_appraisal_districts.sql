CREATE TABLE IF NOT EXISTS appraisal_districts (
  appraisal_district_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  district_name text NOT NULL,
  district_code text,
  website_url text,
  is_active boolean NOT NULL DEFAULT true,
  UNIQUE(county_id, district_code)
);
