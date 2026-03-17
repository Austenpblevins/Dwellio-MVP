CREATE TABLE IF NOT EXISTS parcel_features (
  parcel_feature_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  feature_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(parcel_id, tax_year)
);
CREATE TABLE IF NOT EXISTS comp_candidate_pools (
  comp_candidate_pool_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  comp_type text NOT NULL,
  generated_at timestamptz NOT NULL DEFAULT now(),
  pool_status text,
  candidate_count_total integer,
  candidate_count_ranked integer
);
CREATE TABLE IF NOT EXISTS comp_candidates (
  comp_candidate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  comp_candidate_pool_id uuid NOT NULL REFERENCES comp_candidate_pools(comp_candidate_pool_id) ON DELETE CASCADE,
  subject_parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  comp_parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  comp_type text NOT NULL,
  rank_num integer,
  similarity_score numeric,
  distance_miles numeric,
  sale_date date,
  sale_price numeric,
  sale_price_psf numeric,
  created_at timestamptz NOT NULL DEFAULT now()
);
