CREATE TABLE IF NOT EXISTS effective_tax_rates (
  effective_tax_rate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  effective_tax_rate numeric NOT NULL,
  source_method text,
  UNIQUE(parcel_id, tax_year)
);
CREATE TABLE IF NOT EXISTS sales_raw (
  sale_raw_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  source_system_id uuid REFERENCES source_systems(source_system_id),
  raw_payload jsonb NOT NULL,
  sale_date_raw text,
  sale_price_raw text,
  restricted_flag boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS parcel_sales (
  parcel_sale_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  sale_date date,
  sale_price numeric,
  list_price numeric,
  days_on_market integer,
  sale_price_psf numeric,
  time_adjusted_price numeric,
  validity_code text,
  arms_length_flag boolean,
  restricted_flag boolean NOT NULL DEFAULT false,
  sale_price_confidence numeric,
  sale_type text,
  sale_source_category text,
  derived_from_listing_flag boolean DEFAULT false,
  derived_from_mls_flag boolean DEFAULT false,
  derived_from_deed_flag boolean DEFAULT false,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS neighborhood_stats (
  neighborhood_stat_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  neighborhood_code text NOT NULL,
  property_type_code text NOT NULL,
  period_months integer NOT NULL,
  sale_count integer NOT NULL,
  median_sale_psf numeric,
  p25_sale_psf numeric,
  p75_sale_psf numeric,
  median_dom numeric,
  median_list_to_sale numeric,
  price_std_dev numeric,
  median_sale_price numeric,
  updated_at timestamptz NOT NULL DEFAULT now()
);
