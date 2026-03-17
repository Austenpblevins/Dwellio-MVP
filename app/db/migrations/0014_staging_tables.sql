CREATE TABLE IF NOT EXISTS stg_county_property_raw (
  stg_county_property_raw_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  raw_payload jsonb NOT NULL,
  row_hash text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stg_county_tax_rates_raw (
  stg_county_tax_rates_raw_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  raw_payload jsonb NOT NULL,
  row_hash text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stg_sales_raw (
  stg_sales_raw_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  raw_payload jsonb NOT NULL,
  row_hash text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stg_gis_raw (
  stg_gis_raw_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  raw_payload jsonb NOT NULL,
  row_hash text,
  created_at timestamptz NOT NULL DEFAULT now()
);
