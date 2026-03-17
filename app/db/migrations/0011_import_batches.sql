CREATE TABLE IF NOT EXISTS import_batches (
  import_batch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_system_id uuid REFERENCES source_systems(source_system_id),
  county_id text REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  source_filename text,
  source_checksum text,
  status text NOT NULL DEFAULT 'created',
  row_count integer,
  error_count integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);
