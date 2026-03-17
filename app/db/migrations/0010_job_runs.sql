CREATE TABLE IF NOT EXISTS job_runs (
  job_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  job_name text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  row_count integer,
  error_message text,
  metadata_json jsonb
);
