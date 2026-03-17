CREATE TABLE IF NOT EXISTS ingest_errors (
  ingest_error_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  raw_file_id uuid REFERENCES raw_files(raw_file_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  error_stage text NOT NULL,
  error_message text NOT NULL,
  row_identifier text,
  raw_payload jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
