CREATE TABLE IF NOT EXISTS raw_files (
  raw_file_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  county_id text REFERENCES counties(county_id),
  storage_path text,
  original_filename text,
  checksum text,
  mime_type text,
  size_bytes bigint,
  restricted_flag boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
