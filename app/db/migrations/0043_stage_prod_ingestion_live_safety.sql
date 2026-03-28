ALTER TABLE import_batches
  ADD COLUMN IF NOT EXISTS dataset_type text,
  ADD COLUMN IF NOT EXISTS status_reason text;

UPDATE import_batches ib
SET dataset_type = rf.file_kind
FROM raw_files rf
WHERE rf.import_batch_id = ib.import_batch_id
  AND ib.dataset_type IS NULL;

CREATE INDEX IF NOT EXISTS idx_import_batches_county_year_dataset_created
  ON import_batches(county_id, tax_year, dataset_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_files_county_year_kind_checksum
  ON raw_files(county_id, tax_year, file_kind, checksum, created_at DESC);

COMMENT ON COLUMN import_batches.dataset_type IS 'Logical dataset type for the import batch so acquisition failures and duplicate skips remain inspectable even when no raw file row is created.';
COMMENT ON COLUMN import_batches.status_reason IS 'Operator-facing reason for the latest batch state transition, including acquisition failures, duplicate skips, validation blocks, publish decisions, and rollback outcomes.';
