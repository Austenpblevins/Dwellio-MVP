CREATE TABLE IF NOT EXISTS ingestion_step_runs (
  step_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id uuid NOT NULL REFERENCES import_batches(import_batch_id),
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  step_name text NOT NULL,
  status text NOT NULL,
  attempt_number integer NOT NULL DEFAULT 1,
  retry_of_step_run_id uuid REFERENCES ingestion_step_runs(step_run_id) ON DELETE SET NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  row_count integer,
  error_message text,
  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_step_runs_import_batch
  ON ingestion_step_runs(import_batch_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_step_runs_job_run
  ON ingestion_step_runs(job_run_id, started_at DESC);

COMMENT ON TABLE ingestion_step_runs IS 'Step-level ingestion workflow outcomes for canonical publish and post-commit maintenance tracking.';
