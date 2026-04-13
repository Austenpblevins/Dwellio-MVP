CREATE TABLE IF NOT EXISTS instant_quote_refresh_runs (
  instant_quote_refresh_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  refresh_status text NOT NULL DEFAULT 'running',
  refresh_started_at timestamptz NOT NULL DEFAULT now(),
  refresh_finished_at timestamptz,
  cache_rebuild_duration_ms integer,
  neighborhood_stats_refresh_duration_ms integer,
  segment_stats_refresh_duration_ms integer,
  total_refresh_duration_ms integer,
  source_view_row_count integer,
  subject_cache_row_count integer,
  supportable_subject_row_count integer,
  neighborhood_stats_row_count integer,
  supported_neighborhood_stats_row_count integer,
  segment_stats_row_count integer,
  supported_segment_stats_row_count integer,
  cache_view_row_delta integer,
  warning_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  error_message text,
  validated_at timestamptz,
  validation_report jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_instant_quote_refresh_runs_lookup
  ON instant_quote_refresh_runs(county_id, tax_year, refresh_started_at DESC);

CREATE INDEX IF NOT EXISTS idx_instant_quote_refresh_runs_status
  ON instant_quote_refresh_runs(refresh_status, refresh_started_at DESC);

COMMENT ON TABLE instant_quote_refresh_runs IS 'County-year refresh and validation history for the instant-quote serving cache and stats artifacts.';
