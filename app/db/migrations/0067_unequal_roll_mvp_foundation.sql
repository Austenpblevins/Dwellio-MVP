CREATE TABLE IF NOT EXISTS unequal_roll_runs (
  unequal_roll_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  run_status text NOT NULL DEFAULT 'pending',
  readiness_status text NOT NULL DEFAULT 'pending',
  support_status text NOT NULL DEFAULT 'pending',
  support_blocker_code text,
  model_version text NOT NULL,
  config_version text NOT NULL,
  source_coverage_status text NOT NULL DEFAULT 'pending',
  subject_snapshot_status text NOT NULL DEFAULT 'pending',
  finalized_for_packet boolean NOT NULL DEFAULT false,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT unequal_roll_runs_run_status_check CHECK (
    run_status IN ('pending', 'completed', 'blocked')
  ),
  CONSTRAINT unequal_roll_runs_readiness_status_check CHECK (
    readiness_status IN ('pending', 'ready', 'not_ready')
  ),
  CONSTRAINT unequal_roll_runs_support_status_check CHECK (
    support_status IN (
      'pending',
      'supported',
      'supported_with_review',
      'manual_review_required',
      'unsupported'
    )
  ),
  CONSTRAINT unequal_roll_runs_source_coverage_status_check CHECK (
    source_coverage_status IN (
      'pending',
      'canonical_snapshot_only',
      'canonical_snapshot_with_additive_bathroom_metadata',
      'canonical_snapshot_with_missing_additive_bathroom_metadata',
      'missing_subject_source'
    )
  ),
  CONSTRAINT unequal_roll_runs_subject_snapshot_status_check CHECK (
    subject_snapshot_status IN ('pending', 'completed', 'missing_subject_source')
  )
);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_runs_scope
  ON unequal_roll_runs(county_id, tax_year, parcel_id);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_runs_status
  ON unequal_roll_runs(county_id, tax_year, run_status, support_status);

CREATE TABLE IF NOT EXISTS unequal_roll_subject_snapshots (
  unequal_roll_subject_snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  unequal_roll_run_id uuid NOT NULL REFERENCES unequal_roll_runs(unequal_roll_run_id) ON DELETE CASCADE,
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  address text,
  property_type_code text,
  property_class_code text,
  neighborhood_code text,
  subdivision_name text,
  school_district_name text,
  living_area_sf numeric,
  year_built integer,
  effective_age numeric,
  bedrooms integer,
  full_baths numeric,
  half_baths numeric,
  total_rooms integer,
  stories numeric,
  quality_code text,
  condition_code text,
  pool_flag boolean,
  land_sf numeric,
  land_acres numeric,
  market_value numeric,
  assessed_value numeric,
  appraised_value numeric,
  certified_value numeric,
  notice_value numeric,
  exemption_value_total numeric,
  homestead_flag boolean,
  over65_flag boolean,
  disabled_flag boolean,
  disabled_veteran_flag boolean,
  freeze_flag boolean,
  subject_appraised_psf numeric,
  valuation_bathroom_features_json jsonb,
  snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_provenance_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT unequal_roll_subject_snapshots_run_unique UNIQUE (unequal_roll_run_id)
);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_subject_snapshots_scope
  ON unequal_roll_subject_snapshots(county_id, tax_year, parcel_id);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_subject_snapshots_run
  ON unequal_roll_subject_snapshots(unequal_roll_run_id, county_id, tax_year);

COMMENT ON TABLE unequal_roll_runs IS 'Phase 1 foundation table for internal roll-based unequal-appraisal runs, storing run status and subject-snapshot readiness without introducing comp or valuation logic yet.';
COMMENT ON COLUMN unequal_roll_runs.summary_json IS 'Compact run summary payload for unequal-roll MVP foundation status, including fallback, blockers, support-review classification, and additive-support attachment state.';
COMMENT ON TABLE unequal_roll_subject_snapshots IS 'Immutable run-scoped unequal-roll subject snapshot built from canonical parcel-year data for later candidate selection and adjustment phases.';
COMMENT ON COLUMN unequal_roll_subject_snapshots.valuation_bathroom_features_json IS 'Additive county-specific valuation metadata. Fort Bend bathroom support is stored here without mutating canonical full_baths.';
COMMENT ON COLUMN unequal_roll_subject_snapshots.snapshot_json IS 'Structured immutable snapshot payload mirroring the persisted subject fields plus readiness details for future unequal-roll phases.';
COMMENT ON COLUMN unequal_roll_subject_snapshots.source_provenance_json IS 'Source lineage for the unequal-roll subject snapshot, including view/table inputs, fallback state, and additive Fort Bend bathroom attachment details.';
