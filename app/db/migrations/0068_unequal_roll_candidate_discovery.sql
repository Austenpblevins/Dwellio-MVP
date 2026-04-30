CREATE TABLE IF NOT EXISTS unequal_roll_candidates (
  unequal_roll_candidate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  unequal_roll_run_id uuid NOT NULL REFERENCES unequal_roll_runs(unequal_roll_run_id) ON DELETE CASCADE,
  candidate_parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text,
  address text,
  neighborhood_code text,
  subdivision_name text,
  property_type_code text,
  property_class_code text,
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
  discovery_tier text NOT NULL,
  candidate_status text NOT NULL DEFAULT 'discovered',
  source_provenance_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  candidate_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT unequal_roll_candidates_run_candidate_unique UNIQUE (
    unequal_roll_run_id,
    candidate_parcel_id
  ),
  CONSTRAINT unequal_roll_candidates_discovery_tier_check CHECK (
    discovery_tier IN ('same_neighborhood', 'county_sfr_fallback')
  )
);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_run
  ON unequal_roll_candidates(unequal_roll_run_id, discovery_tier, candidate_status);

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_scope
  ON unequal_roll_candidates(county_id, tax_year, neighborhood_code);

COMMENT ON TABLE unequal_roll_candidates IS 'Phase 2 candidate discovery table for unequal-roll MVP. Stores discovered canonical parcel-year candidates before scoring, ranking, or adjustment logic exists.';
COMMENT ON COLUMN unequal_roll_candidates.discovery_tier IS 'Discovery geography tier used to harvest the candidate. Same-neighborhood candidates are preferred; county-level SFR fallback is bounded and persisted explicitly.';
COMMENT ON COLUMN unequal_roll_candidates.candidate_status IS 'Current discovery-stage persistence status for the candidate row. Phase 2 keeps this narrow and does not yet store scoring or final-selection outcomes.';
COMMENT ON COLUMN unequal_roll_candidates.source_provenance_json IS 'Source lineage for candidate discovery, including parcel_summary_view use, subject relationship flags, and any additive Fort Bend bathroom attachment state carried only in JSON.';
COMMENT ON COLUMN unequal_roll_candidates.candidate_snapshot_json IS 'Immutable candidate snapshot payload persisted at discovery time for later unequal-roll filtering, scoring, and adjustment phases.';
