ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS eligibility_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN unequal_roll_candidates.eligibility_detail_json IS 'Structured Phase 4 explanation scaffold for pre-scoring candidate plausibility, including primary and secondary reasons, threshold observations, and status-aware Fort Bend bathroom review detail.';
