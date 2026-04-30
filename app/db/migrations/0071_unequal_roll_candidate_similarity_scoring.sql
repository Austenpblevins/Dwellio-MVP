ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS raw_similarity_score numeric,
  ADD COLUMN IF NOT EXISTS normalized_similarity_score numeric,
  ADD COLUMN IF NOT EXISTS scoring_version text,
  ADD COLUMN IF NOT EXISTS scoring_config_version text,
  ADD COLUMN IF NOT EXISTS similarity_score_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_similarity
  ON unequal_roll_candidates(unequal_roll_run_id, normalized_similarity_score DESC);

COMMENT ON COLUMN unequal_roll_candidates.raw_similarity_score IS 'Pre-selection similarity score on a 0-100 scale. Used only for later comparison and ranking support, not final value.';
COMMENT ON COLUMN unequal_roll_candidates.normalized_similarity_score IS 'Normalized pre-selection similarity score on a 0-1 scale.';
COMMENT ON COLUMN unequal_roll_candidates.scoring_version IS 'Version label for the unequal-roll candidate similarity scoring logic.';
COMMENT ON COLUMN unequal_roll_candidates.scoring_config_version IS 'Version label for the scoring weight/config contract used for the candidate score.';
COMMENT ON COLUMN unequal_roll_candidates.similarity_score_detail_json IS 'Structured component-level similarity score detail, including locality, physical similarity, county-aware class/quality/condition handling, plausibility influence, and conservative Fort Bend bathroom modifiers.';
