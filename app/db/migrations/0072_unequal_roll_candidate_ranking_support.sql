ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS ranking_position integer,
  ADD COLUMN IF NOT EXISTS ranking_status text NOT NULL DEFAULT 'unranked',
  ADD COLUMN IF NOT EXISTS ranking_version text,
  ADD COLUMN IF NOT EXISTS ranking_config_version text,
  ADD COLUMN IF NOT EXISTS ranking_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_ranking_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_ranking_status_check CHECK (
        ranking_status IN (
          'unranked',
          'rankable',
          'review_rankable',
          'excluded_from_ranking'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_ranking
  ON unequal_roll_candidates(unequal_roll_run_id, ranking_status, ranking_position);

COMMENT ON COLUMN unequal_roll_candidates.ranking_position IS 'Pre-selection rank position among rankable unequal-roll candidates for a run. Excluded candidates remain NULL.';
COMMENT ON COLUMN unequal_roll_candidates.ranking_status IS 'Pre-selection ranking support status. Distinguishes rankable candidates, review-rankable candidates, and candidates excluded from ranking support.';
COMMENT ON COLUMN unequal_roll_candidates.ranking_version IS 'Version label for the unequal-roll pre-selection ranking support logic.';
COMMENT ON COLUMN unequal_roll_candidates.ranking_config_version IS 'Version label for the ranking support config contract used with persisted similarity scores.';
COMMENT ON COLUMN unequal_roll_candidates.ranking_detail_json IS 'Structured ranking-support detail describing whether the candidate was rankable, what score fields drove ordering, and how eligibility context influenced pre-selection ranking support.';
