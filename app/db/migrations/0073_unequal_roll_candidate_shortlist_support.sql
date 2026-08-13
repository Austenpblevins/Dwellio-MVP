ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS shortlist_position integer,
  ADD COLUMN IF NOT EXISTS shortlist_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS shortlist_version text,
  ADD COLUMN IF NOT EXISTS shortlist_config_version text,
  ADD COLUMN IF NOT EXISTS shortlist_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_shortlist_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_shortlist_status_check CHECK (
        shortlist_status IN (
          'not_evaluated',
          'shortlisted',
          'review_shortlisted',
          'not_shortlisted',
          'excluded_from_shortlist'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_shortlist
  ON unequal_roll_candidates(unequal_roll_run_id, shortlist_status, shortlist_position);

COMMENT ON COLUMN unequal_roll_candidates.shortlist_position IS 'Pre-selection shortlist position among shortlist-included unequal-roll candidates for a run. Candidates outside the shortlist or excluded from shortlist support remain NULL.';
COMMENT ON COLUMN unequal_roll_candidates.shortlist_status IS 'Pre-selection shortlist support status. Distinguishes shortlisted, review-shortlisted, not-shortlisted, and ranking-gated candidates without implying final chosen comps.';
COMMENT ON COLUMN unequal_roll_candidates.shortlist_version IS 'Version label for the unequal-roll shortlist support logic.';
COMMENT ON COLUMN unequal_roll_candidates.shortlist_config_version IS 'Version label for the shortlist support config contract, including shortlist target size and close-score tie-break policy.';
COMMENT ON COLUMN unequal_roll_candidates.shortlist_detail_json IS 'Structured shortlist-support detail describing shortlist eligibility, cutoff behavior, close-score policy application, and score/ranking context without implying final comp selection.';
