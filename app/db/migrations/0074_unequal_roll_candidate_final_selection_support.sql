ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS final_selection_support_position integer,
  ADD COLUMN IF NOT EXISTS final_selection_support_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS final_selection_support_version text,
  ADD COLUMN IF NOT EXISTS final_selection_support_config_version text,
  ADD COLUMN IF NOT EXISTS final_selection_support_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_final_selection_support_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_final_selection_support_status_check CHECK (
        final_selection_support_status IN (
          'not_evaluated',
          'selected_support',
          'review_selected_support',
          'not_selected_support',
          'excluded_from_selection_support'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_final_selection_support
  ON unequal_roll_candidates(
    unequal_roll_run_id,
    final_selection_support_status,
    final_selection_support_position
  );

COMMENT ON COLUMN unequal_roll_candidates.final_selection_support_position IS 'Pre-adjustment support position among shortlist-derived unequal-roll candidates selected into the final-selection-support set. Candidates outside the support set remain NULL.';
COMMENT ON COLUMN unequal_roll_candidates.final_selection_support_status IS 'Pre-adjustment final-selection-support status. Distinguishes selected support rows, review carry-forward rows, omitted shortlist rows, and ranking/shortlist-gated rows without implying final chosen comps or value outcomes.';
COMMENT ON COLUMN unequal_roll_candidates.final_selection_support_version IS 'Version label for the unequal-roll final-selection-support logic.';
COMMENT ON COLUMN unequal_roll_candidates.final_selection_support_config_version IS 'Version label for the final-selection-support config contract, including shortlist-input and support-count policy.';
COMMENT ON COLUMN unequal_roll_candidates.final_selection_support_detail_json IS 'Structured final-selection-support detail describing shortlist input status, support-set cutoff behavior, review carry-forward visibility, and score/ranking context without implying adjustment or value logic.';
