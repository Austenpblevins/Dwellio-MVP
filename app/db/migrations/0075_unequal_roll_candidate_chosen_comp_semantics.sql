ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS chosen_comp_position integer,
  ADD COLUMN IF NOT EXISTS chosen_comp_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS chosen_comp_version text,
  ADD COLUMN IF NOT EXISTS chosen_comp_config_version text,
  ADD COLUMN IF NOT EXISTS chosen_comp_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_chosen_comp_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_chosen_comp_status_check CHECK (
        chosen_comp_status IN (
          'not_evaluated',
          'chosen_comp',
          'review_chosen_comp',
          'not_chosen_comp',
          'excluded_from_chosen_comp'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_chosen_comp
  ON unequal_roll_candidates(
    unequal_roll_run_id,
    chosen_comp_status,
    chosen_comp_position
  );

COMMENT ON COLUMN unequal_roll_candidates.chosen_comp_position IS 'Pre-adjustment chosen-comp position among unequal-roll candidates that survived support-set gating and entered the true chosen-comp semantics set. Non-chosen rows remain NULL.';
COMMENT ON COLUMN unequal_roll_candidates.chosen_comp_status IS 'Pre-adjustment chosen-comp semantics status. Distinguishes clean chosen comps, review carry-forward chosen comps, support-eligible near-misses, and hard-gated rows without implying adjustment or final value outcomes.';
COMMENT ON COLUMN unequal_roll_candidates.chosen_comp_version IS 'Version label for the unequal-roll chosen-comp semantics logic.';
COMMENT ON COLUMN unequal_roll_candidates.chosen_comp_config_version IS 'Version label for the chosen-comp semantics config contract, including clean-support-first carry-forward policy and chosen-comp count target.';
COMMENT ON COLUMN unequal_roll_candidates.chosen_comp_detail_json IS 'Structured chosen-comp semantics detail describing clean-support preference, review carry-forward necessity, support-set cutoff behavior, and prior shortlist/ranking/score context without implying adjustment or final value logic.';
