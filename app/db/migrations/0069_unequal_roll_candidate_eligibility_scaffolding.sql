ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS eligibility_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS eligibility_reason_code text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_eligibility_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_eligibility_status_check CHECK (
        eligibility_status IN ('pending', 'eligible', 'review', 'excluded')
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_eligibility
  ON unequal_roll_candidates(unequal_roll_run_id, eligibility_status, discovery_tier);

COMMENT ON COLUMN unequal_roll_candidates.eligibility_status IS 'Phase 3 discovery-stage eligibility disposition. This remains pre-scoring and pre-selection.';
COMMENT ON COLUMN unequal_roll_candidates.eligibility_reason_code IS 'Primary exclusion or review reason assigned during narrow Phase 3 discovery scaffolding.';
