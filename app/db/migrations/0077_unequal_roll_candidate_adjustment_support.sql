ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS adjustment_support_position integer,
  ADD COLUMN IF NOT EXISTS adjustment_support_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS adjustment_support_version text,
  ADD COLUMN IF NOT EXISTS adjustment_support_config_version text,
  ADD COLUMN IF NOT EXISTS adjustment_support_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_adjustment_support_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_adjustment_support_status_check CHECK (
        adjustment_support_status IN (
          'not_evaluated',
          'adjustment_ready',
          'adjustment_ready_with_review',
          'adjustment_limited',
          'adjustment_limited_with_review',
          'excluded_from_adjustment_support'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_adjustment_support
  ON unequal_roll_candidates(
    unequal_roll_run_id,
    adjustment_support_status,
    adjustment_support_position
  );

COMMENT ON COLUMN unequal_roll_candidates.adjustment_support_position IS 'Pre-value adjustment-support position among chosen unequal-roll comps that are carried into adjustment scaffolding. Hard-gated non-chosen rows remain NULL.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_support_status IS 'Pre-value adjustment-support status. Distinguishes clean vs review carry-forward chosen comps and whether current county/feature support leaves the comp fully adjustment-ready or still limited for the next phase.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_support_version IS 'Version label for the unequal-roll adjustment-support scaffolding logic.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_support_config_version IS 'Version label for the unequal-roll adjustment-support config contract, including readiness, burden-scaffolding, and dispersion-scaffolding placeholders.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_support_detail_json IS 'Structured adjustment-support detail describing readiness signals, missing-data scaffolding, burden placeholders, dispersion placeholders, review carry-forward visibility, and acceptable-zone tail governance context without computing final value logic.';
