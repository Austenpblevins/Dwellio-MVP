ALTER TABLE unequal_roll_runs
  ADD COLUMN IF NOT EXISTS final_comp_count integer,
  ADD COLUMN IF NOT EXISTS final_comp_count_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS selection_governance_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS selection_log_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_runs_final_comp_count_status_check'
  ) THEN
    ALTER TABLE unequal_roll_runs
      ADD CONSTRAINT unequal_roll_runs_final_comp_count_status_check CHECK (
        final_comp_count_status IN (
          'not_evaluated',
          'preferred_range',
          'acceptable_range',
          'auto_supported_minimum',
          'manual_review_exception_range',
          'unsupported_below_minimum'
        )
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_runs_selection_governance_status_check'
  ) THEN
    ALTER TABLE unequal_roll_runs
      ADD CONSTRAINT unequal_roll_runs_selection_governance_status_check CHECK (
        selection_governance_status IN (
          'not_evaluated',
          'auto_supported',
          'supported_with_warnings',
          'manual_review_required',
          'unsupported'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_runs_selection_governance
  ON unequal_roll_runs(
    final_comp_count_status,
    selection_governance_status
  );

COMMENT ON COLUMN unequal_roll_runs.final_comp_count IS 'Final chosen-comp count for unequal-roll sample governance after shortlist/support/chosen-comp semantics are applied, but before adjustments or final value logic.';
COMMENT ON COLUMN unequal_roll_runs.final_comp_count_status IS 'Count-policy status for the unequal-roll final comp set. Encodes preferred range, acceptable range, auto-supported minimum, manual-review exception range, and unsupported-below-minimum semantics.';
COMMENT ON COLUMN unequal_roll_runs.selection_governance_status IS 'Overall unequal-roll selection governance status after count, locality share, fallback use, concentration warnings, and review carry-forward context are evaluated.';
COMMENT ON COLUMN unequal_roll_runs.selection_log_json IS 'Formal unequal-roll selection log artifact. Persists candidate pool counts, filters applied, failed reasons, selected-comp survival reasons, near-miss exclusion reasons, and governance metrics for packet defense and internal QA.';
