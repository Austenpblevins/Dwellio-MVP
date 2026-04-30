ALTER TABLE unequal_roll_runs
  ADD COLUMN IF NOT EXISTS final_value_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS final_value_version text,
  ADD COLUMN IF NOT EXISTS final_value_config_version text,
  ADD COLUMN IF NOT EXISTS requested_roll_value numeric,
  ADD COLUMN IF NOT EXISTS requested_reduction_amount numeric,
  ADD COLUMN IF NOT EXISTS requested_reduction_pct numeric,
  ADD COLUMN IF NOT EXISTS final_value_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_runs_final_value_status_check'
  ) THEN
    ALTER TABLE unequal_roll_runs
      ADD CONSTRAINT unequal_roll_runs_final_value_status_check CHECK (
        final_value_status IN (
          'not_evaluated',
          'supported',
          'supported_with_review',
          'manual_review_required',
          'unsupported'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_runs_final_value
  ON unequal_roll_runs(
    county_id,
    tax_year,
    final_value_status
  );

ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS final_value_position integer,
  ADD COLUMN IF NOT EXISTS final_value_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS final_value_detail_json jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_final_value_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_final_value_status_check CHECK (
        final_value_status IN (
          'not_evaluated',
          'included_in_final_value',
          'included_in_final_value_with_review',
          'excluded_review_heavy',
          'excluded_likely_exclude',
          'excluded_from_final_value'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_final_value
  ON unequal_roll_candidates(
    unequal_roll_run_id,
    final_value_status,
    final_value_position
  );

COMMENT ON COLUMN unequal_roll_runs.final_value_status IS 'Run-level final unequal-roll value posture. Uses the median-of-adjusted-appraised-values methodology while distinguishing clean support, review-visible support, manual-review-required results, and unsupported runs.';
COMMENT ON COLUMN unequal_roll_runs.final_value_version IS 'Version label for the unequal-roll final value logic.';
COMMENT ON COLUMN unequal_roll_runs.final_value_config_version IS 'Version label for the unequal-roll final value config contract, including final-value-set inclusion rules and median stability thresholds.';
COMMENT ON COLUMN unequal_roll_runs.requested_roll_value IS 'Final requested unequal-roll value computed as the median of included adjusted comparable appraised values when the run reaches a supportable governed adjusted set.';
COMMENT ON COLUMN unequal_roll_runs.requested_reduction_amount IS 'Requested reduction from the subject appraised value to the final requested unequal-roll value, floored at zero.';
COMMENT ON COLUMN unequal_roll_runs.requested_reduction_pct IS 'Requested reduction as a share of the subject appraised value, floored at zero.';
COMMENT ON COLUMN unequal_roll_runs.final_value_detail_json IS 'Structured final unequal-roll value explanation, including included/excluded adjusted comps, ordered adjusted values, median calculation detail, carried-forward governance posture, and stability/QA outputs.';

COMMENT ON COLUMN unequal_roll_candidates.final_value_position IS 'Ascending order position of the candidate within the final median calculation set when included in the final unequal-roll value.';
COMMENT ON COLUMN unequal_roll_candidates.final_value_status IS 'Candidate-level final value inclusion posture. Distinguishes clean included comps, included-with-review comps, review-heavy exclusions, likely-exclude exclusions, and non-final-value rows.';
COMMENT ON COLUMN unequal_roll_candidates.final_value_detail_json IS 'Structured candidate-level final value detail carrying forward chosen-comp, source, burden, adjusted-set governance, acceptable-zone context, and bathroom-boundary context into the final median-of-adjusted-values layer.';
