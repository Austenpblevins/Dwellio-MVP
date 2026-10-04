-- Forward repair for historical database-wide constraint-name guards.
-- Resolve targets using the active search_path so isolated schema replays receive
-- the same constraints as public. Existing constraints are never dropped/replaced.
-- Column checks permit repairing partial historical replay fixtures as well.
DO $$
BEGIN
  -- Preserve the 0069 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'eligibility_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_eligibility_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_eligibility_status_check CHECK (
        eligibility_status IN ('pending', 'eligible', 'review', 'excluded')
      );
  END IF;

  -- Preserve the 0072 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'ranking_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_ranking_status_check'
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

  -- Preserve the 0073 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'shortlist_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_shortlist_status_check'
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

  -- Preserve the 0074 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'final_selection_support_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_final_selection_support_status_check'
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

  -- Preserve the 0075 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'chosen_comp_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_chosen_comp_status_check'
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

  -- Preserve the 0076 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_runs')
      AND attname = 'final_comp_count_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_runs')
      AND conname = 'unequal_roll_runs_final_comp_count_status_check'
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

  -- Preserve the 0076 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_runs')
      AND attname = 'selection_governance_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_runs')
      AND conname = 'unequal_roll_runs_selection_governance_status_check'
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

  -- Preserve the 0077 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'adjustment_support_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_adjustment_support_status_check'
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

  -- Preserve the 0078 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_adjustments')
      AND attname = 'adjustment_reliability_flag' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_adjustments')
      AND conname = 'unequal_roll_adjustments_adjustment_reliability_flag_check'
  ) THEN
    ALTER TABLE unequal_roll_adjustments
      ADD CONSTRAINT unequal_roll_adjustments_adjustment_reliability_flag_check CHECK (
        adjustment_reliability_flag IN (
          'scaffold',
          'scaffold_review',
          'not_monetized'
        )
      );
  END IF;

  -- Preserve the 0078 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'adjustment_math_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_adjustment_math_status_check'
  ) THEN
    ALTER TABLE unequal_roll_candidates
      ADD CONSTRAINT unequal_roll_candidates_adjustment_math_status_check CHECK (
        adjustment_math_status IN (
          'not_evaluated',
          'adjusted',
          'adjusted_with_review',
          'adjusted_limited',
          'adjusted_limited_with_review',
          'excluded_from_adjustment_math'
        )
      );
  END IF;

  -- Preserve the 0079 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_runs')
      AND attname = 'final_value_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_runs')
      AND conname = 'unequal_roll_runs_final_value_status_check'
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

  -- Preserve the 0079 constraint definition; scope existence to this relation.
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('unequal_roll_candidates')
      AND attname = 'final_value_status' AND NOT attisdropped
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('unequal_roll_candidates')
      AND conname = 'unequal_roll_candidates_final_value_status_check'
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
