CREATE TABLE IF NOT EXISTS unequal_roll_adjustments (
  unequal_roll_adjustment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  unequal_roll_run_id uuid NOT NULL REFERENCES unequal_roll_runs(unequal_roll_run_id) ON DELETE CASCADE,
  unequal_roll_candidate_id uuid NOT NULL REFERENCES unequal_roll_candidates(unequal_roll_candidate_id) ON DELETE CASCADE,
  candidate_parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  adjustment_line_order integer NOT NULL,
  adjustment_type text NOT NULL,
  source_method_code text NOT NULL,
  rate_or_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  subject_value_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  candidate_value_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  difference_value_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  signed_adjustment_amount numeric,
  adjustment_reliability_flag text NOT NULL DEFAULT 'scaffold',
  material_flag boolean NOT NULL DEFAULT false,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT unequal_roll_adjustments_run_candidate_line_unique UNIQUE (
    unequal_roll_candidate_id,
    adjustment_line_order
  )
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_adjustments_adjustment_reliability_flag_check'
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
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_adjustments_run_candidate
  ON unequal_roll_adjustments(
    unequal_roll_run_id,
    unequal_roll_candidate_id,
    adjustment_line_order
  );

ALTER TABLE unequal_roll_candidates
  ADD COLUMN IF NOT EXISTS adjustment_math_status text NOT NULL DEFAULT 'not_evaluated',
  ADD COLUMN IF NOT EXISTS adjustment_math_version text,
  ADD COLUMN IF NOT EXISTS adjustment_math_config_version text,
  ADD COLUMN IF NOT EXISTS adjustment_summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS adjusted_appraised_value numeric,
  ADD COLUMN IF NOT EXISTS total_signed_adjustment numeric,
  ADD COLUMN IF NOT EXISTS total_absolute_adjustment numeric,
  ADD COLUMN IF NOT EXISTS adjustment_pct_of_raw_value numeric,
  ADD COLUMN IF NOT EXISTS material_adjustment_count integer,
  ADD COLUMN IF NOT EXISTS nontrivial_adjustment_sources_count integer;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'unequal_roll_candidates_adjustment_math_status_check'
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
END
$$;

CREATE INDEX IF NOT EXISTS idx_unequal_roll_candidates_adjustment_math
  ON unequal_roll_candidates(
    unequal_roll_run_id,
    adjustment_math_status,
    adjustment_support_position
  );

COMMENT ON TABLE unequal_roll_adjustments IS 'Pre-final-value unequal-roll adjustment line items. Persists per-comp channel structure, scaffold source/basis detail, signed adjustment amounts where currently supportable, and explicit non-monetized channels for later governed refinement.';
COMMENT ON COLUMN unequal_roll_adjustments.source_method_code IS 'Explicit adjustment source/basis method for the persisted line item. Early scaffold phases use transparent fallback method codes rather than opaque inferred rates.';
COMMENT ON COLUMN unequal_roll_adjustments.rate_or_basis_json IS 'Structured rate or basis payload for the adjustment line, including formula code, fallback constants, and governance context.';
COMMENT ON COLUMN unequal_roll_adjustments.signed_adjustment_amount IS 'Signed adjustment amount applied to the comp for this channel in the current scaffold phase. May remain NULL for non-monetized or review-only channels.';
COMMENT ON COLUMN unequal_roll_adjustments.adjustment_reliability_flag IS 'Reliability posture for the persisted adjustment line. Scaffold phases distinguish monetized scaffold lines, review-only scaffold lines, and non-monetized guardrail lines.';

COMMENT ON COLUMN unequal_roll_candidates.adjustment_math_status IS 'Pre-final-value adjustment-math status for chosen unequal-roll comps. Distinguishes clean vs review carry-forward comps and whether the current channel/rate support leaves the comp fully adjusted or still limited.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_math_version IS 'Version label for the unequal-roll adjustment-math scaffolding logic.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_math_config_version IS 'Version label for the unequal-roll adjustment-math config contract, including scaffold formulas, burden thresholds, and dispersion-scaffolding rules.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_summary_json IS 'Structured per-comp adjustment summary describing line-item coverage, burden totals, dispersion/outlier scaffolding, review carry-forward visibility, and acceptable-zone governance carry-forward without computing the final unequal-roll value conclusion.';
COMMENT ON COLUMN unequal_roll_candidates.adjusted_appraised_value IS 'Per-comp adjusted appraised value scaffolded from persisted line-item adjustments. This is an intermediate comp-level normalization artifact, not the final requested unequal-roll value.';
COMMENT ON COLUMN unequal_roll_candidates.total_signed_adjustment IS 'Sum of signed monetized adjustment lines for the comp in the current scaffold phase.';
COMMENT ON COLUMN unequal_roll_candidates.total_absolute_adjustment IS 'Sum of absolute monetized adjustment lines for the comp in the current scaffold phase.';
COMMENT ON COLUMN unequal_roll_candidates.adjustment_pct_of_raw_value IS 'Absolute adjustment burden as a share of raw appraised value for the comp in the current scaffold phase.';
COMMENT ON COLUMN unequal_roll_candidates.material_adjustment_count IS 'Count of monetized adjustment channels whose absolute magnitude is material under the current scaffold policy.';
COMMENT ON COLUMN unequal_roll_candidates.nontrivial_adjustment_sources_count IS 'Count of monetized adjustment channels with nontrivial signed impact under the current scaffold policy.';
