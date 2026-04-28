CREATE TABLE IF NOT EXISTS fort_bend_valuation_bathroom_features (
  fort_bend_valuation_bathroom_feature_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text,
  quick_ref_id text NOT NULL,
  selected_improvement_number text NOT NULL,
  selected_improvement_rule_version text NOT NULL,
  normalization_rule_version text NOT NULL,
  source_file_version text NOT NULL,
  source_file_name text NOT NULL,
  selected_improvement_source_row_count integer NOT NULL DEFAULT 0,
  plumbing_raw numeric,
  half_baths_raw numeric,
  quarter_baths_raw numeric,
  plumbing_raw_values jsonb NOT NULL DEFAULT '[]'::jsonb,
  half_baths_raw_values jsonb NOT NULL DEFAULT '[]'::jsonb,
  quarter_baths_raw_values jsonb NOT NULL DEFAULT '[]'::jsonb,
  full_baths_derived numeric,
  half_baths_derived numeric,
  quarter_baths_derived numeric,
  bathroom_equivalent_derived numeric,
  bathroom_count_status text NOT NULL,
  bathroom_count_confidence text NOT NULL,
  bathroom_flags jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fort_bend_valuation_bathroom_features_unique UNIQUE (county_id, tax_year, quick_ref_id),
  CONSTRAINT fort_bend_valuation_bathroom_features_status_check CHECK (
    bathroom_count_status IN (
      'exact_supported',
      'reconciled_fractional_plumbing',
      'quarter_bath_present',
      'ambiguous_bathroom_count',
      'incomplete_bathroom_count',
      'no_bathroom_source'
    )
  ),
  CONSTRAINT fort_bend_valuation_bathroom_features_confidence_check CHECK (
    bathroom_count_confidence IN ('high', 'medium', 'low', 'none')
  )
);

CREATE INDEX IF NOT EXISTS idx_fb_valuation_bathroom_features_parcel_year
  ON fort_bend_valuation_bathroom_features(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_fb_valuation_bathroom_features_county_year_status
  ON fort_bend_valuation_bathroom_features(county_id, tax_year, bathroom_count_status);

COMMENT ON TABLE fort_bend_valuation_bathroom_features IS 'Additive Fort Bend-only valuation bathroom layer derived from residential segment source data without mutating canonical full_baths.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.selected_improvement_rule_version IS 'Version label for the deterministic Fort Bend primary residential improvement selector used before deriving bathroom features.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.normalization_rule_version IS 'Version label for the additive Fort Bend bathroom normalization contract used for valuation-only features.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.plumbing_raw_values IS 'Distinct raw plumbing values observed on the selected primary residential improvement.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.half_baths_raw_values IS 'Distinct raw half-bath values observed on the selected primary residential improvement.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.quarter_baths_raw_values IS 'Distinct raw quarter-bath values observed on the selected primary residential improvement.';
COMMENT ON COLUMN fort_bend_valuation_bathroom_features.bathroom_flags IS 'Diagnostic flags that preserve ambiguity, quarter-bath presence, and fractional-plumbing reconciliation details for valuation consumers.';
