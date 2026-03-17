CREATE TABLE IF NOT EXISTS valuation_runs (
  valuation_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  run_status text NOT NULL DEFAULT 'created',
  market_value_low numeric,
  market_value_point numeric,
  market_value_high numeric,
  equity_value_low numeric,
  equity_value_point numeric,
  equity_value_high numeric,
  defensible_value_low numeric,
  defensible_value_point numeric,
  defensible_value_high numeric,
  confidence_score numeric,
  market_model_version text,
  equity_model_version text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS parcel_savings_estimates (
  parcel_savings_estimate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  valuation_run_id uuid NOT NULL REFERENCES valuation_runs(valuation_run_id) ON DELETE CASCADE,
  projected_reduction_low numeric,
  projected_reduction_point numeric,
  projected_reduction_high numeric,
  effective_tax_rate numeric,
  gross_tax_savings_low numeric,
  gross_tax_savings_point numeric,
  gross_tax_savings_high numeric,
  success_probability numeric,
  expected_tax_savings_low numeric,
  expected_tax_savings_point numeric,
  expected_tax_savings_high numeric,
  estimated_contingency_fee numeric,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS quote_explanations (
  quote_explanation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  valuation_run_id uuid NOT NULL REFERENCES valuation_runs(valuation_run_id) ON DELETE CASCADE,
  explanation_json jsonb NOT NULL,
  basis text,
  confidence_label text,
  explanation_bullets jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS protest_recommendations (
  protest_recommendation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  valuation_run_id uuid NOT NULL REFERENCES valuation_runs(valuation_run_id) ON DELETE CASCADE,
  recommendation_code text NOT NULL,
  recommendation_reason text,
  confidence numeric,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS decision_tree_results (
  decision_tree_result_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  valuation_run_id uuid NOT NULL REFERENCES valuation_runs(valuation_run_id) ON DELETE CASCADE,
  rule_code text NOT NULL,
  rule_result text NOT NULL,
  rule_score numeric,
  rule_payload_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
