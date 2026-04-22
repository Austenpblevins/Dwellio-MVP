CREATE TABLE IF NOT EXISTS instant_quote_county_tax_capability (
  county_id text NOT NULL,
  tax_year integer NOT NULL,
  exemption_normalization_confidence text NOT NULL DEFAULT 'unknown',
  over65_reliability text NOT NULL DEFAULT 'unknown',
  disabled_reliability text NOT NULL DEFAULT 'unknown',
  disabled_veteran_reliability text NOT NULL DEFAULT 'unknown',
  freeze_reliability text NOT NULL DEFAULT 'unknown',
  tax_unit_assignment_reliability text NOT NULL DEFAULT 'unknown',
  tax_rate_reliability text NOT NULL DEFAULT 'unknown',
  school_ceiling_amount_available boolean NOT NULL DEFAULT false,
  unit_exemption_policy_available boolean NOT NULL DEFAULT false,
  local_option_policy_available boolean NOT NULL DEFAULT false,
  profile_support_level text NOT NULL DEFAULT 'summary_only',
  notes text,
  generated_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (county_id, tax_year)
);

COMMENT ON TABLE instant_quote_county_tax_capability IS 'Materialized county-year tax capability matrix for V5 instant-quote staging, exposing current reliability limits before parcel tax-profile rollout.';
COMMENT ON COLUMN instant_quote_county_tax_capability.exemption_normalization_confidence IS 'County-year confidence classification for exemption normalization truth used by later tax-profile stages.';
COMMENT ON COLUMN instant_quote_county_tax_capability.over65_reliability IS 'County-year reliability of over65 / senior exemption truth for instant-quote tax handling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.disabled_reliability IS 'County-year reliability of disabled-person exemption truth for instant-quote tax handling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.disabled_veteran_reliability IS 'County-year reliability of disabled-veteran / total-exemption truth for instant-quote tax handling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.freeze_reliability IS 'County-year reliability of freeze or ceiling-related signals for instant-quote tax handling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.tax_unit_assignment_reliability IS 'County-year reliability of tax-unit assignment completeness based on the latest quote refresh.';
COMMENT ON COLUMN instant_quote_county_tax_capability.tax_rate_reliability IS 'County-year reliability of tax-rate support based on the latest quote refresh and selected basis status.';
COMMENT ON COLUMN instant_quote_county_tax_capability.school_ceiling_amount_available IS 'True only when explicit school ceiling amount or binding truth is materialized well enough for later-stage modeling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.unit_exemption_policy_available IS 'True only when unit-level exemption policy truth is materialized well enough for later-stage modeling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.local_option_policy_available IS 'True only when local-option percentage exemption policy truth is materialized well enough for later-stage modeling.';
COMMENT ON COLUMN instant_quote_county_tax_capability.profile_support_level IS 'County-year support level for tax-profile work, such as summary_only prior to parcel-level tax-profile materialization.';
COMMENT ON COLUMN instant_quote_county_tax_capability.notes IS 'Concise county-year caveats explaining why the materialized capability statuses are limited or unsupported.';
