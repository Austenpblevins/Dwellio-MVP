ALTER TABLE instant_quote_subject_cache
  ADD COLUMN IF NOT EXISTS assessment_basis_source_value_type text,
  ADD COLUMN IF NOT EXISTS assessment_basis_source_year integer REFERENCES tax_years(tax_year),
  ADD COLUMN IF NOT EXISTS assessment_basis_source_reason text,
  ADD COLUMN IF NOT EXISTS assessment_basis_quality_code text;

COMMENT ON COLUMN instant_quote_subject_cache.assessment_basis_source_value_type IS 'Internal typed source value class used to populate assessment_basis_value on the quote-year cache row.';
COMMENT ON COLUMN instant_quote_subject_cache.assessment_basis_source_year IS 'Tax year from which assessment_basis_value was sourced for the quote-year cache row.';
COMMENT ON COLUMN instant_quote_subject_cache.assessment_basis_source_reason IS 'Deterministic source-selection reason code describing how assessment_basis_value was chosen.';
COMMENT ON COLUMN instant_quote_subject_cache.assessment_basis_quality_code IS 'Internal quality/provenance bucket for assessment_basis_value selection, including current-year authoritative values and prior-year fallback states.';
