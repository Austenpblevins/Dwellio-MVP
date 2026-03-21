ALTER TABLE parcel_exemptions
  ADD COLUMN IF NOT EXISTS raw_exemption_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS source_entry_count integer NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS amount_missing_flag boolean NOT NULL DEFAULT false;

UPDATE parcel_exemptions
SET
  raw_exemption_codes = CASE
    WHEN raw_exemption_codes IS NULL OR cardinality(raw_exemption_codes) = 0 THEN ARRAY[COALESCE(exemption_type_code, 'unknown')]
    ELSE raw_exemption_codes
  END,
  source_entry_count = GREATEST(COALESCE(source_entry_count, 1), 1),
  amount_missing_flag = COALESCE(amount_missing_flag, false) OR (granted_flag AND exemption_amount IS NULL);

CREATE INDEX IF NOT EXISTS idx_parcel_exemptions_type_lookup
  ON parcel_exemptions(tax_year, exemption_type_code);

INSERT INTO exemption_types (
  exemption_type_code,
  label,
  description,
  category,
  display_order,
  is_homestead_related,
  is_senior_related,
  is_disabled_related,
  active_flag,
  metadata_json
)
VALUES
  (
    'homestead',
    'Homestead',
    'Residence homestead exemption applied to an owner-occupied parcel.',
    'residential',
    10,
    true,
    false,
    false,
    true,
    '{"aliases":["hs","hs_amt","homestead","residence_homestead"],"summary_flags":["homestead"]}'::jsonb
  ),
  (
    'over65',
    'Over 65',
    'Senior or over-65 exemption.',
    'residential',
    20,
    false,
    true,
    false,
    true,
    '{"aliases":["ov65","ov65_amt","over65","over_65"],"summary_flags":["over65"]}'::jsonb
  ),
  (
    'disabled_person',
    'Disabled Person',
    'Disabled person exemption.',
    'residential',
    30,
    false,
    false,
    true,
    true,
    '{"aliases":["disabled","dp","dp_amt","disabled_person"],"summary_flags":["disabled"]}'::jsonb
  ),
  (
    'disabled_veteran',
    'Disabled Veteran',
    'Disabled veteran exemption.',
    'residential',
    40,
    false,
    false,
    false,
    true,
    '{"aliases":["dv","dv_amt","disabled_vet","disabled_veteran"],"summary_flags":["disabled_veteran"]}'::jsonb
  ),
  (
    'surviving_spouse',
    'Surviving Spouse',
    'Surviving spouse exemption.',
    'residential',
    50,
    false,
    false,
    false,
    true,
    '{"aliases":["surviving_spouse","surv_spouse"],"summary_flags":[]}'::jsonb
  ),
  (
    'ag',
    'Agricultural',
    'Agricultural or open-space exemption.',
    'agricultural',
    60,
    false,
    false,
    false,
    true,
    '{"aliases":["ag","ag_use","agricultural"],"summary_flags":[]}'::jsonb
  ),
  (
    'freeze_ceiling',
    'Freeze Ceiling',
    'Tax ceiling or freeze-related status.',
    'residential',
    70,
    false,
    false,
    false,
    true,
    '{"aliases":["freeze","freeze_ceiling","tax_ceiling","ceiling"],"summary_flags":["freeze"]}'::jsonb
  )
ON CONFLICT (exemption_type_code) DO UPDATE
SET
  label = EXCLUDED.label,
  description = EXCLUDED.description,
  category = EXCLUDED.category,
  display_order = EXCLUDED.display_order,
  is_homestead_related = EXCLUDED.is_homestead_related,
  is_senior_related = EXCLUDED.is_senior_related,
  is_disabled_related = EXCLUDED.is_disabled_related,
  active_flag = EXCLUDED.active_flag,
  metadata_json = COALESCE(exemption_types.metadata_json, '{}'::jsonb) || EXCLUDED.metadata_json,
  updated_at = now();

INSERT INTO canonical_field_dictionary (
  dataset_type,
  canonical_field_code,
  canonical_table,
  canonical_section,
  canonical_field,
  data_type,
  required_flag,
  repeatable_flag,
  null_handling_strategy,
  dependency_codes,
  transformation_notes,
  description,
  example_value,
  active_flag
)
VALUES
  (
    'property_roll',
    'property_roll.exemptions.raw_exemption_code',
    'parcel_exemptions',
    'exemptions',
    'raw_exemption_code',
    'text',
    false,
    true,
    'allow_null',
    '[]'::jsonb,
    'Preserved alongside normalized exemption_type_code for auditability and county-specific tax computation follow-up.',
    'County-native exemption code or source column identifier for the exemption entry.',
    'HS',
    true
  )
ON CONFLICT (canonical_field_code) DO UPDATE
SET
  canonical_table = EXCLUDED.canonical_table,
  canonical_section = EXCLUDED.canonical_section,
  canonical_field = EXCLUDED.canonical_field,
  data_type = EXCLUDED.data_type,
  required_flag = EXCLUDED.required_flag,
  repeatable_flag = EXCLUDED.repeatable_flag,
  null_handling_strategy = EXCLUDED.null_handling_strategy,
  dependency_codes = EXCLUDED.dependency_codes,
  transformation_notes = EXCLUDED.transformation_notes,
  description = EXCLUDED.description,
  example_value = EXCLUDED.example_value,
  active_flag = EXCLUDED.active_flag,
  updated_at = now();

CREATE OR REPLACE VIEW parcel_exemption_rollup_view AS
WITH parcel_years AS (
  SELECT DISTINCT pys.parcel_id, pys.tax_year
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true

  UNION

  SELECT DISTINCT pa.parcel_id, pa.tax_year
  FROM parcel_assessments pa

  UNION

  SELECT DISTINCT pe.parcel_id, pe.tax_year
  FROM parcel_exemptions pe
),
exemption_base AS (
  SELECT
    py.parcel_id,
    py.tax_year,
    p.county_id,
    p.account_number,
    pe.parcel_exemption_id,
    pe.exemption_type_code,
    pe.exemption_amount,
    pe.granted_flag,
    pe.raw_exemption_codes,
    pe.source_entry_count,
    pe.amount_missing_flag,
    COALESCE(et.metadata_json, '{}'::jsonb) AS exemption_metadata_json
  FROM parcel_years py
  JOIN parcels p
    ON p.parcel_id = py.parcel_id
  LEFT JOIN parcel_exemptions pe
    ON pe.parcel_id = py.parcel_id
   AND pe.tax_year = py.tax_year
  LEFT JOIN exemption_types et
    ON et.exemption_type_code = pe.exemption_type_code
),
raw_codes AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    array_agg(DISTINCT raw_code ORDER BY raw_code) AS raw_exemption_codes
  FROM parcel_exemptions pe
  CROSS JOIN LATERAL unnest(
    CASE
      WHEN pe.raw_exemption_codes IS NULL OR cardinality(pe.raw_exemption_codes) = 0
        THEN ARRAY[COALESCE(pe.exemption_type_code, '')]
      ELSE pe.raw_exemption_codes
    END
  ) AS raw_code
  WHERE btrim(raw_code) <> ''
  GROUP BY pe.parcel_id, pe.tax_year
),
rollup_core AS (
  SELECT
    eb.county_id,
    eb.parcel_id,
    eb.tax_year,
    eb.account_number,
    COUNT(eb.parcel_exemption_id) AS exemption_record_count,
    COUNT(eb.parcel_exemption_id) FILTER (WHERE eb.granted_flag) AS granted_exemption_count,
    COALESCE(SUM(eb.exemption_amount) FILTER (WHERE eb.granted_flag AND eb.exemption_amount IS NOT NULL), 0::numeric) AS granted_exemption_amount_total,
    COALESCE(
      array_agg(DISTINCT eb.exemption_type_code ORDER BY eb.exemption_type_code)
      FILTER (WHERE eb.exemption_type_code IS NOT NULL),
      ARRAY[]::text[]
    ) AS exemption_type_codes,
    COALESCE(raw_codes.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
    COALESCE(SUM(eb.source_entry_count) FILTER (WHERE eb.parcel_exemption_id IS NOT NULL), 0)::integer AS source_entry_count,
    COALESCE(
      BOOL_OR((eb.exemption_metadata_json -> 'summary_flags') ? 'homestead')
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS homestead_flag,
    COALESCE(
      BOOL_OR((eb.exemption_metadata_json -> 'summary_flags') ? 'over65')
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS over65_flag,
    COALESCE(
      BOOL_OR((eb.exemption_metadata_json -> 'summary_flags') ? 'disabled')
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS disabled_flag,
    COALESCE(
      BOOL_OR((eb.exemption_metadata_json -> 'summary_flags') ? 'disabled_veteran')
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS disabled_veteran_flag,
    COALESCE(
      BOOL_OR((eb.exemption_metadata_json -> 'summary_flags') ? 'freeze')
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS freeze_flag,
    COUNT(*) FILTER (
      WHERE eb.parcel_exemption_id IS NOT NULL
        AND (eb.raw_exemption_codes IS NULL OR cardinality(eb.raw_exemption_codes) = 0)
    ) > 0 AS missing_raw_code_flag,
    COALESCE(
      BOOL_OR(eb.amount_missing_flag OR (eb.granted_flag AND eb.exemption_amount IS NULL))
      FILTER (WHERE eb.parcel_exemption_id IS NOT NULL),
      false
    ) AS missing_amount_flag,
    pa.exemption_value_total AS assessment_exemption_value_total,
    pa.homestead_flag AS assessment_homestead_flag
  FROM exemption_base eb
  LEFT JOIN raw_codes
    ON raw_codes.parcel_id = eb.parcel_id
   AND raw_codes.tax_year = eb.tax_year
  LEFT JOIN parcel_assessments pa
    ON pa.parcel_id = eb.parcel_id
   AND pa.tax_year = eb.tax_year
  GROUP BY
    eb.county_id,
    eb.parcel_id,
    eb.tax_year,
    eb.account_number,
    raw_codes.raw_exemption_codes,
    pa.exemption_value_total,
    pa.homestead_flag
)
SELECT
  rc.county_id,
  rc.parcel_id,
  rc.tax_year,
  rc.account_number,
  rc.exemption_record_count,
  rc.granted_exemption_count,
  rc.granted_exemption_amount_total,
  rc.exemption_type_codes,
  rc.raw_exemption_codes,
  rc.source_entry_count,
  rc.homestead_flag,
  rc.over65_flag,
  rc.disabled_flag,
  rc.disabled_veteran_flag,
  rc.freeze_flag,
  rc.assessment_exemption_value_total,
  rc.assessment_homestead_flag,
  rc.missing_raw_code_flag,
  rc.missing_amount_flag,
  (
    rc.assessment_exemption_value_total IS NOT NULL
    AND ABS(rc.granted_exemption_amount_total - rc.assessment_exemption_value_total) > 0.01
  ) AS assessment_total_mismatch_flag,
  (
    rc.assessment_homestead_flag IS NOT NULL
    AND rc.assessment_homestead_flag IS DISTINCT FROM rc.homestead_flag
  ) AS homestead_flag_mismatch_flag,
  (
    rc.freeze_flag
    AND NOT (rc.over65_flag OR rc.disabled_flag OR rc.disabled_veteran_flag)
  ) AS freeze_without_qualifying_exemption_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE
        WHEN rc.assessment_exemption_value_total IS NOT NULL
          AND rc.assessment_exemption_value_total > 0
          AND rc.exemption_record_count = 0
        THEN 'missing_exemption_records'
      END,
      CASE WHEN rc.missing_raw_code_flag THEN 'missing_raw_exemption_code' END,
      CASE WHEN rc.missing_amount_flag THEN 'missing_exemption_amount' END,
      CASE
        WHEN rc.assessment_exemption_value_total IS NOT NULL
          AND ABS(rc.granted_exemption_amount_total - rc.assessment_exemption_value_total) > 0.01
        THEN 'assessment_exemption_total_mismatch'
      END,
      CASE
        WHEN rc.assessment_homestead_flag IS NOT NULL
          AND rc.assessment_homestead_flag IS DISTINCT FROM rc.homestead_flag
        THEN 'homestead_flag_mismatch'
      END,
      CASE
        WHEN rc.freeze_flag
          AND NOT (rc.over65_flag OR rc.disabled_flag OR rc.disabled_veteran_flag)
        THEN 'freeze_without_qualifying_exemption'
      END
    ],
    NULL
  ) AS qa_issue_codes,
  cardinality(
    ARRAY_REMOVE(
      ARRAY[
        CASE
          WHEN rc.assessment_exemption_value_total IS NOT NULL
            AND rc.assessment_exemption_value_total > 0
            AND rc.exemption_record_count = 0
          THEN 'missing_exemption_records'
        END,
        CASE WHEN rc.missing_raw_code_flag THEN 'missing_raw_exemption_code' END,
        CASE WHEN rc.missing_amount_flag THEN 'missing_exemption_amount' END,
        CASE
          WHEN rc.assessment_exemption_value_total IS NOT NULL
            AND ABS(rc.granted_exemption_amount_total - rc.assessment_exemption_value_total) > 0.01
          THEN 'assessment_exemption_total_mismatch'
        END,
        CASE
          WHEN rc.assessment_homestead_flag IS NOT NULL
            AND rc.assessment_homestead_flag IS DISTINCT FROM rc.homestead_flag
          THEN 'homestead_flag_mismatch'
        END,
        CASE
          WHEN rc.freeze_flag
            AND NOT (rc.over65_flag OR rc.disabled_flag OR rc.disabled_veteran_flag)
          THEN 'freeze_without_qualifying_exemption'
        END
      ],
      NULL
    )
  ) > 0 AS has_qa_issue
FROM rollup_core rc;

COMMENT ON VIEW parcel_exemption_rollup_view IS 'Parcel-year exemption rollup with summary flags, preserved raw exemption codes, and QA issue markers for exemption normalization.';
