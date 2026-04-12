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
VALUES (
  'unknown',
  'Unknown Exemption',
  'County exemption code preserved with no mapped canonical subtype yet.',
  'unknown',
  999,
  false,
  false,
  false,
  true,
  '{"aliases":["unknown"],"summary_flags":[]}'::jsonb
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
