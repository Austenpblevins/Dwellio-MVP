ALTER TABLE improvements
  ADD COLUMN IF NOT EXISTS total_rooms integer;

ALTER TABLE parcel_improvements
  ADD COLUMN IF NOT EXISTS total_rooms integer;

COMMENT ON COLUMN improvements.total_rooms IS
  'Canonical total room count for the primary structure when the county source provides an explicit room-detail signal.';
COMMENT ON COLUMN parcel_improvements.total_rooms IS
  'Canonical total room count for the parcel-year primary structure when the county source provides an explicit room-detail signal.';

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
    'property_roll.improvements.total_rooms',
    'improvements',
    'improvements',
    'total_rooms',
    'integer',
    false,
    true,
    'allow_null',
    '[]'::jsonb,
    'Populated only from explicit county room-detail records; no heuristic room-count inference is allowed.',
    'Total room count for the primary structure.',
    '8',
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
