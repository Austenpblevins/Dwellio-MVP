CREATE TABLE IF NOT EXISTS county_field_mappings (
  county_field_mapping_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  source_name text NOT NULL,
  source_field text NOT NULL,
  canonical_table text NOT NULL,
  canonical_field text NOT NULL,
  transform_rule text,
  UNIQUE(county_id, source_name, source_field)
);
