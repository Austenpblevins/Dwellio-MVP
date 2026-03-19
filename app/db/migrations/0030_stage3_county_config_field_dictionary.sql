DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'null_handling_strategy_enum') THEN
    CREATE TYPE null_handling_strategy_enum AS ENUM (
      'allow_null',
      'reject_row',
      'use_default',
      'empty_list'
    );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS canonical_field_dictionary (
  canonical_field_dictionary_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_type text NOT NULL,
  canonical_field_code text NOT NULL UNIQUE,
  canonical_table text NOT NULL,
  canonical_section text NOT NULL,
  canonical_field text NOT NULL,
  data_type text NOT NULL,
  required_flag boolean NOT NULL DEFAULT false,
  repeatable_flag boolean NOT NULL DEFAULT false,
  null_handling_strategy null_handling_strategy_enum NOT NULL DEFAULT 'allow_null',
  dependency_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  transformation_notes text,
  description text NOT NULL,
  example_value text,
  active_flag boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (dataset_type, canonical_table, canonical_section, canonical_field)
);

ALTER TABLE county_field_mappings
  ADD COLUMN IF NOT EXISTS dataset_type text,
  ADD COLUMN IF NOT EXISTS staging_table text,
  ADD COLUMN IF NOT EXISTS canonical_field_code text REFERENCES canonical_field_dictionary(canonical_field_code) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS null_handling_strategy null_handling_strategy_enum NOT NULL DEFAULT 'allow_null',
  ADD COLUMN IF NOT EXISTS dependency_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS transformation_notes text,
  ADD COLUMN IF NOT EXISTS active_flag boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS mapping_metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_canonical_field_dictionary_lookup
  ON canonical_field_dictionary(dataset_type, canonical_table, canonical_section, canonical_field);

CREATE INDEX IF NOT EXISTS idx_county_field_mappings_dataset_lookup
  ON county_field_mappings(county_id, dataset_type, staging_table, canonical_table, canonical_field);

COMMENT ON TABLE canonical_field_dictionary IS 'Machine-readable canonical field catalog used by county config mappings and review workflows.';
COMMENT ON TABLE county_field_mappings IS 'Relational mirror for county-to-canonical field mappings. File configs remain the operational source during early county onboarding.';
COMMENT ON COLUMN county_field_mappings.canonical_field_code IS 'Canonical field dictionary code such as property_roll.parcel.account_number.';
COMMENT ON COLUMN county_field_mappings.dependency_codes IS 'Upstream canonical field codes or dataset dependencies required before this mapping can run.';

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
  ('property_roll', 'property_roll.parcel.account_number', 'parcels', 'parcel', 'account_number', 'text', true, false, 'reject_row', '[]'::jsonb, 'Canonical parcel account anchor.', 'County appraisal account number used as the current parcel identifier.', '1001001001001', true),
  ('property_roll', 'property_roll.parcel.cad_property_id', 'parcels', 'parcel', 'cad_property_id', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'County CAD property identifier when supplied.', 'HCAD-1001', true),
  ('property_roll', 'property_roll.parcel.situs_address', 'parcels', 'parcel', 'situs_address', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Parcel situs street address.', '101 Main St', true),
  ('property_roll', 'property_roll.parcel.situs_city', 'parcels', 'parcel', 'situs_city', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Parcel situs city.', 'Houston', true),
  ('property_roll', 'property_roll.parcel.situs_zip', 'parcels', 'parcel', 'situs_zip', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Parcel situs ZIP code.', '77002', true),
  ('property_roll', 'property_roll.parcel.owner_name', 'parcels', 'parcel', 'owner_name', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'Current owner name from the source roll.', 'Alex Example', true),
  ('property_roll', 'property_roll.parcel.property_type_code', 'parcels', 'parcel', 'property_type_code', 'text', false, false, 'use_default', '[]'::jsonb, 'Defaults to sfr for current MVP scope.', 'Canonical parcel property type code for county roll normalization.', 'sfr', true),
  ('property_roll', 'property_roll.parcel.property_class_code', 'parcels', 'parcel', 'property_class_code', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'County property class code.', 'A1', true),
  ('property_roll', 'property_roll.parcel.neighborhood_code', 'parcels', 'parcel', 'neighborhood_code', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'County neighborhood code.', 'HOU-001', true),
  ('property_roll', 'property_roll.parcel.subdivision_name', 'parcels', 'parcel', 'subdivision_name', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'County subdivision or subdivision-like label.', 'Downtown', true),
  ('property_roll', 'property_roll.parcel.school_district_name', 'parcels', 'parcel', 'school_district_name', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'School district name attributed to the parcel.', 'Houston ISD', true),
  ('property_roll', 'property_roll.parcel.source_record_hash', 'parcels', 'parcel', 'source_record_hash', 'text', true, false, 'reject_row', '[]'::jsonb, 'Computed hash of the source payload for lineage and idempotency.', 'Deterministic source record hash carried into canonical parcel rows.', NULL, true),
  ('property_roll', 'property_roll.address.situs_address', 'parcel_addresses', 'address', 'situs_address', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Current situs address copied into the address history table.', '101 Main St', true),
  ('property_roll', 'property_roll.address.situs_city', 'parcel_addresses', 'address', 'situs_city', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Current situs city copied into the address history table.', 'Houston', true),
  ('property_roll', 'property_roll.address.situs_zip', 'parcel_addresses', 'address', 'situs_zip', 'text', true, false, 'reject_row', '[]'::jsonb, NULL, 'Current situs ZIP copied into the address history table.', '77002', true),
  ('property_roll', 'property_roll.address.normalized_address', 'parcel_addresses', 'address', 'normalized_address', 'text', true, false, 'reject_row', '[]'::jsonb, 'Normalized with address query normalization helpers.', 'Normalized address string used for search/read-model joins.', '101 main st houston tx 77002', true),
  ('property_roll', 'property_roll.characteristics.property_type_code', 'property_characteristics', 'characteristics', 'property_type_code', 'text', false, false, 'use_default', '[]'::jsonb, NULL, 'Canonical property type code for downstream filters.', 'sfr', true),
  ('property_roll', 'property_roll.characteristics.property_class_code', 'property_characteristics', 'characteristics', 'property_class_code', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'County property class code retained alongside canonical type.', 'A1', true),
  ('property_roll', 'property_roll.characteristics.neighborhood_code', 'property_characteristics', 'characteristics', 'neighborhood_code', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'Neighborhood code used for equalization and comp grouping.', 'HOU-001', true),
  ('property_roll', 'property_roll.characteristics.subdivision_name', 'property_characteristics', 'characteristics', 'subdivision_name', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'Subdivision label retained in canonical property characteristics.', 'Downtown', true),
  ('property_roll', 'property_roll.characteristics.school_district_name', 'property_characteristics', 'characteristics', 'school_district_name', 'text', false, false, 'allow_null', '[]'::jsonb, NULL, 'School district name associated with the parcel.', 'Houston ISD', true),
  ('property_roll', 'property_roll.characteristics.homestead_flag', 'property_characteristics', 'characteristics', 'homestead_flag', 'boolean', false, false, 'allow_null', '["property_roll.exemptions.exemption_type_code"]'::jsonb, 'Derived from exemption items.', 'True when the source exemption array contains a homestead-style exemption.', 'true', true),
  ('property_roll', 'property_roll.characteristics.owner_occupied_flag', 'property_characteristics', 'characteristics', 'owner_occupied_flag', 'boolean', false, false, 'allow_null', '["property_roll.exemptions.exemption_type_code"]'::jsonb, 'Derived from homestead-style exemptions for MVP.', 'Owner occupancy proxy used until county-specific occupancy signals are ingested.', 'true', true),
  ('property_roll', 'property_roll.characteristics.primary_use_code', 'property_characteristics', 'characteristics', 'primary_use_code', 'text', false, false, 'use_default', '[]'::jsonb, 'Defaults to residential for the Harris/Fort Bend MVP.', 'Primary use code stored in canonical property characteristics.', 'residential', true),
  ('property_roll', 'property_roll.characteristics.neighborhood_group', 'property_characteristics', 'characteristics', 'neighborhood_group', 'text', false, false, 'allow_null', '[]'::jsonb, 'Defaults to neighborhood code in early stages.', 'Neighborhood grouping label used for comp/equalization rollups.', 'HOU-001', true),
  ('property_roll', 'property_roll.characteristics.effective_age', 'property_characteristics', 'characteristics', 'effective_age', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Effective age retained for feature generation.', '14', true),
  ('property_roll', 'property_roll.improvements.building_label', 'improvements', 'improvements', 'building_label', 'text', false, true, 'use_default', '[]'::jsonb, 'Defaults to Main for singleton improvement records.', 'Human-readable label for the normalized improvement entry.', 'Main', true),
  ('property_roll', 'property_roll.improvements.living_area_sf', 'improvements', 'improvements', 'living_area_sf', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Primary structure living area.', '2150', true),
  ('property_roll', 'property_roll.improvements.year_built', 'improvements', 'improvements', 'year_built', 'integer', false, true, 'allow_null', '[]'::jsonb, NULL, 'Original year built.', '2004', true),
  ('property_roll', 'property_roll.improvements.effective_year_built', 'improvements', 'improvements', 'effective_year_built', 'integer', false, true, 'allow_null', '[]'::jsonb, NULL, 'Effective year built after renovations.', '2012', true),
  ('property_roll', 'property_roll.improvements.effective_age', 'improvements', 'improvements', 'effective_age', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Improvement effective age.', '14', true),
  ('property_roll', 'property_roll.improvements.bedrooms', 'improvements', 'improvements', 'bedrooms', 'integer', false, true, 'allow_null', '[]'::jsonb, NULL, 'Bedroom count for primary structure.', '4', true),
  ('property_roll', 'property_roll.improvements.full_baths', 'improvements', 'improvements', 'full_baths', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Full bathroom count.', '2', true),
  ('property_roll', 'property_roll.improvements.half_baths', 'improvements', 'improvements', 'half_baths', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Half bathroom count.', '1', true),
  ('property_roll', 'property_roll.improvements.stories', 'improvements', 'improvements', 'stories', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Story count.', '2', true),
  ('property_roll', 'property_roll.improvements.quality_code', 'improvements', 'improvements', 'quality_code', 'text', false, true, 'allow_null', '[]'::jsonb, NULL, 'County quality code.', 'AVG', true),
  ('property_roll', 'property_roll.improvements.condition_code', 'improvements', 'improvements', 'condition_code', 'text', false, true, 'allow_null', '[]'::jsonb, NULL, 'County condition code.', 'GOOD', true),
  ('property_roll', 'property_roll.improvements.garage_spaces', 'improvements', 'improvements', 'garage_spaces', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Garage space count.', '2', true),
  ('property_roll', 'property_roll.improvements.pool_flag', 'improvements', 'improvements', 'pool_flag', 'boolean', false, true, 'allow_null', '[]'::jsonb, NULL, 'Pool presence flag.', 'false', true),
  ('property_roll', 'property_roll.land_segments.segment_num', 'land_segments', 'land_segments', 'segment_num', 'integer', false, true, 'use_default', '[]'::jsonb, 'Defaults to 1 for singleton MVP land segments.', 'Land segment ordinal within the parcel-year snapshot.', '1', true),
  ('property_roll', 'property_roll.land_segments.land_type_code', 'land_segments', 'land_segments', 'land_type_code', 'text', false, true, 'use_default', '[]'::jsonb, 'Defaults to site for MVP land segments.', 'Canonical land segment type code.', 'site', true),
  ('property_roll', 'property_roll.land_segments.land_sf', 'land_segments', 'land_segments', 'land_sf', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Land square footage.', '6400', true),
  ('property_roll', 'property_roll.land_segments.land_acres', 'land_segments', 'land_segments', 'land_acres', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Land acreage when present.', '0.1469', true),
  ('property_roll', 'property_roll.land_segments.market_value', 'land_segments', 'land_segments', 'market_value', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Land market value component.', '95000', true),
  ('property_roll', 'property_roll.value_components.component_code', 'value_components', 'value_components', 'component_code', 'text', true, true, 'reject_row', '[]'::jsonb, 'Template-driven component code for each value component entry.', 'Canonical code for the generated value component row.', 'land', true),
  ('property_roll', 'property_roll.value_components.component_label', 'value_components', 'value_components', 'component_label', 'text', false, true, 'allow_null', '[]'::jsonb, NULL, 'Human-readable component label.', 'Land Value', true),
  ('property_roll', 'property_roll.value_components.component_category', 'value_components', 'value_components', 'component_category', 'text', false, true, 'allow_null', '[]'::jsonb, NULL, 'Component category, usually market for current MVP property roll mappings.', 'market', true),
  ('property_roll', 'property_roll.value_components.market_value', 'value_components', 'value_components', 'market_value', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Component market value.', '95000', true),
  ('property_roll', 'property_roll.value_components.assessed_value', 'value_components', 'value_components', 'assessed_value', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Component assessed value.', '95000', true),
  ('property_roll', 'property_roll.value_components.taxable_value', 'value_components', 'value_components', 'taxable_value', 'numeric', false, true, 'allow_null', '["property_roll.exemptions.exemption_amount"]'::jsonb, 'Market total taxable value may subtract exemption totals.', 'Taxable value stored for each generated value component row.', '250000', true),
  ('property_roll', 'property_roll.assessment.land_value', 'parcel_assessments', 'assessment', 'land_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Land value in the parcel assessment summary.', '95000', true),
  ('property_roll', 'property_roll.assessment.improvement_value', 'parcel_assessments', 'assessment', 'improvement_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Improvement value in the parcel assessment summary.', '255000', true),
  ('property_roll', 'property_roll.assessment.market_value', 'parcel_assessments', 'assessment', 'market_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Total market value for the parcel-year.', '350000', true),
  ('property_roll', 'property_roll.assessment.assessed_value', 'parcel_assessments', 'assessment', 'assessed_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Assessed value from the source roll.', '330000', true),
  ('property_roll', 'property_roll.assessment.capped_value', 'parcel_assessments', 'assessment', 'capped_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, 'Defaults to assessed value until cap-specific feeds arrive.', 'Capped or limited assessment basis used for tax calculations.', '330000', true),
  ('property_roll', 'property_roll.assessment.appraised_value', 'parcel_assessments', 'assessment', 'appraised_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Appraised value from the source roll.', '345000', true),
  ('property_roll', 'property_roll.assessment.exemption_value_total', 'parcel_assessments', 'assessment', 'exemption_value_total', 'numeric', false, false, 'allow_null', '["property_roll.exemptions.exemption_amount"]'::jsonb, 'Summed from exemption item values.', 'Total exemption value carried onto the parcel assessment summary.', '100000', true),
  ('property_roll', 'property_roll.assessment.notice_value', 'parcel_assessments', 'assessment', 'notice_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Current notice value used by the protest workflow.', '360000', true),
  ('property_roll', 'property_roll.assessment.certified_value', 'parcel_assessments', 'assessment', 'certified_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Certified value when available.', NULL, true),
  ('property_roll', 'property_roll.assessment.prior_year_market_value', 'parcel_assessments', 'assessment', 'prior_year_market_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Prior-year market value from the source roll.', '340000', true),
  ('property_roll', 'property_roll.assessment.prior_year_assessed_value', 'parcel_assessments', 'assessment', 'prior_year_assessed_value', 'numeric', false, false, 'allow_null', '[]'::jsonb, NULL, 'Prior-year assessed value from the source roll.', '320000', true),
  ('property_roll', 'property_roll.exemptions.exemption_type_code', 'parcel_exemptions', 'exemptions', 'exemption_type_code', 'text', false, true, 'allow_null', '[]'::jsonb, NULL, 'Normalized exemption type code per exemption record.', 'homestead', true),
  ('property_roll', 'property_roll.exemptions.exemption_amount', 'parcel_exemptions', 'exemptions', 'exemption_amount', 'numeric', false, true, 'allow_null', '[]'::jsonb, NULL, 'Exemption amount per exemption record.', '100000', true)
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

DROP TRIGGER IF EXISTS set_canonical_field_dictionary_updated_at ON canonical_field_dictionary;
CREATE TRIGGER set_canonical_field_dictionary_updated_at
BEFORE UPDATE ON canonical_field_dictionary
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_county_field_mappings_updated_at ON county_field_mappings;
CREATE TRIGGER set_county_field_mappings_updated_at
BEFORE UPDATE ON county_field_mappings
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();
