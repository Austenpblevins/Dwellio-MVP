ALTER TABLE parcel_taxing_units
  ADD COLUMN IF NOT EXISTS assignment_reason_code text,
  ADD COLUMN IF NOT EXISTS match_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO taxing_unit_types (unit_type_code, label, description, display_order)
VALUES
  ('county', 'County', 'County-level taxing authority.', 10),
  ('city', 'City', 'City or municipal taxing authority.', 20),
  ('school', 'School District', 'School district taxing authority.', 30),
  ('mud', 'MUD', 'Municipal utility district taxing authority.', 40),
  ('special', 'Special District', 'Special district or other add-on taxing authority.', 50)
ON CONFLICT (unit_type_code) DO UPDATE
SET
  label = EXCLUDED.label,
  description = EXCLUDED.description,
  display_order = EXCLUDED.display_order,
  updated_at = now();

INSERT INTO source_systems (
  source_system_code,
  source_system_name,
  restricted_flag,
  source_type,
  county_id,
  entity_coverage,
  official_url,
  access_method,
  file_format,
  cadence,
  reliability_tier,
  auth_required,
  manual_only,
  parser_module_name,
  adapter_name,
  active_flag,
  metadata_json
)
VALUES
  (
    'HCAD_TAX_RATES',
    'HCAD Tax Rates',
    false,
    'county_tax_rates',
    'harris',
    ARRAY['tax_unit', 'tax_rate', 'parcel_tax_assignment_hint'],
    'https://hcad.org',
    'fixture_json',
    'json',
    'annual',
    'development_fixture',
    false,
    false,
    'app.county_adapters.harris.parse',
    'HarrisCountyAdapter',
    true,
    '{"stage": "stage6", "source_family": "tax_rates"}'::jsonb
  ),
  (
    'FBCAD_TAX_RATES',
    'FBCAD Tax Rates',
    false,
    'county_tax_rates',
    'fort_bend',
    ARRAY['tax_unit', 'tax_rate', 'parcel_tax_assignment_hint'],
    'https://www.fbcad.org',
    'fixture_csv',
    'csv',
    'annual',
    'development_fixture',
    false,
    false,
    'app.county_adapters.fort_bend.parse',
    'FortBendCountyAdapter',
    true,
    '{"stage": "stage6", "source_family": "tax_rates"}'::jsonb
  )
ON CONFLICT (source_system_code) DO UPDATE
SET
  source_system_name = EXCLUDED.source_system_name,
  source_type = EXCLUDED.source_type,
  county_id = EXCLUDED.county_id,
  entity_coverage = EXCLUDED.entity_coverage,
  official_url = EXCLUDED.official_url,
  access_method = EXCLUDED.access_method,
  file_format = EXCLUDED.file_format,
  cadence = EXCLUDED.cadence,
  reliability_tier = EXCLUDED.reliability_tier,
  auth_required = EXCLUDED.auth_required,
  manual_only = EXCLUDED.manual_only,
  parser_module_name = EXCLUDED.parser_module_name,
  adapter_name = EXCLUDED.adapter_name,
  active_flag = EXCLUDED.active_flag,
  metadata_json = EXCLUDED.metadata_json;

CREATE INDEX IF NOT EXISTS idx_parcel_taxing_units_reason_lookup
  ON parcel_taxing_units(tax_year, assignment_reason_code);

CREATE OR REPLACE VIEW v_parcel_tax_component_breakdown AS
SELECT
  p.county_id,
  ptu.parcel_id,
  ptu.tax_year,
  p.account_number,
  tu.unit_type_code,
  tu.unit_code,
  tu.unit_name,
  tr.rate_component,
  COALESCE(tr.rate_value, tr.rate_per_100 / 100.0) AS rate_value,
  tr.rate_per_100,
  ptu.assignment_method,
  ptu.assignment_confidence,
  ptu.is_primary,
  ptu.assignment_reason_code,
  ptu.match_basis_json
FROM parcel_taxing_units ptu
JOIN parcels p
  ON p.parcel_id = ptu.parcel_id
JOIN taxing_units tu
  ON tu.taxing_unit_id = ptu.taxing_unit_id
JOIN tax_rates tr
  ON tr.taxing_unit_id = ptu.taxing_unit_id
 AND tr.tax_year = ptu.tax_year
 AND tr.is_current = true;

CREATE OR REPLACE VIEW v_effective_tax_rate_rollup AS
SELECT
  county_id,
  parcel_id,
  tax_year,
  account_number,
  SUM(rate_value) AS effective_tax_rate,
  jsonb_agg(
    jsonb_build_object(
      'unit_type_code', unit_type_code,
      'unit_code', unit_code,
      'unit_name', unit_name,
      'rate_component', rate_component,
      'rate_value', rate_value,
      'rate_per_100', rate_per_100,
      'assignment_method', assignment_method,
      'assignment_confidence', assignment_confidence,
      'assignment_reason_code', assignment_reason_code,
      'is_primary', is_primary
    )
    ORDER BY unit_type_code, unit_name, rate_component
  ) AS component_breakdown_json
FROM v_parcel_tax_component_breakdown
GROUP BY county_id, parcel_id, tax_year, account_number;

CREATE OR REPLACE VIEW v_parcel_tax_assignment_qa AS
SELECT
  p.county_id,
  pys.parcel_id,
  pys.tax_year,
  p.account_number,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'county') AS county_assignment_count,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'city') AS city_assignment_count,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'school') AS school_assignment_count,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'mud') AS mud_assignment_count,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'special') AS special_assignment_count,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'county') = 0 AS missing_county_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'city') = 0 AS missing_city_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'school') = 0 AS missing_school_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'mud') = 0 AS missing_mud_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'county') > 1 AS conflicting_county_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'city') > 1 AS conflicting_city_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'school') > 1 AS conflicting_school_assignment,
  COUNT(*) FILTER (WHERE tu.unit_type_code = 'mud') > 1 AS conflicting_mud_assignment
FROM parcel_year_snapshots pys
JOIN parcels p
  ON p.parcel_id = pys.parcel_id
LEFT JOIN parcel_taxing_units ptu
  ON ptu.parcel_id = pys.parcel_id
 AND ptu.tax_year = pys.tax_year
LEFT JOIN taxing_units tu
  ON tu.taxing_unit_id = ptu.taxing_unit_id
WHERE pys.is_current = true
GROUP BY p.county_id, pys.parcel_id, pys.tax_year, p.account_number;

COMMENT ON VIEW v_parcel_tax_component_breakdown IS 'Admin-readable parcel-to-tax-unit component rate breakdown for Stage 6 debugging and tax calculations.';
COMMENT ON VIEW v_effective_tax_rate_rollup IS 'Derived parcel-year total effective tax rate plus structured component breakdown.';
COMMENT ON VIEW v_parcel_tax_assignment_qa IS 'QA coverage view that highlights missing or conflicting parcel-to-tax-unit assignments by parcel-year.';
