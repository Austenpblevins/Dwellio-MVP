CREATE INDEX IF NOT EXISTS idx_appraisal_districts_county
  ON appraisal_districts(county_id);

CREATE INDEX IF NOT EXISTS idx_source_systems_county_code
  ON source_systems(county_id, source_system_code);

CREATE INDEX IF NOT EXISTS idx_job_runs_lookup
  ON job_runs(county_id, tax_year, job_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_import_batches_lookup
  ON import_batches(county_id, tax_year, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_files_batch
  ON raw_files(import_batch_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingest_errors_batch_stage
  ON ingest_errors(import_batch_id, error_stage, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_validation_results_scope_severity
  ON validation_results(validation_scope, severity, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_validation_results_entity
  ON validation_results(entity_table, entity_id);

CREATE INDEX IF NOT EXISTS idx_lineage_records_target
  ON lineage_records(target_table, target_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_lineage_records_source
  ON lineage_records(source_table, source_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_parcels_county_year_account
  ON parcels(county_id, tax_year, account_number);

CREATE INDEX IF NOT EXISTS idx_parcels_address_lookup
  ON parcels(county_id, situs_zip, situs_address);

CREATE INDEX IF NOT EXISTS idx_parcels_geom
  ON parcels
  USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_parcel_aliases_type_value
  ON parcel_aliases(alias_type, alias_value);

CREATE INDEX IF NOT EXISTS idx_parcel_addresses_current
  ON parcel_addresses(parcel_id, is_current);

CREATE INDEX IF NOT EXISTS idx_parcel_addresses_normalized_trgm
  ON parcel_addresses
  USING gin (normalized_address gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_parcel_improvements_parcel_year
  ON parcel_improvements(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_parcel_lands_parcel_year
  ON parcel_lands(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_parcel_assessments_parcel_year
  ON parcel_assessments(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_parcel_exemptions_parcel_year
  ON parcel_exemptions(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_parcel_year_snapshots_lookup
  ON parcel_year_snapshots(parcel_id, tax_year, is_current);

CREATE INDEX IF NOT EXISTS idx_improvements_snapshot
  ON improvements(parcel_year_snapshot_id);

CREATE INDEX IF NOT EXISTS idx_land_segments_snapshot
  ON land_segments(parcel_year_snapshot_id);

CREATE INDEX IF NOT EXISTS idx_value_components_snapshot
  ON value_components(parcel_year_snapshot_id, component_category);

CREATE INDEX IF NOT EXISTS idx_taxing_units_lookup
  ON taxing_units(county_id, tax_year, unit_type_code, unit_code);

CREATE INDEX IF NOT EXISTS idx_tax_rates_lookup
  ON tax_rates(taxing_unit_id, tax_year, rate_component);

CREATE INDEX IF NOT EXISTS idx_parcel_taxing_units_lookup
  ON parcel_taxing_units(parcel_id, tax_year, taxing_unit_id);

CREATE INDEX IF NOT EXISTS idx_parcel_geometries_geom
  ON parcel_geometries
  USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_taxing_unit_boundaries_geom
  ON taxing_unit_boundaries
  USING gist (geom);

CREATE INDEX IF NOT EXISTS idx_deed_records_parcel_date
  ON deed_records(parcel_id, recording_date DESC);

CREATE INDEX IF NOT EXISTS idx_deed_parties_name_trgm
  ON deed_parties
  USING gin (normalized_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_owner_periods_current
  ON parcel_owner_periods(parcel_id, is_current, start_date DESC);

CREATE INDEX IF NOT EXISTS idx_current_owner_rollups_lookup
  ON current_owner_rollups(parcel_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_current_owner_rollups_owner_trgm
  ON current_owner_rollups
  USING gin (owner_name_normalized gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_search_documents_account
  ON search_documents(county_id, tax_year, account_number);

CREATE INDEX IF NOT EXISTS idx_search_documents_address_trgm
  ON search_documents
  USING gin (normalized_address gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_search_documents_search_text_trgm
  ON search_documents
  USING gin (search_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_evidence_packets_case
  ON evidence_packets(protest_case_id, tax_year, packet_type);

CREATE INDEX IF NOT EXISTS idx_manual_overrides_target
  ON manual_overrides(target_table, target_record_id, status);

INSERT INTO tax_years (tax_year, starts_on, ends_on, protest_deadline_default, is_active)
VALUES (2026, DATE '2026-01-01', DATE '2026-12-31', DATE '2026-05-15', true)
ON CONFLICT (tax_year) DO UPDATE
SET
  starts_on = EXCLUDED.starts_on,
  ends_on = EXCLUDED.ends_on,
  protest_deadline_default = EXCLUDED.protest_deadline_default,
  is_active = EXCLUDED.is_active;

INSERT INTO appraisal_districts (county_id, district_name, district_code, website_url, is_active)
VALUES
  ('harris', 'Harris Central Appraisal District', 'HCAD', 'https://hcad.org', true),
  ('fort_bend', 'Fort Bend Central Appraisal District', 'FBCAD', 'https://www.fbcad.org', true)
ON CONFLICT (county_id, district_code) DO UPDATE
SET
  district_name = EXCLUDED.district_name,
  website_url = EXCLUDED.website_url,
  is_active = EXCLUDED.is_active;

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
    'MANUAL_UPLOAD',
    'Manual Upload',
    false,
    'manual',
    NULL,
    ARRAY['parcel', 'tax_rate', 'gis', 'deed'],
    NULL,
    'manual_upload',
    'mixed',
    'ad_hoc',
    'high',
    false,
    true,
    NULL,
    NULL,
    true,
    '{"notes": "Fallback for county files and one-off recovery loads."}'::jsonb
  ),
  (
    'HCAD_BULK',
    'HCAD Bulk Export',
    false,
    'county_bulk',
    'harris',
    ARRAY['parcel', 'assessment', 'exemption'],
    'https://hcad.org',
    'bulk_download',
    'csv',
    'annual',
    'high',
    false,
    false,
    'app.county_adapters.harris.parse',
    'HarrisCountyAdapter',
    true,
    '{}'::jsonb
  ),
  (
    'FBCAD_EXPORT',
    'FBCAD Export',
    false,
    'county_bulk',
    'fort_bend',
    ARRAY['parcel', 'assessment', 'exemption'],
    'https://www.fbcad.org',
    'bulk_download',
    'csv',
    'annual',
    'high',
    false,
    false,
    'app.county_adapters.fort_bend.parse',
    'FortBendCountyAdapter',
    true,
    '{}'::jsonb
  ),
  (
    'DEED_FEED',
    'Deed Feed',
    false,
    'county_recording',
    NULL,
    ARRAY['deed', 'ownership'],
    NULL,
    'manual_upload',
    'csv',
    'ad_hoc',
    'medium',
    false,
    true,
    NULL,
    NULL,
    true,
    '{}'::jsonb
  ),
  (
    'GIS_BOUNDARY',
    'GIS Boundary Feed',
    false,
    'gis',
    NULL,
    ARRAY['parcel_geometry', 'taxing_unit_boundary'],
    NULL,
    'manual_upload',
    'geojson',
    'ad_hoc',
    'medium',
    false,
    true,
    NULL,
    NULL,
    true,
    '{}'::jsonb
  )
ON CONFLICT (source_system_code) DO UPDATE
SET
  source_system_name = EXCLUDED.source_system_name,
  restricted_flag = EXCLUDED.restricted_flag,
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

INSERT INTO workflow_statuses (workflow_status_code, label, status_group)
VALUES
  ('new', 'New', 'lead'),
  ('quoted', 'Quoted', 'lead'),
  ('contacted', 'Contacted', 'lead'),
  ('signed', 'Signed', 'client'),
  ('active', 'Active', 'case'),
  ('closed', 'Closed', 'case')
ON CONFLICT (workflow_status_code) DO UPDATE
SET
  label = EXCLUDED.label,
  status_group = EXCLUDED.status_group;

INSERT INTO hearing_types (hearing_type_code, label)
VALUES
  ('informal', 'Informal'),
  ('arb', 'ARB')
ON CONFLICT (hearing_type_code) DO UPDATE
SET
  label = EXCLUDED.label;

INSERT INTO property_type_codes (property_type_code, label, is_residential)
VALUES ('sfr', 'Single Family Residential', true)
ON CONFLICT (property_type_code) DO UPDATE
SET
  label = EXCLUDED.label,
  is_residential = EXCLUDED.is_residential;

INSERT INTO exemption_types (
  exemption_type_code,
  label,
  description,
  category,
  display_order,
  is_homestead_related,
  is_senior_related,
  is_disabled_related,
  active_flag
)
VALUES
  ('homestead', 'Homestead', 'General residence homestead exemption.', 'residential', 10, true, false, false, true),
  ('over65', 'Over 65', 'Senior homeowner exemption.', 'residential', 20, true, true, false, true),
  ('disabled_person', 'Disabled Person', 'Disability-related residential exemption.', 'residential', 30, true, false, true, true),
  ('disabled_veteran', 'Disabled Veteran', 'Disabled veteran exemption.', 'veteran', 40, false, false, true, true),
  ('surviving_spouse', 'Surviving Spouse', 'Surviving spouse related exemption.', 'residential', 50, false, true, false, true),
  ('ag', 'Agricultural', 'Agricultural or special valuation exemption.', 'special', 60, false, false, false, true),
  ('freeze_ceiling', 'Freeze Ceiling', 'Tax ceiling or freeze-related status.', 'residential', 70, true, true, true, true)
ON CONFLICT (exemption_type_code) DO UPDATE
SET
  label = EXCLUDED.label,
  description = EXCLUDED.description,
  category = EXCLUDED.category,
  display_order = EXCLUDED.display_order,
  is_homestead_related = EXCLUDED.is_homestead_related,
  is_senior_related = EXCLUDED.is_senior_related,
  is_disabled_related = EXCLUDED.is_disabled_related,
  active_flag = EXCLUDED.active_flag;

INSERT INTO taxing_unit_types (unit_type_code, label, description, display_order)
VALUES
  ('county', 'County', 'County-level taxing unit.', 10),
  ('city', 'City', 'City or municipality taxing unit.', 20),
  ('school_district', 'School District', 'Independent school district taxing unit.', 30),
  ('mud', 'MUD', 'Municipal utility district or similar special taxing district.', 40),
  ('special_district', 'Special District', 'Other special purpose taxing district.', 50)
ON CONFLICT (unit_type_code) DO UPDATE
SET
  label = EXCLUDED.label,
  description = EXCLUDED.description,
  display_order = EXCLUDED.display_order;

INSERT INTO county_configs (
  county_id,
  tax_year,
  appraisal_district_id,
  parser_module_name,
  protest_deadline_rule,
  homestead_cap_percent,
  data_refresh_frequency,
  quote_enabled,
  public_quote_enabled,
  restricted_sales_enabled,
  config_json
)
SELECT
  ad.county_id,
  2026,
  ad.appraisal_district_id,
  CASE ad.county_id
    WHEN 'harris' THEN 'app.county_adapters.harris'
    WHEN 'fort_bend' THEN 'app.county_adapters.fort_bend'
    ELSE NULL
  END,
  'state_default',
  0.10,
  'annual',
  true,
  true,
  false,
  '{}'::jsonb
FROM appraisal_districts ad
WHERE ad.county_id IN ('harris', 'fort_bend')
ON CONFLICT (county_id, tax_year) DO UPDATE
SET
  appraisal_district_id = EXCLUDED.appraisal_district_id,
  parser_module_name = EXCLUDED.parser_module_name,
  protest_deadline_rule = EXCLUDED.protest_deadline_rule,
  homestead_cap_percent = EXCLUDED.homestead_cap_percent,
  data_refresh_frequency = EXCLUDED.data_refresh_frequency,
  quote_enabled = EXCLUDED.quote_enabled,
  public_quote_enabled = EXCLUDED.public_quote_enabled,
  restricted_sales_enabled = EXCLUDED.restricted_sales_enabled,
  config_json = EXCLUDED.config_json;
