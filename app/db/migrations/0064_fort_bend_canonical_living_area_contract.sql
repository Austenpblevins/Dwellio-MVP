ALTER TABLE improvements
  ADD COLUMN IF NOT EXISTS gross_component_area_sf numeric,
  ADD COLUMN IF NOT EXISTS living_area_source text;

ALTER TABLE parcel_improvements
  ADD COLUMN IF NOT EXISTS gross_component_area_sf numeric,
  ADD COLUMN IF NOT EXISTS living_area_source text;

COMMENT ON COLUMN improvements.gross_component_area_sf IS
  'County-specific gross structural/component area kept separately from canonical living_area_sf.';
COMMENT ON COLUMN improvements.living_area_source IS
  'Authoritative source used to populate canonical living_area_sf (for example property_summary_export).';
COMMENT ON COLUMN parcel_improvements.gross_component_area_sf IS
  'County-specific gross structural/component area kept separately from canonical living_area_sf.';
COMMENT ON COLUMN parcel_improvements.living_area_source IS
  'Authoritative source used to populate canonical living_area_sf (for example property_summary_export).';
