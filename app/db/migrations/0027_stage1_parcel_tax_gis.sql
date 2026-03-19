DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'assignment_method_enum') THEN
    CREATE TYPE assignment_method_enum AS ENUM (
      'source_direct',
      'source_inferred',
      'gis',
      'manual'
    );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'geometry_role_enum') THEN
    CREATE TYPE geometry_role_enum AS ENUM (
      'parcel_polygon',
      'parcel_centroid',
      'taxing_unit_boundary'
    );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS parcel_year_snapshots (
  parcel_year_snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  appraisal_district_id uuid REFERENCES appraisal_districts(appraisal_district_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  snapshot_status text NOT NULL DEFAULT 'published',
  is_current boolean NOT NULL DEFAULT true,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  source_record_hash text,
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_id, tax_year)
);

CREATE TABLE IF NOT EXISTS property_characteristics (
  property_characteristic_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_year_snapshot_id uuid NOT NULL REFERENCES parcel_year_snapshots(parcel_year_snapshot_id) ON DELETE CASCADE,
  property_type_code text REFERENCES property_type_codes(property_type_code),
  property_class_code text,
  neighborhood_code text,
  subdivision_name text,
  school_district_name text,
  homestead_flag boolean,
  owner_occupied_flag boolean,
  primary_use_code text,
  neighborhood_group text,
  effective_age numeric,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_year_snapshot_id)
);

CREATE TABLE IF NOT EXISTS improvements (
  improvement_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_year_snapshot_id uuid NOT NULL REFERENCES parcel_year_snapshots(parcel_year_snapshot_id) ON DELETE CASCADE,
  improvement_type text NOT NULL DEFAULT 'primary_structure',
  building_label text,
  living_area_sf numeric,
  year_built integer,
  effective_year_built integer,
  effective_age numeric,
  bedrooms integer,
  full_baths numeric,
  half_baths numeric,
  stories numeric,
  quality_code text,
  condition_code text,
  garage_spaces numeric,
  pool_flag boolean,
  fireplace_count integer,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS land_segments (
  land_segment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_year_snapshot_id uuid NOT NULL REFERENCES parcel_year_snapshots(parcel_year_snapshot_id) ON DELETE CASCADE,
  segment_num integer NOT NULL DEFAULT 1,
  land_type_code text,
  land_sf numeric,
  land_acres numeric,
  frontage_sf numeric,
  depth_sf numeric,
  market_value numeric,
  ag_use_value numeric,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_year_snapshot_id, segment_num)
);

CREATE TABLE IF NOT EXISTS value_components (
  value_component_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_year_snapshot_id uuid NOT NULL REFERENCES parcel_year_snapshots(parcel_year_snapshot_id) ON DELETE CASCADE,
  component_code text NOT NULL,
  component_label text,
  component_category text,
  market_value numeric,
  assessed_value numeric,
  taxable_value numeric,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_year_snapshot_id, component_code)
);

CREATE TABLE IF NOT EXISTS taxing_unit_types (
  unit_type_code text PRIMARY KEY,
  label text NOT NULL,
  description text,
  display_order integer NOT NULL DEFAULT 100,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS taxing_units (
  taxing_unit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  appraisal_district_id uuid REFERENCES appraisal_districts(appraisal_district_id),
  unit_type_code text NOT NULL REFERENCES taxing_unit_types(unit_type_code),
  unit_code text NOT NULL,
  unit_name text NOT NULL,
  parent_taxing_unit_id uuid REFERENCES taxing_units(taxing_unit_id) ON DELETE SET NULL,
  state_geoid text,
  active_flag boolean NOT NULL DEFAULT true,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  source_record_hash text,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (county_id, tax_year, unit_code)
);

CREATE TABLE IF NOT EXISTS tax_rates (
  tax_rate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  taxing_unit_id uuid NOT NULL REFERENCES taxing_units(taxing_unit_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  rate_component text NOT NULL DEFAULT 'ad_valorem',
  rate_value numeric(12,8) NOT NULL,
  rate_per_100 numeric(12,6),
  effective_from date,
  effective_to date,
  is_current boolean NOT NULL DEFAULT true,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  source_record_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (taxing_unit_id, tax_year, rate_component)
);

CREATE TABLE IF NOT EXISTS parcel_taxing_units (
  parcel_taxing_unit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  taxing_unit_id uuid NOT NULL REFERENCES taxing_units(taxing_unit_id) ON DELETE CASCADE,
  assignment_method assignment_method_enum NOT NULL DEFAULT 'source_direct',
  assignment_confidence numeric(5,4),
  is_primary boolean NOT NULL DEFAULT false,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  source_record_hash text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_id, tax_year, taxing_unit_id)
);

CREATE TABLE IF NOT EXISTS parcel_geometries (
  parcel_geometry_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  geometry_role geometry_role_enum NOT NULL,
  geom geometry(Geometry, 4326) NOT NULL,
  centroid geometry(Point, 4326),
  area_sqft numeric,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  source_record_hash text,
  is_current boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_id, tax_year, geometry_role)
);

CREATE TABLE IF NOT EXISTS taxing_unit_boundaries (
  taxing_unit_boundary_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  taxing_unit_id uuid NOT NULL REFERENCES taxing_units(taxing_unit_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  boundary_scope text NOT NULL DEFAULT 'service_area',
  boundary_name text,
  geom geometry(MultiPolygon, 4326) NOT NULL,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  source_record_hash text,
  is_current boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (taxing_unit_id, tax_year, boundary_scope)
);

ALTER TABLE effective_tax_rates
  ADD COLUMN IF NOT EXISTS calculation_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

COMMENT ON TABLE parcel_year_snapshots IS 'Annual parcel anchor that keeps parcel identity stable while preserving parcel-year state and provenance.';
COMMENT ON TABLE value_components IS 'Granular parcel-year value lines that complement aggregate parcel_assessments totals.';
COMMENT ON TABLE taxing_units IS 'Tax authority master keyed by county and tax year so Harris and Fort Bend can evolve independently.';
COMMENT ON TABLE parcel_geometries IS 'Historical or alternate parcel geometry storage separate from the convenience geom on parcels.';
COMMENT ON TABLE taxing_unit_boundaries IS 'Spatial boundaries for school districts, MUDs, and other taxing units used for assignment QA and GIS joins.';

DROP TRIGGER IF EXISTS set_parcel_year_snapshots_updated_at ON parcel_year_snapshots;
CREATE TRIGGER set_parcel_year_snapshots_updated_at
BEFORE UPDATE ON parcel_year_snapshots
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_property_characteristics_updated_at ON property_characteristics;
CREATE TRIGGER set_property_characteristics_updated_at
BEFORE UPDATE ON property_characteristics
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_improvements_updated_at ON improvements;
CREATE TRIGGER set_improvements_updated_at
BEFORE UPDATE ON improvements
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_land_segments_updated_at ON land_segments;
CREATE TRIGGER set_land_segments_updated_at
BEFORE UPDATE ON land_segments
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_value_components_updated_at ON value_components;
CREATE TRIGGER set_value_components_updated_at
BEFORE UPDATE ON value_components
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_taxing_unit_types_updated_at ON taxing_unit_types;
CREATE TRIGGER set_taxing_unit_types_updated_at
BEFORE UPDATE ON taxing_unit_types
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_taxing_units_updated_at ON taxing_units;
CREATE TRIGGER set_taxing_units_updated_at
BEFORE UPDATE ON taxing_units
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_tax_rates_updated_at ON tax_rates;
CREATE TRIGGER set_tax_rates_updated_at
BEFORE UPDATE ON tax_rates
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_taxing_units_updated_at ON parcel_taxing_units;
CREATE TRIGGER set_parcel_taxing_units_updated_at
BEFORE UPDATE ON parcel_taxing_units
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_geometries_updated_at ON parcel_geometries;
CREATE TRIGGER set_parcel_geometries_updated_at
BEFORE UPDATE ON parcel_geometries
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_taxing_unit_boundaries_updated_at ON taxing_unit_boundaries;
CREATE TRIGGER set_taxing_unit_boundaries_updated_at
BEFORE UPDATE ON taxing_unit_boundaries
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_effective_tax_rates_updated_at ON effective_tax_rates;
CREATE TRIGGER set_effective_tax_rates_updated_at
BEFORE UPDATE ON effective_tax_rates
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();
