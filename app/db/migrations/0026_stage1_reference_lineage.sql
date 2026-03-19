DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'validation_scope_enum') THEN
    CREATE TYPE validation_scope_enum AS ENUM (
      'file_schema',
      'staging_row',
      'canonical_publish',
      'tax_assignment',
      'ownership_reconciliation',
      'search_index',
      'publish'
    );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'validation_severity_enum') THEN
    CREATE TYPE validation_severity_enum AS ENUM ('info', 'warning', 'error');
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'lineage_relation_type_enum') THEN
    CREATE TYPE lineage_relation_type_enum AS ENUM (
      'raw_to_staging',
      'staging_to_canonical',
      'canonical_to_derived',
      'publish',
      'manual_override'
    );
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION set_row_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

ALTER TABLE source_systems
  ADD COLUMN IF NOT EXISTS source_type text,
  ADD COLUMN IF NOT EXISTS county_id text REFERENCES counties(county_id),
  ADD COLUMN IF NOT EXISTS entity_coverage text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS official_url text,
  ADD COLUMN IF NOT EXISTS access_method text,
  ADD COLUMN IF NOT EXISTS file_format text,
  ADD COLUMN IF NOT EXISTS cadence text,
  ADD COLUMN IF NOT EXISTS reliability_tier text,
  ADD COLUMN IF NOT EXISTS auth_required boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS manual_only boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS legal_notes text,
  ADD COLUMN IF NOT EXISTS parser_module_name text,
  ADD COLUMN IF NOT EXISTS adapter_name text,
  ADD COLUMN IF NOT EXISTS active_flag boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE job_runs
  ADD COLUMN IF NOT EXISTS import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS raw_file_id uuid REFERENCES raw_files(raw_file_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS job_stage text,
  ADD COLUMN IF NOT EXISTS dry_run_flag boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS publish_version text,
  ADD COLUMN IF NOT EXISTS retry_of_job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS triggered_by text,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE import_batches
  ADD COLUMN IF NOT EXISTS source_url text,
  ADD COLUMN IF NOT EXISTS file_format text,
  ADD COLUMN IF NOT EXISTS publish_state text NOT NULL DEFAULT 'draft',
  ADD COLUMN IF NOT EXISTS publish_version text,
  ADD COLUMN IF NOT EXISTS dry_run_flag boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE raw_files
  ADD COLUMN IF NOT EXISTS tax_year integer REFERENCES tax_years(tax_year),
  ADD COLUMN IF NOT EXISTS file_kind text,
  ADD COLUMN IF NOT EXISTS source_url text,
  ADD COLUMN IF NOT EXISTS file_format text,
  ADD COLUMN IF NOT EXISTS detected_encoding text,
  ADD COLUMN IF NOT EXISTS archived_at timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_aliases
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_addresses
  ADD COLUMN IF NOT EXISTS address_type text NOT NULL DEFAULT 'situs',
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS source_record_hash text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_improvements
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS source_record_hash text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_lands
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS source_record_hash text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_assessments
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS source_record_hash text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_exemptions
  ADD COLUMN IF NOT EXISTS source_system_id uuid REFERENCES source_systems(source_system_id),
  ADD COLUMN IF NOT EXISTS source_record_hash text,
  ADD COLUMN IF NOT EXISTS granted_flag boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS validation_results (
  validation_result_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  raw_file_id uuid REFERENCES raw_files(raw_file_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  validation_scope validation_scope_enum NOT NULL,
  severity validation_severity_enum NOT NULL,
  entity_table text,
  entity_id uuid,
  validation_code text NOT NULL,
  message text NOT NULL,
  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lineage_records (
  lineage_record_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  raw_file_id uuid REFERENCES raw_files(raw_file_id) ON DELETE SET NULL,
  relation_type lineage_relation_type_enum NOT NULL,
  source_table text NOT NULL,
  source_id uuid,
  target_table text NOT NULL,
  target_id uuid,
  source_record_hash text,
  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS exemption_types (
  exemption_type_code text PRIMARY KEY,
  label text NOT NULL,
  description text,
  category text,
  display_order integer NOT NULL DEFAULT 100,
  is_homestead_related boolean NOT NULL DEFAULT false,
  is_senior_related boolean NOT NULL DEFAULT false,
  is_disabled_related boolean NOT NULL DEFAULT false,
  active_flag boolean NOT NULL DEFAULT true,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE constraint_name = 'parcel_exemptions_exemption_type_code_fkey'
      AND table_name = 'parcel_exemptions'
  ) THEN
    ALTER TABLE parcel_exemptions
      ADD CONSTRAINT parcel_exemptions_exemption_type_code_fkey
      FOREIGN KEY (exemption_type_code)
      REFERENCES exemption_types(exemption_type_code);
  END IF;
END
$$;

COMMENT ON TABLE validation_results IS 'Persisted QA and publish validation outcomes for file, staging, canonical, and derived checks.';
COMMENT ON TABLE lineage_records IS 'Lineage edges that trace normalized and derived records back to source jobs, batches, and raw files.';
COMMENT ON TABLE exemption_types IS 'Normalized Texas exemption catalog used by parcel-year exemption facts.';

DROP TRIGGER IF EXISTS set_counties_updated_at ON counties;
CREATE TRIGGER set_counties_updated_at
BEFORE UPDATE ON counties
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_county_configs_updated_at ON county_configs;
CREATE TRIGGER set_county_configs_updated_at
BEFORE UPDATE ON county_configs
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_source_systems_updated_at ON source_systems;
CREATE TRIGGER set_source_systems_updated_at
BEFORE UPDATE ON source_systems
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_job_runs_updated_at ON job_runs;
CREATE TRIGGER set_job_runs_updated_at
BEFORE UPDATE ON job_runs
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_import_batches_updated_at ON import_batches;
CREATE TRIGGER set_import_batches_updated_at
BEFORE UPDATE ON import_batches
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_raw_files_updated_at ON raw_files;
CREATE TRIGGER set_raw_files_updated_at
BEFORE UPDATE ON raw_files
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcels_updated_at ON parcels;
CREATE TRIGGER set_parcels_updated_at
BEFORE UPDATE ON parcels
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_aliases_updated_at ON parcel_aliases;
CREATE TRIGGER set_parcel_aliases_updated_at
BEFORE UPDATE ON parcel_aliases
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_addresses_updated_at ON parcel_addresses;
CREATE TRIGGER set_parcel_addresses_updated_at
BEFORE UPDATE ON parcel_addresses
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_improvements_updated_at ON parcel_improvements;
CREATE TRIGGER set_parcel_improvements_updated_at
BEFORE UPDATE ON parcel_improvements
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_lands_updated_at ON parcel_lands;
CREATE TRIGGER set_parcel_lands_updated_at
BEFORE UPDATE ON parcel_lands
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_assessments_updated_at ON parcel_assessments;
CREATE TRIGGER set_parcel_assessments_updated_at
BEFORE UPDATE ON parcel_assessments
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_exemptions_updated_at ON parcel_exemptions;
CREATE TRIGGER set_parcel_exemptions_updated_at
BEFORE UPDATE ON parcel_exemptions
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_exemption_types_updated_at ON exemption_types;
CREATE TRIGGER set_exemption_types_updated_at
BEFORE UPDATE ON exemption_types
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();
