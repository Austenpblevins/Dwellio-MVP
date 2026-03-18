DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'deed_party_role_enum') THEN
    CREATE TYPE deed_party_role_enum AS ENUM ('grantor', 'grantee', 'other');
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'override_status_enum') THEN
    CREATE TYPE override_status_enum AS ENUM ('pending', 'approved', 'rejected', 'applied', 'expired');
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS deed_records (
  deed_record_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text NOT NULL REFERENCES counties(county_id),
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE SET NULL,
  tax_year integer REFERENCES tax_years(tax_year),
  source_system_id uuid REFERENCES source_systems(source_system_id),
  import_batch_id uuid REFERENCES import_batches(import_batch_id) ON DELETE SET NULL,
  job_run_id uuid REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
  instrument_number text,
  volume_page text,
  recording_date date,
  execution_date date,
  consideration_amount numeric,
  document_type text,
  transfer_type text,
  grantor_summary text,
  grantee_summary text,
  source_record_hash text,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (county_id, instrument_number)
);

CREATE TABLE IF NOT EXISTS deed_parties (
  deed_party_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deed_record_id uuid NOT NULL REFERENCES deed_records(deed_record_id) ON DELETE CASCADE,
  party_role deed_party_role_enum NOT NULL,
  party_name text NOT NULL,
  normalized_name text,
  party_order integer,
  mailing_address text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS parcel_owner_periods (
  parcel_owner_period_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  owner_name text NOT NULL,
  owner_name_normalized text,
  start_date date,
  end_date date,
  source_basis text NOT NULL,
  deed_record_id uuid REFERENCES deed_records(deed_record_id) ON DELETE SET NULL,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  confidence_score numeric(5,4),
  is_current boolean NOT NULL DEFAULT false,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS current_owner_rollups (
  current_owner_rollup_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  owner_name text NOT NULL,
  owner_name_normalized text,
  owner_names_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  mailing_address text,
  mailing_city text,
  mailing_state text DEFAULT 'TX',
  mailing_zip text,
  source_basis text NOT NULL,
  source_record_hash text,
  source_system_id uuid REFERENCES source_systems(source_system_id),
  owner_period_id uuid REFERENCES parcel_owner_periods(parcel_owner_period_id) ON DELETE SET NULL,
  confidence_score numeric(5,4),
  override_flag boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_id, tax_year)
);

CREATE TABLE IF NOT EXISTS search_documents (
  search_document_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  county_id text NOT NULL REFERENCES counties(county_id),
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  account_number text NOT NULL,
  normalized_address text NOT NULL,
  normalized_owner_name text,
  display_address text,
  search_text text NOT NULL,
  document_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (parcel_id, tax_year)
);

CREATE TABLE IF NOT EXISTS evidence_packets (
  evidence_packet_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid REFERENCES protest_cases(protest_case_id) ON DELETE SET NULL,
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  packet_type text NOT NULL DEFAULT 'informal',
  packet_status text NOT NULL DEFAULT 'draft',
  valuation_run_id uuid REFERENCES valuation_runs(valuation_run_id) ON DELETE SET NULL,
  storage_path text,
  packet_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  generated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS manual_overrides (
  manual_override_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  county_id text REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  target_table text NOT NULL,
  target_record_id uuid,
  override_scope text NOT NULL,
  override_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  reason text NOT NULL,
  status override_status_enum NOT NULL DEFAULT 'pending',
  source_basis text,
  applied_by text,
  approved_by text,
  effective_from timestamptz NOT NULL DEFAULT now(),
  effective_to timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS manual_override_events (
  manual_override_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  manual_override_id uuid NOT NULL REFERENCES manual_overrides(manual_override_id) ON DELETE CASCADE,
  event_type text NOT NULL,
  event_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE valuation_runs
  ADD COLUMN IF NOT EXISTS defensible_value_rule text NOT NULL DEFAULT 'min(market,equity)',
  ADD COLUMN IF NOT EXISTS model_inputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE parcel_savings_estimates
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE quote_explanations
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE protest_recommendations
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE decision_tree_results
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE protest_cases
  ADD COLUMN IF NOT EXISTS appraisal_district_id uuid REFERENCES appraisal_districts(appraisal_district_id),
  ADD COLUMN IF NOT EXISTS representation_agreement_id uuid REFERENCES representation_agreements(representation_agreement_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS workflow_status_code text REFERENCES workflow_statuses(workflow_status_code),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE case_outcomes
  ADD COLUMN IF NOT EXISTS hearing_type_code text REFERENCES hearing_types(hearing_type_code),
  ADD COLUMN IF NOT EXISTS outcome_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'valuation_runs_defensible_value_point_chk'
  ) THEN
    ALTER TABLE valuation_runs
      ADD CONSTRAINT valuation_runs_defensible_value_point_chk
      CHECK (
        defensible_value_point IS NULL
        OR (
          (market_value_point IS NULL OR defensible_value_point <= market_value_point)
          AND (equity_value_point IS NULL OR defensible_value_point <= equity_value_point)
        )
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'valuation_runs_defensible_value_low_chk'
  ) THEN
    ALTER TABLE valuation_runs
      ADD CONSTRAINT valuation_runs_defensible_value_low_chk
      CHECK (
        defensible_value_low IS NULL
        OR (
          (market_value_low IS NULL OR defensible_value_low <= market_value_low)
          AND (equity_value_low IS NULL OR defensible_value_low <= equity_value_low)
        )
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'valuation_runs_defensible_value_high_chk'
  ) THEN
    ALTER TABLE valuation_runs
      ADD CONSTRAINT valuation_runs_defensible_value_high_chk
      CHECK (
        defensible_value_high IS NULL
        OR (
          (market_value_high IS NULL OR defensible_value_high <= market_value_high)
          AND (equity_value_high IS NULL OR defensible_value_high <= equity_value_high)
        )
      );
  END IF;
END
$$;

COMMENT ON TABLE current_owner_rollups IS 'Current parcel-year owner summary derived from deed evidence, CAD data, and future override hooks.';
COMMENT ON TABLE search_documents IS 'Search-support storage; public search remains the v_search_read_model contract.';
COMMENT ON TABLE manual_overrides IS 'Generic manual override registry for tax assignment, ownership, valuation, and other bounded corrective actions.';

DROP TRIGGER IF EXISTS set_deed_records_updated_at ON deed_records;
CREATE TRIGGER set_deed_records_updated_at
BEFORE UPDATE ON deed_records
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_deed_parties_updated_at ON deed_parties;
CREATE TRIGGER set_deed_parties_updated_at
BEFORE UPDATE ON deed_parties
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_owner_periods_updated_at ON parcel_owner_periods;
CREATE TRIGGER set_parcel_owner_periods_updated_at
BEFORE UPDATE ON parcel_owner_periods
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_current_owner_rollups_updated_at ON current_owner_rollups;
CREATE TRIGGER set_current_owner_rollups_updated_at
BEFORE UPDATE ON current_owner_rollups
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_search_documents_updated_at ON search_documents;
CREATE TRIGGER set_search_documents_updated_at
BEFORE UPDATE ON search_documents
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_evidence_packets_updated_at ON evidence_packets;
CREATE TRIGGER set_evidence_packets_updated_at
BEFORE UPDATE ON evidence_packets
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_manual_overrides_updated_at ON manual_overrides;
CREATE TRIGGER set_manual_overrides_updated_at
BEFORE UPDATE ON manual_overrides
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_valuation_runs_updated_at ON valuation_runs;
CREATE TRIGGER set_valuation_runs_updated_at
BEFORE UPDATE ON valuation_runs
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_parcel_savings_estimates_updated_at ON parcel_savings_estimates;
CREATE TRIGGER set_parcel_savings_estimates_updated_at
BEFORE UPDATE ON parcel_savings_estimates
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_quote_explanations_updated_at ON quote_explanations;
CREATE TRIGGER set_quote_explanations_updated_at
BEFORE UPDATE ON quote_explanations
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_protest_recommendations_updated_at ON protest_recommendations;
CREATE TRIGGER set_protest_recommendations_updated_at
BEFORE UPDATE ON protest_recommendations
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_decision_tree_results_updated_at ON decision_tree_results;
CREATE TRIGGER set_decision_tree_results_updated_at
BEFORE UPDATE ON decision_tree_results
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_protest_cases_updated_at ON protest_cases;
CREATE TRIGGER set_protest_cases_updated_at
BEFORE UPDATE ON protest_cases
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_case_outcomes_updated_at ON case_outcomes;
CREATE TRIGGER set_case_outcomes_updated_at
BEFORE UPDATE ON case_outcomes
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();
