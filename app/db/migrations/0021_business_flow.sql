CREATE TABLE IF NOT EXISTS leads (
  lead_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE SET NULL,
  county_id text REFERENCES counties(county_id),
  tax_year integer REFERENCES tax_years(tax_year),
  first_name text,
  last_name text,
  email citext,
  phone text,
  source_channel text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS lead_events (
  lead_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id uuid NOT NULL REFERENCES leads(lead_id) ON DELETE CASCADE,
  event_code text NOT NULL,
  event_payload jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS clients (
  client_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id uuid REFERENCES leads(lead_id) ON DELETE SET NULL,
  first_name text,
  last_name text,
  email citext,
  phone text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS representation_agreements (
  representation_agreement_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id uuid NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  agreement_status text NOT NULL,
  signed_at timestamptz,
  document_url text
);
CREATE TABLE IF NOT EXISTS protest_cases (
  protest_case_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id uuid REFERENCES clients(client_id) ON DELETE SET NULL,
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  tax_year integer NOT NULL REFERENCES tax_years(tax_year),
  valuation_run_id uuid REFERENCES valuation_runs(valuation_run_id) ON DELETE SET NULL,
  case_status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS case_outcomes (
  case_outcome_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid NOT NULL REFERENCES protest_cases(protest_case_id) ON DELETE CASCADE,
  outcome_code text NOT NULL,
  final_value numeric,
  reduction_amount numeric,
  savings_amount numeric,
  outcome_date date
);
