CREATE TABLE IF NOT EXISTS client_parcels (
  client_parcel_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id uuid NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  relationship_type text NOT NULL DEFAULT 'subject_property',
  is_primary boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (client_id, parcel_id)
);

CREATE TABLE IF NOT EXISTS case_assignments (
  case_assignment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid NOT NULL REFERENCES protest_cases(protest_case_id) ON DELETE CASCADE,
  assignee_name text NOT NULL,
  assignee_role text NOT NULL,
  assignment_status text NOT NULL DEFAULT 'active',
  assigned_at timestamptz NOT NULL DEFAULT now(),
  due_at timestamptz,
  active_flag boolean NOT NULL DEFAULT true,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hearings (
  hearing_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid NOT NULL REFERENCES protest_cases(protest_case_id) ON DELETE CASCADE,
  hearing_type_code text NOT NULL REFERENCES hearing_types(hearing_type_code),
  hearing_status text NOT NULL DEFAULT 'pending',
  scheduled_at timestamptz,
  location_text text,
  hearing_reference text,
  result_summary text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS case_status_history (
  case_status_history_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid NOT NULL REFERENCES protest_cases(protest_case_id) ON DELETE CASCADE,
  workflow_status_code text REFERENCES workflow_statuses(workflow_status_code),
  case_status text NOT NULL,
  reason_text text,
  changed_by text,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE case_notes
  ADD COLUMN IF NOT EXISTS note_code text NOT NULL DEFAULT 'general',
  ADD COLUMN IF NOT EXISTS author_label text,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS evidence_packet_items (
  evidence_packet_item_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  evidence_packet_id uuid NOT NULL REFERENCES evidence_packets(evidence_packet_id) ON DELETE CASCADE,
  item_type text NOT NULL DEFAULT 'section',
  section_code text NOT NULL,
  title text NOT NULL,
  body_text text,
  source_basis text,
  display_order integer NOT NULL DEFAULT 100,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evidence_comp_sets (
  evidence_comp_set_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  evidence_packet_id uuid NOT NULL REFERENCES evidence_packets(evidence_packet_id) ON DELETE CASCADE,
  basis_type text NOT NULL,
  set_label text NOT NULL,
  notes text,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evidence_comp_set_items (
  evidence_comp_set_item_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  evidence_comp_set_id uuid NOT NULL REFERENCES evidence_comp_sets(evidence_comp_set_id) ON DELETE CASCADE,
  parcel_sale_id uuid REFERENCES parcel_sales(parcel_sale_id) ON DELETE SET NULL,
  parcel_id uuid REFERENCES parcels(parcel_id) ON DELETE SET NULL,
  comp_role text NOT NULL DEFAULT 'supporting',
  comp_rank integer,
  rationale_text text,
  adjustment_summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO workflow_statuses (workflow_status_code, label, status_group)
VALUES
  ('intake_review', 'Intake Review', 'case'),
  ('packet_review', 'Packet Review', 'case'),
  ('ready_to_file', 'Ready to File', 'case'),
  ('hearing_pending', 'Hearing Pending', 'case'),
  ('decision_received', 'Decision Received', 'case')
ON CONFLICT (workflow_status_code) DO UPDATE
SET
  label = EXCLUDED.label,
  status_group = EXCLUDED.status_group;

CREATE INDEX IF NOT EXISTS idx_client_parcels_client
  ON client_parcels(client_id, is_primary);

CREATE INDEX IF NOT EXISTS idx_case_assignments_case
  ON case_assignments(protest_case_id, active_flag, assigned_at DESC);

CREATE INDEX IF NOT EXISTS idx_hearings_case
  ON hearings(protest_case_id, scheduled_at DESC);

CREATE INDEX IF NOT EXISTS idx_case_status_history_case
  ON case_status_history(protest_case_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_evidence_packet_items_packet
  ON evidence_packet_items(evidence_packet_id, display_order, section_code);

CREATE INDEX IF NOT EXISTS idx_evidence_comp_sets_packet
  ON evidence_comp_sets(evidence_packet_id, basis_type);

CREATE INDEX IF NOT EXISTS idx_evidence_comp_set_items_set
  ON evidence_comp_set_items(evidence_comp_set_id, comp_rank);

DROP TRIGGER IF EXISTS set_client_parcels_updated_at ON client_parcels;
CREATE TRIGGER set_client_parcels_updated_at
BEFORE UPDATE ON client_parcels
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_case_assignments_updated_at ON case_assignments;
CREATE TRIGGER set_case_assignments_updated_at
BEFORE UPDATE ON case_assignments
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_hearings_updated_at ON hearings;
CREATE TRIGGER set_hearings_updated_at
BEFORE UPDATE ON hearings
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_case_notes_updated_at ON case_notes;
CREATE TRIGGER set_case_notes_updated_at
BEFORE UPDATE ON case_notes
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_evidence_packet_items_updated_at ON evidence_packet_items;
CREATE TRIGGER set_evidence_packet_items_updated_at
BEFORE UPDATE ON evidence_packet_items
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_evidence_comp_sets_updated_at ON evidence_comp_sets;
CREATE TRIGGER set_evidence_comp_sets_updated_at
BEFORE UPDATE ON evidence_comp_sets
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();

DROP TRIGGER IF EXISTS set_evidence_comp_set_items_updated_at ON evidence_comp_set_items;
CREATE TRIGGER set_evidence_comp_set_items_updated_at
BEFORE UPDATE ON evidence_comp_set_items
FOR EACH ROW
EXECUTE FUNCTION set_row_updated_at();
