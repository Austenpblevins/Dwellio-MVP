CREATE TABLE IF NOT EXISTS case_notes (
  case_note_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid NOT NULL REFERENCES protest_cases(protest_case_id) ON DELETE CASCADE,
  note_text text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
