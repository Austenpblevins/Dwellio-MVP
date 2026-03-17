CREATE TABLE IF NOT EXISTS invoices (
  invoice_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  protest_case_id uuid REFERENCES protest_cases(protest_case_id) ON DELETE SET NULL,
  client_id uuid REFERENCES clients(client_id) ON DELETE SET NULL,
  invoice_status text NOT NULL
);
