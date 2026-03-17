CREATE TABLE IF NOT EXISTS workflow_statuses (
  workflow_status_code text PRIMARY KEY,
  label text NOT NULL,
  status_group text
);
