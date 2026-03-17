CREATE TABLE IF NOT EXISTS source_systems (
  source_system_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_system_code text NOT NULL UNIQUE,
  source_system_name text NOT NULL,
  restricted_flag boolean NOT NULL DEFAULT false
);
