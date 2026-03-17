CREATE TABLE IF NOT EXISTS counties (
  county_id text PRIMARY KEY,
  county_name text NOT NULL,
  timezone text DEFAULT 'America/Chicago',
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO counties (county_id, county_name)
VALUES ('harris', 'Harris County'), ('fort_bend', 'Fort Bend County')
ON CONFLICT (county_id) DO NOTHING;
