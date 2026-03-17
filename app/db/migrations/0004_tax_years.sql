CREATE TABLE IF NOT EXISTS tax_years (
  tax_year integer PRIMARY KEY,
  starts_on date,
  ends_on date,
  protest_deadline_default date,
  is_active boolean NOT NULL DEFAULT true
);
