INSERT INTO tax_years (
  tax_year,
  starts_on,
  ends_on,
  valuation_date,
  protest_deadline_default,
  certified_roll_date,
  is_active
)
VALUES
  (2022, DATE '2022-01-01', DATE '2022-12-31', DATE '2022-01-01', DATE '2022-05-15', NULL, true),
  (2023, DATE '2023-01-01', DATE '2023-12-31', DATE '2023-01-01', DATE '2023-05-15', NULL, true),
  (2024, DATE '2024-01-01', DATE '2024-12-31', DATE '2024-01-01', DATE '2024-05-15', NULL, true),
  (2025, DATE '2025-01-01', DATE '2025-12-31', DATE '2025-01-01', DATE '2025-05-15', NULL, true)
ON CONFLICT (tax_year) DO UPDATE
SET
  starts_on = EXCLUDED.starts_on,
  ends_on = EXCLUDED.ends_on,
  valuation_date = EXCLUDED.valuation_date,
  protest_deadline_default = EXCLUDED.protest_deadline_default,
  is_active = EXCLUDED.is_active;
