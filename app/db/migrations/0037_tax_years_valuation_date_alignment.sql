ALTER TABLE tax_years
  ADD COLUMN IF NOT EXISTS valuation_date date,
  ADD COLUMN IF NOT EXISTS certified_roll_date date;

UPDATE tax_years
SET valuation_date = make_date(tax_year, 1, 1)
WHERE valuation_date IS NULL;

COMMENT ON COLUMN tax_years.valuation_date IS 'Canonical annual valuation date used by downstream valuation and QA workflows. Backfilled conservatively to January 1 of the tax year for existing rows.';
COMMENT ON COLUMN tax_years.certified_roll_date IS 'Certified roll date for the tax year when known. Left nullable because historical backfill semantics are county-specific and not safely derivable from existing starts_on/ends_on fields.';
