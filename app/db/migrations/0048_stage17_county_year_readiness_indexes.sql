CREATE INDEX IF NOT EXISTS idx_parcel_year_snapshots_current_scope
  ON parcel_year_snapshots(county_id, tax_year)
  WHERE is_current = true;

CREATE INDEX IF NOT EXISTS idx_search_documents_scope
  ON search_documents(county_id, tax_year);
