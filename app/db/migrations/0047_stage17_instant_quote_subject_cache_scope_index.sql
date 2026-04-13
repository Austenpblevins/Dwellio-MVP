CREATE INDEX IF NOT EXISTS idx_instant_quote_subject_cache_scope
  ON instant_quote_subject_cache(county_id, tax_year, parcel_id);
