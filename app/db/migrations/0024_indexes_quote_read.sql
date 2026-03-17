CREATE INDEX IF NOT EXISTS idx_parcel_sales_parcel_date ON parcel_sales(parcel_id, sale_date DESC);
CREATE INDEX IF NOT EXISTS idx_neighborhood_stats_lookup ON neighborhood_stats(county_id, tax_year, neighborhood_code, property_type_code, period_months);
CREATE INDEX IF NOT EXISTS idx_comp_candidates_subject_type_rank ON comp_candidates(subject_parcel_id, comp_type, rank_num);
CREATE INDEX IF NOT EXISTS idx_quote_explanations_parcel_year ON quote_explanations(parcel_id, tax_year);
CREATE INDEX IF NOT EXISTS idx_protest_recommendations_parcel_year ON protest_recommendations(parcel_id, tax_year);
CREATE INDEX IF NOT EXISTS idx_protest_cases_parcel_year ON protest_cases(parcel_id, tax_year);
