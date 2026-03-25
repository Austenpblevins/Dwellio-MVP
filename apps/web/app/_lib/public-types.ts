export type ParcelSearchResult = {
  county_id: string;
  tax_year: number | null;
  account_number: string;
  parcel_id: string;
  address: string;
  situs_zip: string | null;
  owner_name: string | null;
  match_basis: string;
  match_score: number;
  confidence_label: string;
};

export type SearchResponse = {
  results: ParcelSearchResult[];
};

export type ParcelOwnerSummary = {
  display_name: string | null;
  owner_type: "individual" | "entity" | "unknown";
  privacy_mode: "masked_individual_name" | "public_entity_name" | "hidden";
  confidence_label: "high" | "medium" | "limited";
};

export type ParcelValueSummary = {
  market_value: number | null;
  assessed_value: number | null;
  appraised_value: number | null;
  certified_value: number | null;
  notice_value: number | null;
};

export type ParcelExemptionSummary = {
  exemption_value_total: number | null;
  homestead_flag: boolean | null;
  over65_flag: boolean | null;
  disabled_flag: boolean | null;
  disabled_veteran_flag: boolean | null;
  freeze_flag: boolean | null;
  exemption_type_codes: string[];
  raw_exemption_codes: string[];
};

export type ParcelTaxRateComponent = {
  unit_type_code: string | null;
  unit_code: string | null;
  unit_name: string | null;
  rate_component: string | null;
  rate_value: number | null;
  rate_per_100: number | null;
  assignment_method: string | null;
  assignment_confidence: number | null;
  assignment_reason_code: string | null;
  is_primary: boolean | null;
};

export type ParcelTaxSummary = {
  effective_tax_rate: number | null;
  estimated_taxable_value: number | null;
  estimated_annual_tax: number | null;
  component_breakdown: ParcelTaxRateComponent[];
};

export type ParcelDataCaveat = {
  code: string;
  severity: "info" | "warning" | "critical";
  title: string;
  message: string;
};

export type ParcelSummaryResponse = {
  county_id: string;
  tax_year: number;
  account_number: string;
  parcel_id: string;
  address: string;
  owner_name: string | null;
  property_type_code: string | null;
  property_class_code: string | null;
  neighborhood_code: string | null;
  subdivision_name: string | null;
  school_district_name: string | null;
  living_area_sf: number | null;
  year_built: number | null;
  effective_age: number | null;
  bedrooms: number | null;
  full_baths: number | null;
  half_baths: number | null;
  land_sf: number | null;
  land_acres: number | null;
  market_value: number | null;
  assessed_value: number | null;
  appraised_value: number | null;
  certified_value: number | null;
  notice_value: number | null;
  exemption_value_total: number | null;
  homestead_flag: boolean | null;
  over65_flag: boolean | null;
  disabled_flag: boolean | null;
  disabled_veteran_flag: boolean | null;
  freeze_flag: boolean | null;
  effective_tax_rate: number | null;
  estimated_taxable_value: number | null;
  estimated_annual_tax: number | null;
  exemption_type_codes: string[];
  raw_exemption_codes: string[];
  completeness_score: number;
  warning_codes: string[];
  public_summary_ready_flag: boolean;
  owner_summary: ParcelOwnerSummary | null;
  value_summary: ParcelValueSummary | null;
  exemption_summary: ParcelExemptionSummary | null;
  tax_summary: ParcelTaxSummary | null;
  caveats: ParcelDataCaveat[];
};

export type QuoteResponse = {
  county_id: string;
  tax_year: number;
  account_number: string;
  parcel_id: string;
  address: string;
  current_notice_value: number | null;
  market_value_point: number | null;
  equity_value_point: number | null;
  defensible_value_point: number | null;
  gross_tax_savings_point: number | null;
  expected_tax_savings_point: number | null;
  expected_tax_savings_low: number | null;
  expected_tax_savings_high: number | null;
  estimated_contingency_fee: number | null;
  confidence: string | null;
  basis: string | null;
  protest_recommendation: string | null;
  explanation_json: Record<string, unknown>;
  explanation_bullets: string[];
};
