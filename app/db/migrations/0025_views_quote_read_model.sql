CREATE OR REPLACE VIEW v_quote_read_model AS
SELECT
    p.county_id,
    vr.tax_year,
    p.account_number,
    p.parcel_id,
    COALESCE(
      pa.situs_address || ', ' || pa.situs_city || ', ' || COALESCE(pa.situs_state, 'TX') || ' ' || pa.situs_zip,
      p.situs_address || ', ' || p.situs_city || ', ' || COALESCE(p.situs_state, 'TX') || ' ' || p.situs_zip
    ) AS address,
    ass.notice_value AS current_notice_value,
    vr.market_value_point,
    vr.equity_value_point,
    vr.defensible_value_point,
    pse.gross_tax_savings_point,
    pse.expected_tax_savings_point,
    pse.expected_tax_savings_low,
    pse.expected_tax_savings_high,
    pse.estimated_contingency_fee,
    qe.confidence_label AS confidence,
    qe.basis,
    pr.recommendation_code AS protest_recommendation,
    qe.explanation_json,
    qe.explanation_bullets,
    vr.created_at AS valuation_created_at
FROM valuation_runs vr
JOIN parcels p ON p.parcel_id = vr.parcel_id
LEFT JOIN parcel_addresses pa ON pa.parcel_id = p.parcel_id AND pa.is_current = true
LEFT JOIN parcel_assessments ass ON ass.parcel_id = vr.parcel_id AND ass.tax_year = vr.tax_year
LEFT JOIN parcel_savings_estimates pse ON pse.valuation_run_id = vr.valuation_run_id
LEFT JOIN quote_explanations qe ON qe.valuation_run_id = vr.valuation_run_id
LEFT JOIN protest_recommendations pr ON pr.valuation_run_id = vr.valuation_run_id;

CREATE OR REPLACE VIEW v_search_read_model AS
SELECT
    p.county_id,
    p.account_number,
    p.parcel_id,
    COALESCE(pa.situs_address, p.situs_address) AS situs_address,
    COALESCE(pa.situs_zip, p.situs_zip) AS situs_zip,
    COALESCE(pa.normalized_address, upper(regexp_replace(COALESCE(pa.situs_address, p.situs_address, ''), '[^A-Za-z0-9 ]', '', 'g'))) AS normalized_address,
    p.owner_name
FROM parcels p
LEFT JOIN parcel_addresses pa ON pa.parcel_id = p.parcel_id AND pa.is_current = true;
