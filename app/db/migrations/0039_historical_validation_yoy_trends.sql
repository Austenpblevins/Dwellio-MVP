CREATE OR REPLACE VIEW parcel_year_trend_view AS
SELECT
  current_summary.county_id,
  current_summary.parcel_id,
  current_summary.account_number,
  current_summary.tax_year,
  prior_summary.tax_year AS prior_tax_year,
  current_summary.market_value,
  prior_summary.market_value AS prior_market_value,
  current_summary.appraised_value,
  prior_summary.appraised_value AS prior_appraised_value,
  current_summary.assessed_value,
  prior_summary.assessed_value AS prior_assessed_value,
  current_summary.notice_value,
  prior_summary.notice_value AS prior_notice_value,
  current_summary.exemption_value_total,
  prior_summary.exemption_value_total AS prior_exemption_value_total,
  current_summary.effective_tax_rate,
  prior_summary.effective_tax_rate AS prior_effective_tax_rate,
  current_summary.estimated_annual_tax,
  prior_summary.estimated_annual_tax AS prior_estimated_annual_tax,
  current_summary.homestead_flag,
  prior_summary.homestead_flag AS prior_homestead_flag,
  current_summary.freeze_flag,
  prior_summary.freeze_flag AS prior_freeze_flag,
  (prior_summary.parcel_id IS NOT NULL) AS has_prior_year,
  CASE
    WHEN prior_summary.appraised_value IS NULL OR current_summary.appraised_value IS NULL
      THEN NULL
    ELSE current_summary.appraised_value - prior_summary.appraised_value
  END AS appraised_value_change,
  CASE
    WHEN prior_summary.appraised_value IS NULL
      OR prior_summary.appraised_value = 0
      OR current_summary.appraised_value IS NULL
      THEN NULL
    ELSE (current_summary.appraised_value - prior_summary.appraised_value) / prior_summary.appraised_value
  END AS appraised_value_change_pct,
  CASE
    WHEN prior_summary.assessed_value IS NULL OR current_summary.assessed_value IS NULL
      THEN NULL
    ELSE current_summary.assessed_value - prior_summary.assessed_value
  END AS assessed_value_change,
  CASE
    WHEN prior_summary.assessed_value IS NULL
      OR prior_summary.assessed_value = 0
      OR current_summary.assessed_value IS NULL
      THEN NULL
    ELSE (current_summary.assessed_value - prior_summary.assessed_value) / prior_summary.assessed_value
  END AS assessed_value_change_pct,
  CASE
    WHEN prior_summary.notice_value IS NULL OR current_summary.notice_value IS NULL
      THEN NULL
    ELSE current_summary.notice_value - prior_summary.notice_value
  END AS notice_value_change,
  CASE
    WHEN prior_summary.notice_value IS NULL
      OR prior_summary.notice_value = 0
      OR current_summary.notice_value IS NULL
      THEN NULL
    ELSE (current_summary.notice_value - prior_summary.notice_value) / prior_summary.notice_value
  END AS notice_value_change_pct,
  CASE
    WHEN prior_summary.effective_tax_rate IS NULL OR current_summary.effective_tax_rate IS NULL
      THEN NULL
    ELSE current_summary.effective_tax_rate - prior_summary.effective_tax_rate
  END AS effective_tax_rate_change,
  CASE
    WHEN prior_summary.effective_tax_rate IS NULL
      OR prior_summary.effective_tax_rate = 0
      OR current_summary.effective_tax_rate IS NULL
      THEN NULL
    ELSE (current_summary.effective_tax_rate - prior_summary.effective_tax_rate) / prior_summary.effective_tax_rate
  END AS effective_tax_rate_change_pct,
  CASE
    WHEN prior_summary.exemption_value_total IS NULL OR current_summary.exemption_value_total IS NULL
      THEN NULL
    ELSE current_summary.exemption_value_total - prior_summary.exemption_value_total
  END AS exemption_value_change,
  CASE
    WHEN prior_summary.estimated_annual_tax IS NULL OR current_summary.estimated_annual_tax IS NULL
      THEN NULL
    ELSE current_summary.estimated_annual_tax - prior_summary.estimated_annual_tax
  END AS estimated_annual_tax_change,
  CASE
    WHEN prior_summary.estimated_annual_tax IS NULL
      OR prior_summary.estimated_annual_tax = 0
      OR current_summary.estimated_annual_tax IS NULL
      THEN NULL
    ELSE (current_summary.estimated_annual_tax - prior_summary.estimated_annual_tax) / prior_summary.estimated_annual_tax
  END AS estimated_annual_tax_change_pct,
  (
    COALESCE(current_summary.exemption_value_total, 0::numeric)
      IS DISTINCT FROM
    COALESCE(prior_summary.exemption_value_total, 0::numeric)
  ) AS exemption_changed_flag,
  current_summary.homestead_flag IS DISTINCT FROM prior_summary.homestead_flag AS homestead_changed_flag,
  current_summary.freeze_flag IS DISTINCT FROM prior_summary.freeze_flag AS freeze_changed_flag
FROM parcel_summary_view current_summary
LEFT JOIN parcel_summary_view prior_summary
  ON prior_summary.parcel_id = current_summary.parcel_id
 AND prior_summary.tax_year = current_summary.tax_year - 1;

CREATE OR REPLACE VIEW neighborhood_year_trend_view AS
SELECT
  current_stats.county_id,
  current_stats.tax_year,
  prior_stats.tax_year AS prior_tax_year,
  current_stats.neighborhood_code,
  current_stats.property_type_code,
  current_stats.period_months,
  current_stats.sale_count,
  prior_stats.sale_count AS prior_sale_count,
  current_stats.median_sale_psf,
  prior_stats.median_sale_psf AS prior_median_sale_psf,
  current_stats.p25_sale_psf,
  prior_stats.p25_sale_psf AS prior_p25_sale_psf,
  current_stats.p75_sale_psf,
  prior_stats.p75_sale_psf AS prior_p75_sale_psf,
  current_stats.price_std_dev,
  prior_stats.price_std_dev AS prior_price_std_dev,
  current_stats.median_sale_price,
  prior_stats.median_sale_price AS prior_median_sale_price,
  CASE
    WHEN prior_stats.median_sale_psf IS NULL OR current_stats.median_sale_psf IS NULL
      THEN NULL
    ELSE current_stats.median_sale_psf - prior_stats.median_sale_psf
  END AS median_sale_psf_change,
  CASE
    WHEN prior_stats.median_sale_psf IS NULL
      OR prior_stats.median_sale_psf = 0
      OR current_stats.median_sale_psf IS NULL
      THEN NULL
    ELSE (current_stats.median_sale_psf - prior_stats.median_sale_psf) / prior_stats.median_sale_psf
  END AS median_sale_psf_change_pct,
  CASE
    WHEN prior_stats.median_sale_price IS NULL OR current_stats.median_sale_price IS NULL
      THEN NULL
    ELSE current_stats.median_sale_price - prior_stats.median_sale_price
  END AS median_sale_price_change,
  CASE
    WHEN prior_stats.median_sale_price IS NULL
      OR prior_stats.median_sale_price = 0
      OR current_stats.median_sale_price IS NULL
      THEN NULL
    ELSE (current_stats.median_sale_price - prior_stats.median_sale_price) / prior_stats.median_sale_price
  END AS median_sale_price_change_pct,
  CASE
    WHEN prior_stats.price_std_dev IS NULL OR current_stats.price_std_dev IS NULL
      THEN NULL
    ELSE current_stats.price_std_dev - prior_stats.price_std_dev
  END AS price_std_dev_change,
  (prior_stats.neighborhood_stat_id IS NOT NULL) AS has_prior_year,
  (
    current_stats.sale_count < 5
    OR COALESCE(prior_stats.sale_count, 0) < 5
  ) AS weak_sample_support_flag
FROM neighborhood_stats current_stats
LEFT JOIN neighborhood_stats prior_stats
  ON prior_stats.county_id = current_stats.county_id
 AND prior_stats.tax_year = current_stats.tax_year - 1
 AND prior_stats.neighborhood_code = current_stats.neighborhood_code
 AND prior_stats.property_type_code = current_stats.property_type_code
 AND prior_stats.period_months = current_stats.period_months;

COMMENT ON VIEW parcel_year_trend_view IS 'Derived parcel-year trend view built from parcel_summary_view and prior-year parcel summary rows. Supports reproducible YOY feature engineering without changing canonical parcel history.';
COMMENT ON VIEW neighborhood_year_trend_view IS 'Derived neighborhood/year trend view built from neighborhood_stats and prior-year neighborhood_stats rows for historical validation and market trend support.';
