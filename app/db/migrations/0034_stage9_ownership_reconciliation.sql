ALTER TABLE parcel_year_snapshots
  ADD COLUMN IF NOT EXISTS cad_owner_name text,
  ADD COLUMN IF NOT EXISTS cad_owner_name_normalized text;

ALTER TABLE current_owner_rollups
  ADD COLUMN IF NOT EXISTS metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_deed_records_import_batch
  ON deed_records(import_batch_id, county_id, tax_year);

CREATE INDEX IF NOT EXISTS idx_deed_records_instrument_lookup
  ON deed_records(county_id, instrument_number, recording_date DESC);

CREATE INDEX IF NOT EXISTS idx_manual_overrides_ownership_active
  ON manual_overrides(county_id, override_scope, status, effective_from DESC);

CREATE OR REPLACE VIEW v_owner_reconciliation_evidence AS
WITH deed_support AS (
  SELECT
    dr.parcel_id,
    COALESCE(dr.tax_year, pys.tax_year) AS tax_year,
    COUNT(*) AS deed_record_count,
    COUNT(*) FILTER (
      WHERE COALESCE(dr.recording_date, dr.execution_date) > make_date(COALESCE(dr.tax_year, pys.tax_year), 12, 31)
    ) AS future_dated_deed_count,
    array_agg(DISTINCT dr.deed_record_id ORDER BY dr.deed_record_id) AS deed_record_ids,
    MAX(COALESCE(dr.recording_date, dr.execution_date)) AS latest_deed_date
  FROM deed_records dr
  LEFT JOIN parcel_year_snapshots pys
    ON pys.parcel_id = dr.parcel_id
  WHERE dr.parcel_id IS NOT NULL
  GROUP BY dr.parcel_id, COALESCE(dr.tax_year, pys.tax_year)
),
override_support AS (
  SELECT
    mo.county_id,
    mo.tax_year,
    mo.target_record_id AS parcel_id,
    mo.manual_override_id,
    mo.override_payload,
    mo.reason,
    row_number() OVER (
      PARTITION BY mo.county_id, mo.tax_year, mo.target_record_id
      ORDER BY mo.effective_from DESC, mo.created_at DESC
    ) AS override_rank
  FROM manual_overrides mo
  WHERE mo.override_scope = 'ownership'
    AND mo.status IN ('approved', 'applied')
    AND mo.target_table = 'parcels'
    AND mo.target_record_id IS NOT NULL
    AND COALESCE(mo.effective_to, now() + interval '100 years') >= now()
),
current_period_counts AS (
  SELECT
    pop.parcel_id,
    COUNT(*) FILTER (WHERE pop.is_current) AS current_period_count,
    COUNT(*) FILTER (
      WHERE pop.start_date IS NOT NULL
        AND pop.end_date IS NOT NULL
        AND pop.start_date > pop.end_date
    ) AS invalid_period_count
  FROM parcel_owner_periods pop
  GROUP BY pop.parcel_id
)
SELECT
  pys.county_id,
  pys.tax_year,
  pys.parcel_id,
  pys.account_number,
  pys.cad_owner_name,
  pys.cad_owner_name_normalized,
  cor.current_owner_rollup_id,
  cor.owner_name AS selected_owner_name,
  cor.owner_name_normalized AS selected_owner_name_normalized,
  cor.source_basis,
  cor.confidence_score,
  cor.override_flag,
  cor.owner_period_id,
  cor.metadata_json,
  COALESCE(ds.deed_record_count, 0) AS deed_record_count,
  COALESCE(ds.future_dated_deed_count, 0) AS future_dated_deed_count,
  ds.latest_deed_date,
  ds.deed_record_ids,
  COALESCE(cpc.current_period_count, 0) AS current_period_count,
  COALESCE(cpc.invalid_period_count, 0) AS invalid_owner_period_count,
  os.manual_override_id,
  os.reason AS manual_override_reason
FROM parcel_year_snapshots pys
LEFT JOIN current_owner_rollups cor
  ON cor.parcel_id = pys.parcel_id
 AND cor.tax_year = pys.tax_year
LEFT JOIN deed_support ds
  ON ds.parcel_id = pys.parcel_id
 AND ds.tax_year = pys.tax_year
LEFT JOIN current_period_counts cpc
  ON cpc.parcel_id = pys.parcel_id
LEFT JOIN override_support os
  ON os.county_id = pys.county_id
 AND os.tax_year = pys.tax_year
 AND os.parcel_id = pys.parcel_id
 AND os.override_rank = 1
WHERE pys.is_current = true;

CREATE OR REPLACE VIEW v_owner_reconciliation_qa AS
SELECT
  county_id,
  tax_year,
  parcel_id,
  account_number,
  cad_owner_name,
  selected_owner_name,
  source_basis,
  confidence_score,
  deed_record_count,
  future_dated_deed_count,
  current_period_count,
  invalid_owner_period_count,
  manual_override_id,
  current_owner_rollup_id IS NULL AS missing_current_owner_rollup,
  deed_record_count > 0 AND current_period_count = 0 AS missing_owner_periods_for_deeds,
  future_dated_deed_count > 0 AS future_dated_deed_flag,
  current_period_count > 1 AS conflicting_current_owner_periods,
  invalid_owner_period_count > 0 AS invalid_owner_period_flag,
  selected_owner_name IS NOT NULL
    AND cad_owner_name IS NOT NULL
    AND selected_owner_name IS DISTINCT FROM cad_owner_name
    AND COALESCE(override_flag, false) = false
    AS cad_owner_mismatch_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN current_owner_rollup_id IS NULL THEN 'missing_current_owner_rollup' END,
      CASE WHEN deed_record_count > 0 AND current_period_count = 0 THEN 'missing_owner_periods_for_deeds' END,
      CASE WHEN future_dated_deed_count > 0 THEN 'future_dated_deed' END,
      CASE WHEN current_period_count > 1 THEN 'conflicting_current_owner_periods' END,
      CASE WHEN invalid_owner_period_count > 0 THEN 'invalid_owner_period' END,
      CASE
        WHEN selected_owner_name IS NOT NULL
          AND cad_owner_name IS NOT NULL
          AND selected_owner_name IS DISTINCT FROM cad_owner_name
          AND COALESCE(override_flag, false) = false
        THEN 'cad_owner_mismatch'
      END
    ],
    NULL
  ) AS qa_issue_codes
FROM v_owner_reconciliation_evidence;

COMMENT ON VIEW v_owner_reconciliation_evidence IS 'Admin/debug ownership evidence view showing CAD owner snapshots, deed support, selected owner rollups, and applied ownership overrides.';
COMMENT ON VIEW v_owner_reconciliation_qa IS 'Parcel-year ownership reconciliation QA view highlighting missing rollups, conflicting owner periods, future-dated deeds, and CAD-versus-derived owner mismatches.';
