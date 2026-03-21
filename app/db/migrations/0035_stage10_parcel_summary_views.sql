CREATE OR REPLACE VIEW parcel_owner_current_view AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number,
    pys.cad_owner_name,
    pys.cad_owner_name_normalized
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
)
SELECT
  cs.county_id,
  cs.parcel_id,
  cs.tax_year,
  cs.account_number,
  cor.current_owner_rollup_id,
  cor.owner_name AS current_owner_name,
  cor.owner_name_normalized AS current_owner_name_normalized,
  cor.owner_names_json,
  cor.mailing_address,
  cor.mailing_city,
  COALESCE(cor.mailing_state, 'TX') AS mailing_state,
  cor.mailing_zip,
  cor.source_basis,
  cor.source_record_hash,
  cor.source_system_id,
  cor.owner_period_id,
  cor.confidence_score,
  cor.override_flag,
  cs.cad_owner_name,
  cs.cad_owner_name_normalized,
  (cor.current_owner_rollup_id IS NULL) AS missing_owner_rollup_flag,
  (
    cor.owner_name IS NOT NULL
    AND cs.cad_owner_name IS NOT NULL
    AND cor.owner_name IS DISTINCT FROM cs.cad_owner_name
    AND COALESCE(cor.override_flag, false) = false
  ) AS cad_owner_mismatch_flag,
  COALESCE(cor.metadata_json, '{}'::jsonb) AS metadata_json
FROM current_snapshots cs
LEFT JOIN current_owner_rollups cor
  ON cor.parcel_id = cs.parcel_id
 AND cor.tax_year = cs.tax_year;

CREATE OR REPLACE VIEW parcel_effective_tax_rate_view AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
),
component_rows AS (
  SELECT
    ptu.parcel_id,
    ptu.tax_year,
    tu.unit_type_code,
    tu.unit_code,
    tu.unit_name,
    tr.rate_component,
    COALESCE(tr.rate_value, tr.rate_per_100 / 100.0) AS rate_value,
    tr.rate_per_100,
    ptu.assignment_method,
    ptu.assignment_confidence,
    ptu.is_primary,
    ptu.assignment_reason_code,
    ptu.match_basis_json
  FROM parcel_taxing_units ptu
  JOIN taxing_units tu
    ON tu.taxing_unit_id = ptu.taxing_unit_id
  JOIN tax_rates tr
    ON tr.taxing_unit_id = ptu.taxing_unit_id
   AND tr.tax_year = ptu.tax_year
   AND tr.is_current = true
),
component_rollup AS (
  SELECT
    cr.parcel_id,
    cr.tax_year,
    COUNT(*) AS component_count,
    SUM(cr.rate_value) AS component_effective_tax_rate,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'county') AS county_assignment_count,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'city') AS city_assignment_count,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'school') AS school_assignment_count,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'mud') AS mud_assignment_count,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'special') AS special_assignment_count,
    BOOL_OR(cr.assignment_method = 'manual') AS has_manual_assignment,
    BOOL_OR(cr.assignment_method = 'gis') AS has_gis_assignment,
    jsonb_agg(
      jsonb_build_object(
        'unit_type_code', cr.unit_type_code,
        'unit_code', cr.unit_code,
        'unit_name', cr.unit_name,
        'rate_component', cr.rate_component,
        'rate_value', cr.rate_value,
        'rate_per_100', cr.rate_per_100,
        'assignment_method', cr.assignment_method,
        'assignment_confidence', cr.assignment_confidence,
        'assignment_reason_code', cr.assignment_reason_code,
        'is_primary', cr.is_primary,
        'match_basis_json', cr.match_basis_json
      )
      ORDER BY cr.unit_type_code, cr.unit_name, cr.rate_component
    ) AS component_breakdown_json
  FROM component_rows cr
  GROUP BY cr.parcel_id, cr.tax_year
)
SELECT
  cs.county_id,
  cs.parcel_id,
  cs.tax_year,
  cs.account_number,
  etr.effective_tax_rate AS stored_effective_tax_rate,
  cr.component_effective_tax_rate,
  COALESCE(etr.effective_tax_rate, cr.component_effective_tax_rate) AS effective_tax_rate,
  COALESCE(etr.source_method, 'component_rollup') AS source_method,
  COALESCE(etr.calculation_basis_json, '{}'::jsonb) AS calculation_basis_json,
  COALESCE(cr.component_count, 0) AS component_count,
  COALESCE(cr.component_breakdown_json, '[]'::jsonb) AS component_breakdown_json,
  COALESCE(cr.county_assignment_count, 0) AS county_assignment_count,
  COALESCE(cr.city_assignment_count, 0) AS city_assignment_count,
  COALESCE(cr.school_assignment_count, 0) AS school_assignment_count,
  COALESCE(cr.mud_assignment_count, 0) AS mud_assignment_count,
  COALESCE(cr.special_assignment_count, 0) AS special_assignment_count,
  COALESCE(cr.has_manual_assignment, false) AS has_manual_assignment,
  COALESCE(cr.has_gis_assignment, false) AS has_gis_assignment,
  COALESCE(cr.county_assignment_count, 0) = 0 AS missing_county_assignment,
  COALESCE(cr.school_assignment_count, 0) = 0 AS missing_school_assignment,
  COALESCE(cr.county_assignment_count, 0) > 1 AS conflicting_county_assignment,
  COALESCE(cr.school_assignment_count, 0) > 1 AS conflicting_school_assignment,
  COALESCE(etr.effective_tax_rate, cr.component_effective_tax_rate) IS NULL AS missing_effective_tax_rate_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN COALESCE(cr.county_assignment_count, 0) = 0 THEN 'missing_county_assignment' END,
      CASE WHEN COALESCE(cr.school_assignment_count, 0) = 0 THEN 'missing_school_assignment' END,
      CASE WHEN COALESCE(cr.county_assignment_count, 0) > 1 THEN 'conflicting_county_assignment' END,
      CASE WHEN COALESCE(cr.school_assignment_count, 0) > 1 THEN 'conflicting_school_assignment' END,
      CASE
        WHEN COALESCE(etr.effective_tax_rate, cr.component_effective_tax_rate) IS NULL
        THEN 'missing_effective_tax_rate'
      END
    ],
    NULL
  ) AS warning_codes
FROM current_snapshots cs
LEFT JOIN effective_tax_rates etr
  ON etr.parcel_id = cs.parcel_id
 AND etr.tax_year = cs.tax_year
LEFT JOIN component_rollup cr
  ON cr.parcel_id = cs.parcel_id
 AND cr.tax_year = cs.tax_year;

CREATE OR REPLACE VIEW parcel_tax_estimate_summary AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
),
raw_codes AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    array_agg(DISTINCT raw_code ORDER BY raw_code) AS raw_exemption_codes
  FROM parcel_exemptions pe
  CROSS JOIN LATERAL unnest(
    CASE
      WHEN pe.raw_exemption_codes IS NULL OR cardinality(pe.raw_exemption_codes) = 0
        THEN ARRAY[COALESCE(pe.exemption_type_code, '')]
      ELSE pe.raw_exemption_codes
    END
  ) AS raw_code
  WHERE btrim(raw_code) <> ''
  GROUP BY pe.parcel_id, pe.tax_year
),
exemption_rollup AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    COUNT(*) AS exemption_record_count,
    COUNT(*) FILTER (WHERE pe.granted_flag) AS granted_exemption_count,
    COALESCE(
      SUM(pe.exemption_amount) FILTER (WHERE pe.granted_flag AND pe.exemption_amount IS NOT NULL),
      0::numeric
    ) AS granted_exemption_amount_total,
    COALESCE(
      array_agg(DISTINCT pe.exemption_type_code ORDER BY pe.exemption_type_code)
      FILTER (WHERE pe.exemption_type_code IS NOT NULL),
      ARRAY[]::text[]
    ) AS exemption_type_codes,
    COALESCE(rc.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'homestead'),
      false
    ) AS homestead_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'over65'),
      false
    ) AS over65_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled'),
      false
    ) AS disabled_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled_veteran'),
      false
    ) AS disabled_veteran_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'freeze'),
      false
    ) AS freeze_flag,
    COALESCE(
      BOOL_OR(pe.amount_missing_flag OR (pe.granted_flag AND pe.exemption_amount IS NULL)),
      false
    ) AS missing_exemption_amount_flag,
    pa.exemption_value_total AS assessment_exemption_value_total,
    pa.homestead_flag AS assessment_homestead_flag
  FROM parcel_exemptions pe
  LEFT JOIN exemption_types et
    ON et.exemption_type_code = pe.exemption_type_code
  LEFT JOIN raw_codes rc
    ON rc.parcel_id = pe.parcel_id
   AND rc.tax_year = pe.tax_year
  LEFT JOIN parcel_assessments pa
    ON pa.parcel_id = pe.parcel_id
   AND pa.tax_year = pe.tax_year
  GROUP BY pe.parcel_id, pe.tax_year, rc.raw_exemption_codes, pa.exemption_value_total, pa.homestead_flag
),
tax_rate_rollup AS (
  SELECT
    ptu.parcel_id,
    ptu.tax_year,
    SUM(COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)) AS component_effective_tax_rate
  FROM parcel_taxing_units ptu
  JOIN tax_rates tr
    ON tr.taxing_unit_id = ptu.taxing_unit_id
   AND tr.tax_year = ptu.tax_year
   AND tr.is_current = true
  GROUP BY ptu.parcel_id, ptu.tax_year
),
tax_basis AS (
  SELECT
    cs.county_id,
    cs.parcel_id,
    cs.tax_year,
    cs.account_number,
    pa.market_value,
    pa.assessed_value,
    pa.capped_value,
    pa.appraised_value,
    pa.certified_value,
    pa.notice_value,
    pa.exemption_value_total,
    pa.homestead_flag AS assessment_homestead_flag,
    COALESCE(etr.effective_tax_rate, trr.component_effective_tax_rate) AS effective_tax_rate,
    COALESCE(etr.source_method, 'component_rollup') AS source_method,
    COALESCE(pa.certified_value, pa.appraised_value, pa.assessed_value, pa.market_value, pa.notice_value) AS assessment_basis_value
  FROM current_snapshots cs
  LEFT JOIN parcel_assessments pa
    ON pa.parcel_id = cs.parcel_id
   AND pa.tax_year = cs.tax_year
  LEFT JOIN effective_tax_rates etr
    ON etr.parcel_id = cs.parcel_id
   AND etr.tax_year = cs.tax_year
  LEFT JOIN tax_rate_rollup trr
    ON trr.parcel_id = cs.parcel_id
   AND trr.tax_year = cs.tax_year
)
SELECT
  tb.county_id,
  tb.parcel_id,
  tb.tax_year,
  tb.account_number,
  tb.market_value,
  tb.assessed_value,
  tb.capped_value,
  tb.appraised_value,
  tb.certified_value,
  tb.notice_value,
  tb.assessment_basis_value,
  tb.exemption_value_total,
  tb.assessment_homestead_flag,
  COALESCE(er.exemption_record_count, 0) AS exemption_record_count,
  COALESCE(er.granted_exemption_count, 0) AS granted_exemption_count,
  COALESCE(er.granted_exemption_amount_total, 0::numeric) AS granted_exemption_amount_total,
  COALESCE(er.exemption_type_codes, ARRAY[]::text[]) AS exemption_type_codes,
  COALESCE(er.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
  COALESCE(er.homestead_flag, false) AS homestead_flag,
  COALESCE(er.over65_flag, false) AS over65_flag,
  COALESCE(er.disabled_flag, false) AS disabled_flag,
  COALESCE(er.disabled_veteran_flag, false) AS disabled_veteran_flag,
  COALESCE(er.freeze_flag, false) AS freeze_flag,
  tb.effective_tax_rate,
  tb.source_method AS effective_tax_rate_source_method,
  er.assessment_exemption_value_total,
  CASE
    WHEN tb.assessment_basis_value IS NULL THEN NULL
    ELSE GREATEST(tb.assessment_basis_value - COALESCE(tb.exemption_value_total, 0), 0)
  END AS estimated_taxable_value,
  CASE
    WHEN tb.notice_value IS NULL THEN NULL
    ELSE GREATEST(tb.notice_value - COALESCE(tb.exemption_value_total, 0), 0)
  END AS estimated_notice_taxable_value,
  CASE
    WHEN tb.assessment_basis_value IS NULL OR tb.effective_tax_rate IS NULL THEN NULL
    ELSE GREATEST(tb.assessment_basis_value - COALESCE(tb.exemption_value_total, 0), 0) * tb.effective_tax_rate
  END AS estimated_annual_tax,
  CASE
    WHEN tb.notice_value IS NULL OR tb.effective_tax_rate IS NULL THEN NULL
    ELSE GREATEST(tb.notice_value - COALESCE(tb.exemption_value_total, 0), 0) * tb.effective_tax_rate
  END AS estimated_notice_tax,
  tb.assessment_basis_value IS NULL AS missing_assessment_flag,
  tb.effective_tax_rate IS NULL AS missing_effective_tax_rate_flag,
  COALESCE(er.missing_exemption_amount_flag, false) AS missing_exemption_amount_flag,
  (
    er.assessment_exemption_value_total IS NOT NULL
    AND ABS(COALESCE(er.granted_exemption_amount_total, 0) - er.assessment_exemption_value_total) > 0.01
  ) AS assessment_exemption_total_mismatch_flag,
  (
    COALESCE(er.assessment_homestead_flag, tb.assessment_homestead_flag) IS NOT NULL
    AND COALESCE(er.assessment_homestead_flag, tb.assessment_homestead_flag)
      IS DISTINCT FROM COALESCE(er.homestead_flag, false)
  ) AS homestead_flag_mismatch_flag,
  (
    COALESCE(er.freeze_flag, false)
    AND NOT (
      COALESCE(er.over65_flag, false)
      OR COALESCE(er.disabled_flag, false)
      OR COALESCE(er.disabled_veteran_flag, false)
    )
  ) AS freeze_without_qualifying_exemption_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN tb.assessment_basis_value IS NULL THEN 'missing_assessment' END,
      CASE WHEN tb.effective_tax_rate IS NULL THEN 'missing_effective_tax_rate' END,
      CASE WHEN COALESCE(er.missing_exemption_amount_flag, false) THEN 'missing_exemption_amount' END,
      CASE
        WHEN er.assessment_exemption_value_total IS NOT NULL
          AND ABS(COALESCE(er.granted_exemption_amount_total, 0) - er.assessment_exemption_value_total) > 0.01
        THEN 'assessment_exemption_total_mismatch'
      END,
      CASE
        WHEN COALESCE(er.assessment_homestead_flag, tb.assessment_homestead_flag) IS NOT NULL
          AND COALESCE(er.assessment_homestead_flag, tb.assessment_homestead_flag)
            IS DISTINCT FROM COALESCE(er.homestead_flag, false)
        THEN 'homestead_flag_mismatch'
      END,
      CASE
        WHEN COALESCE(er.freeze_flag, false)
          AND NOT (
            COALESCE(er.over65_flag, false)
            OR COALESCE(er.disabled_flag, false)
            OR COALESCE(er.disabled_veteran_flag, false)
          )
        THEN 'freeze_without_qualifying_exemption'
      END
    ],
    NULL
  ) AS warning_codes
FROM tax_basis tb
LEFT JOIN exemption_rollup er
  ON er.parcel_id = tb.parcel_id
 AND er.tax_year = tb.tax_year;

CREATE OR REPLACE VIEW parcel_search_view AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_year_snapshot_id,
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number,
    pys.cad_owner_name,
    pys.cad_owner_name_normalized
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
),
current_addresses AS (
  SELECT DISTINCT ON (pa.parcel_id)
    pa.parcel_id,
    pa.situs_address,
    pa.situs_city,
    COALESCE(pa.situs_state, 'TX') AS situs_state,
    pa.situs_zip,
    pa.normalized_address
  FROM parcel_addresses pa
  WHERE pa.is_current = true
  ORDER BY pa.parcel_id, pa.updated_at DESC, pa.created_at DESC, pa.parcel_address_id DESC
)
SELECT
  cs.county_id,
  cs.tax_year,
  p.parcel_id,
  cs.account_number,
  p.cad_property_id,
  COALESCE(ca.situs_address, p.situs_address) AS situs_address,
  COALESCE(ca.situs_city, p.situs_city) AS situs_city,
  COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')) AS situs_state,
  COALESCE(ca.situs_zip, p.situs_zip) AS situs_zip,
  concat_ws(
    ', ',
    COALESCE(ca.situs_address, p.situs_address),
    COALESCE(ca.situs_city, p.situs_city),
    concat_ws(' ', COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')), COALESCE(ca.situs_zip, p.situs_zip))
  ) AS address,
  COALESCE(
    ca.normalized_address,
    upper(
      regexp_replace(
        concat_ws(' ', COALESCE(ca.situs_address, p.situs_address), COALESCE(ca.situs_city, p.situs_city), COALESCE(ca.situs_zip, p.situs_zip)),
        '[^A-Za-z0-9 ]',
        '',
        'g'
      )
    )
  ) AS normalized_address,
  COALESCE(cor.owner_name, cs.cad_owner_name, p.owner_name) AS owner_name,
  COALESCE(
    cor.owner_name_normalized,
    cs.cad_owner_name_normalized,
    upper(regexp_replace(COALESCE(p.owner_name, ''), '[^A-Za-z0-9 ]', '', 'g'))
  ) AS owner_name_normalized,
  COALESCE(pc.property_type_code, p.property_type_code) AS property_type_code,
  COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
  COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code,
  COALESCE(pc.subdivision_name, p.subdivision_name) AS subdivision_name,
  COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name,
  COALESCE(pc.homestead_flag, false) AS homestead_flag,
  concat_ws(
    ' ',
    COALESCE(
      ca.normalized_address,
      upper(
        regexp_replace(
          concat_ws(' ', COALESCE(ca.situs_address, p.situs_address), COALESCE(ca.situs_city, p.situs_city), COALESCE(ca.situs_zip, p.situs_zip)),
          '[^A-Za-z0-9 ]',
          '',
          'g'
        )
      )
    ),
    cs.account_number,
    COALESCE(p.cad_property_id, ''),
    COALESCE(cor.owner_name_normalized, cs.cad_owner_name_normalized, upper(regexp_replace(COALESCE(p.owner_name, ''), '[^A-Za-z0-9 ]', '', 'g'))),
    COALESCE(pc.subdivision_name, p.subdivision_name, ''),
    COALESCE(pc.school_district_name, p.school_district_name, '')
  ) AS search_text
FROM current_snapshots cs
JOIN parcels p
  ON p.parcel_id = cs.parcel_id
LEFT JOIN current_addresses ca
  ON ca.parcel_id = cs.parcel_id
LEFT JOIN property_characteristics pc
  ON pc.parcel_year_snapshot_id = cs.parcel_year_snapshot_id
LEFT JOIN current_owner_rollups cor
  ON cor.parcel_id = cs.parcel_id
 AND cor.tax_year = cs.tax_year;

CREATE OR REPLACE VIEW parcel_data_completeness_view AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_year_snapshot_id,
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
),
current_addresses AS (
  SELECT DISTINCT ON (pa.parcel_id)
    pa.parcel_id,
    pa.situs_address,
    pa.situs_city,
    pa.situs_zip
  FROM parcel_addresses pa
  WHERE pa.is_current = true
  ORDER BY pa.parcel_id, pa.updated_at DESC, pa.created_at DESC, pa.parcel_address_id DESC
),
tax_assignment_flags AS (
  SELECT
    ptu.parcel_id,
    ptu.tax_year,
    COUNT(*) AS tax_assignment_count,
    COUNT(*) FILTER (WHERE tu.unit_type_code = 'county') AS county_assignment_count,
    COUNT(*) FILTER (WHERE tu.unit_type_code = 'school') AS school_assignment_count
  FROM parcel_taxing_units ptu
  JOIN taxing_units tu
    ON tu.taxing_unit_id = ptu.taxing_unit_id
  GROUP BY ptu.parcel_id, ptu.tax_year
),
geometry_flags AS (
  SELECT
    pg.parcel_id,
    pg.tax_year,
    BOOL_OR(pg.geometry_role = 'parcel_polygon' AND pg.is_current) AS has_parcel_polygon,
    BOOL_OR(pg.geometry_role = 'parcel_centroid' AND pg.is_current) AS has_parcel_centroid
  FROM parcel_geometries pg
  GROUP BY pg.parcel_id, pg.tax_year
),
exemption_flags AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    COUNT(*) AS exemption_record_count
  FROM parcel_exemptions pe
  GROUP BY pe.parcel_id, pe.tax_year
),
tax_rate_flags AS (
  SELECT
    ptu.parcel_id,
    ptu.tax_year,
    SUM(COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)) AS component_effective_tax_rate
  FROM parcel_taxing_units ptu
  JOIN tax_rates tr
    ON tr.taxing_unit_id = ptu.taxing_unit_id
   AND tr.tax_year = ptu.tax_year
   AND tr.is_current = true
  GROUP BY ptu.parcel_id, ptu.tax_year
)
SELECT
  cs.county_id,
  cs.parcel_id,
  cs.tax_year,
  cs.account_number,
  (ca.parcel_id IS NOT NULL AND ca.situs_address IS NOT NULL) AS has_address,
  (pc.property_characteristic_id IS NOT NULL) AS has_characteristics,
  (pi.parcel_improvement_id IS NOT NULL) AS has_improvement,
  (pl.parcel_land_id IS NOT NULL) AS has_land,
  (pa.parcel_assessment_id IS NOT NULL) AS has_assessment,
  (COALESCE(ef.exemption_record_count, 0) > 0 OR pa.exemption_value_total IS NOT NULL) AS has_exemption_data,
  (COALESCE(taf.tax_assignment_count, 0) > 0) AS has_tax_assignments,
  (COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NOT NULL) AS has_effective_tax_rate,
  (cor.current_owner_rollup_id IS NOT NULL) AS has_owner_rollup,
  (COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false)) AS has_geometry,
  (
    (CASE WHEN ca.parcel_id IS NOT NULL AND ca.situs_address IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN pc.property_characteristic_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN pi.parcel_improvement_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN pl.parcel_land_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN pa.parcel_assessment_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN COALESCE(ef.exemption_record_count, 0) > 0 OR pa.exemption_value_total IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN COALESCE(taf.tax_assignment_count, 0) > 0 THEN 1 ELSE 0 END) +
    (CASE WHEN COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN cor.current_owner_rollup_id IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false) THEN 1 ELSE 0 END)
  ) AS completeness_component_count,
  10 AS completeness_total_count,
  ROUND(
    (
      (
        (CASE WHEN ca.parcel_id IS NOT NULL AND ca.situs_address IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN pc.property_characteristic_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN pi.parcel_improvement_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN pl.parcel_land_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN pa.parcel_assessment_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(ef.exemption_record_count, 0) > 0 OR pa.exemption_value_total IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(taf.tax_assignment_count, 0) > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN cor.current_owner_rollup_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false) THEN 1 ELSE 0 END)
      )::numeric / 10::numeric
    ) * 100.0,
    2
  ) AS completeness_score,
  (
    ca.parcel_id IS NOT NULL
    AND pa.parcel_assessment_id IS NOT NULL
    AND COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NOT NULL
    AND cor.current_owner_rollup_id IS NOT NULL
  ) AS public_summary_ready_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN ca.parcel_id IS NULL OR ca.situs_address IS NULL THEN 'missing_address' END,
      CASE WHEN pc.property_characteristic_id IS NULL THEN 'missing_characteristics' END,
      CASE WHEN pi.parcel_improvement_id IS NULL THEN 'missing_improvement' END,
      CASE WHEN pl.parcel_land_id IS NULL THEN 'missing_land' END,
      CASE WHEN pa.parcel_assessment_id IS NULL THEN 'missing_assessment' END,
      CASE
        WHEN NOT (COALESCE(ef.exemption_record_count, 0) > 0 OR pa.exemption_value_total IS NOT NULL)
        THEN 'missing_exemption_data'
      END,
      CASE WHEN COALESCE(taf.tax_assignment_count, 0) = 0 THEN 'missing_tax_assignments' END,
      CASE WHEN COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NULL THEN 'missing_effective_tax_rate' END,
      CASE WHEN cor.current_owner_rollup_id IS NULL THEN 'missing_owner_rollup' END,
      CASE
        WHEN NOT (COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false))
        THEN 'missing_geometry'
      END,
      CASE WHEN COALESCE(taf.county_assignment_count, 0) = 0 THEN 'missing_county_assignment' END,
      CASE WHEN COALESCE(taf.school_assignment_count, 0) = 0 THEN 'missing_school_assignment' END
    ],
    NULL
  ) AS warning_codes,
  cardinality(
    ARRAY_REMOVE(
      ARRAY[
        CASE WHEN ca.parcel_id IS NULL OR ca.situs_address IS NULL THEN 'missing_address' END,
        CASE WHEN pa.parcel_assessment_id IS NULL THEN 'missing_assessment' END,
        CASE WHEN COALESCE(etr.effective_tax_rate, trf.component_effective_tax_rate) IS NULL THEN 'missing_effective_tax_rate' END,
        CASE WHEN cor.current_owner_rollup_id IS NULL THEN 'missing_owner_rollup' END
      ],
      NULL
    )
  ) > 0 AS admin_review_required
FROM current_snapshots cs
LEFT JOIN current_addresses ca
  ON ca.parcel_id = cs.parcel_id
LEFT JOIN property_characteristics pc
  ON pc.parcel_year_snapshot_id = cs.parcel_year_snapshot_id
LEFT JOIN parcel_improvements pi
  ON pi.parcel_id = cs.parcel_id
 AND pi.tax_year = cs.tax_year
LEFT JOIN parcel_lands pl
  ON pl.parcel_id = cs.parcel_id
 AND pl.tax_year = cs.tax_year
LEFT JOIN parcel_assessments pa
  ON pa.parcel_id = cs.parcel_id
 AND pa.tax_year = cs.tax_year
LEFT JOIN exemption_flags ef
  ON ef.parcel_id = cs.parcel_id
 AND ef.tax_year = cs.tax_year
LEFT JOIN tax_assignment_flags taf
  ON taf.parcel_id = cs.parcel_id
 AND taf.tax_year = cs.tax_year
LEFT JOIN effective_tax_rates etr
  ON etr.parcel_id = cs.parcel_id
 AND etr.tax_year = cs.tax_year
LEFT JOIN tax_rate_flags trf
  ON trf.parcel_id = cs.parcel_id
 AND trf.tax_year = cs.tax_year
LEFT JOIN current_owner_rollups cor
  ON cor.parcel_id = cs.parcel_id
 AND cor.tax_year = cs.tax_year
LEFT JOIN geometry_flags gf
  ON gf.parcel_id = cs.parcel_id
 AND gf.tax_year = cs.tax_year;

CREATE OR REPLACE VIEW parcel_summary_view AS
WITH current_snapshots AS (
  SELECT
    pys.parcel_year_snapshot_id,
    pys.parcel_id,
    pys.county_id,
    pys.tax_year,
    pys.account_number,
    pys.cad_owner_name,
    pys.cad_owner_name_normalized
  FROM parcel_year_snapshots pys
  WHERE pys.is_current = true
),
current_addresses AS (
  SELECT DISTINCT ON (pa.parcel_id)
    pa.parcel_id,
    pa.situs_address,
    pa.situs_city,
    COALESCE(pa.situs_state, 'TX') AS situs_state,
    pa.situs_zip,
    pa.normalized_address
  FROM parcel_addresses pa
  WHERE pa.is_current = true
  ORDER BY pa.parcel_id, pa.updated_at DESC, pa.created_at DESC, pa.parcel_address_id DESC
),
raw_codes AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    array_agg(DISTINCT raw_code ORDER BY raw_code) AS raw_exemption_codes
  FROM parcel_exemptions pe
  CROSS JOIN LATERAL unnest(
    CASE
      WHEN pe.raw_exemption_codes IS NULL OR cardinality(pe.raw_exemption_codes) = 0
        THEN ARRAY[COALESCE(pe.exemption_type_code, '')]
      ELSE pe.raw_exemption_codes
    END
  ) AS raw_code
  WHERE btrim(raw_code) <> ''
  GROUP BY pe.parcel_id, pe.tax_year
),
exemption_rollup AS (
  SELECT
    pe.parcel_id,
    pe.tax_year,
    COUNT(*) AS exemption_record_count,
    COALESCE(
      SUM(pe.exemption_amount) FILTER (WHERE pe.granted_flag AND pe.exemption_amount IS NOT NULL),
      0::numeric
    ) AS granted_exemption_amount_total,
    COALESCE(
      array_agg(DISTINCT pe.exemption_type_code ORDER BY pe.exemption_type_code)
      FILTER (WHERE pe.exemption_type_code IS NOT NULL),
      ARRAY[]::text[]
    ) AS exemption_type_codes,
    COALESCE(rc.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'homestead'),
      false
    ) AS homestead_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'over65'),
      false
    ) AS over65_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled'),
      false
    ) AS disabled_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled_veteran'),
      false
    ) AS disabled_veteran_flag,
    COALESCE(
      BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'freeze'),
      false
    ) AS freeze_flag,
    COALESCE(
      BOOL_OR(pe.amount_missing_flag OR (pe.granted_flag AND pe.exemption_amount IS NULL)),
      false
    ) AS missing_exemption_amount_flag
  FROM parcel_exemptions pe
  LEFT JOIN exemption_types et
    ON et.exemption_type_code = pe.exemption_type_code
  LEFT JOIN raw_codes rc
    ON rc.parcel_id = pe.parcel_id
   AND rc.tax_year = pe.tax_year
  GROUP BY pe.parcel_id, pe.tax_year, rc.raw_exemption_codes
),
component_rows AS (
  SELECT
    ptu.parcel_id,
    ptu.tax_year,
    tu.unit_type_code,
    tu.unit_code,
    tu.unit_name,
    tr.rate_component,
    COALESCE(tr.rate_value, tr.rate_per_100 / 100.0) AS rate_value,
    tr.rate_per_100,
    ptu.assignment_method,
    ptu.assignment_confidence,
    ptu.is_primary,
    ptu.assignment_reason_code,
    ptu.match_basis_json
  FROM parcel_taxing_units ptu
  JOIN taxing_units tu
    ON tu.taxing_unit_id = ptu.taxing_unit_id
  JOIN tax_rates tr
    ON tr.taxing_unit_id = ptu.taxing_unit_id
   AND tr.tax_year = ptu.tax_year
   AND tr.is_current = true
),
tax_rollup AS (
  SELECT
    cr.parcel_id,
    cr.tax_year,
    SUM(cr.rate_value) AS component_effective_tax_rate,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'county') AS county_assignment_count,
    COUNT(*) FILTER (WHERE cr.unit_type_code = 'school') AS school_assignment_count,
    jsonb_agg(
      jsonb_build_object(
        'unit_type_code', cr.unit_type_code,
        'unit_code', cr.unit_code,
        'unit_name', cr.unit_name,
        'rate_component', cr.rate_component,
        'rate_value', cr.rate_value,
        'rate_per_100', cr.rate_per_100,
        'assignment_method', cr.assignment_method,
        'assignment_confidence', cr.assignment_confidence,
        'assignment_reason_code', cr.assignment_reason_code,
        'is_primary', cr.is_primary,
        'match_basis_json', cr.match_basis_json
      )
      ORDER BY cr.unit_type_code, cr.unit_name, cr.rate_component
    ) AS component_breakdown_json
  FROM component_rows cr
  GROUP BY cr.parcel_id, cr.tax_year
),
geometry_flags AS (
  SELECT
    pg.parcel_id,
    pg.tax_year,
    BOOL_OR(pg.geometry_role = 'parcel_polygon' AND pg.is_current) AS has_parcel_polygon,
    BOOL_OR(pg.geometry_role = 'parcel_centroid' AND pg.is_current) AS has_parcel_centroid
  FROM parcel_geometries pg
  GROUP BY pg.parcel_id, pg.tax_year
),
summary_basis AS (
  SELECT
    cs.county_id,
    cs.parcel_id,
    cs.tax_year,
    cs.account_number,
    p.cad_property_id,
    COALESCE(ca.situs_address, p.situs_address) AS situs_address,
    COALESCE(ca.situs_city, p.situs_city) AS situs_city,
    COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')) AS situs_state,
    COALESCE(ca.situs_zip, p.situs_zip) AS situs_zip,
    COALESCE(ca.normalized_address, upper(regexp_replace(COALESCE(ca.situs_address, p.situs_address, ''), '[^A-Za-z0-9 ]', '', 'g'))) AS normalized_address,
    COALESCE(cor.owner_name, cs.cad_owner_name, p.owner_name) AS owner_name,
    COALESCE(cor.owner_name_normalized, cs.cad_owner_name_normalized) AS owner_name_normalized,
    cor.source_basis AS owner_source_basis,
    cor.confidence_score AS owner_confidence_score,
    COALESCE(cor.override_flag, false) AS owner_override_flag,
    cs.cad_owner_name,
    cs.cad_owner_name_normalized,
    COALESCE(pc.property_type_code, p.property_type_code) AS property_type_code,
    COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
    COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code,
    COALESCE(pc.subdivision_name, p.subdivision_name) AS subdivision_name,
    COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name,
    pi.living_area_sf,
    pi.year_built,
    pi.effective_year_built,
    COALESCE(pi.effective_age, pc.effective_age) AS effective_age,
    pi.bedrooms,
    pi.full_baths,
    pi.half_baths,
    pi.stories,
    pi.quality_code,
    pi.condition_code,
    pi.garage_spaces,
    pi.pool_flag,
    pl.land_sf,
    pl.land_acres,
    pl.frontage_sf,
    pl.depth_sf,
    pa.market_value,
    pa.assessed_value,
    pa.capped_value,
    pa.appraised_value,
    pa.certified_value,
    pa.notice_value,
    pa.exemption_value_total,
    pa.homestead_flag AS assessment_homestead_flag,
    COALESCE(er.exemption_record_count, 0) AS exemption_record_count,
    COALESCE(er.exemption_type_codes, ARRAY[]::text[]) AS exemption_type_codes,
    COALESCE(er.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
    COALESCE(er.homestead_flag, false) AS homestead_flag,
    COALESCE(er.over65_flag, false) AS over65_flag,
    COALESCE(er.disabled_flag, false) AS disabled_flag,
    COALESCE(er.disabled_veteran_flag, false) AS disabled_veteran_flag,
    COALESCE(er.freeze_flag, false) AS freeze_flag,
    COALESCE(er.missing_exemption_amount_flag, false) AS missing_exemption_amount_flag,
    (
      pa.exemption_value_total IS NOT NULL
      AND ABS(COALESCE(er.granted_exemption_amount_total, 0) - pa.exemption_value_total) > 0.01
    ) AS assessment_exemption_total_mismatch_flag,
    (
      COALESCE(er.freeze_flag, false)
      AND NOT (
        COALESCE(er.over65_flag, false)
        OR COALESCE(er.disabled_flag, false)
        OR COALESCE(er.disabled_veteran_flag, false)
      )
    ) AS freeze_without_qualifying_exemption_flag,
    COALESCE(etr.effective_tax_rate, tr.component_effective_tax_rate) AS effective_tax_rate,
    COALESCE(tr.component_breakdown_json, '[]'::jsonb) AS component_breakdown_json,
    COALESCE(tr.county_assignment_count, 0) AS county_assignment_count,
    COALESCE(tr.school_assignment_count, 0) AS school_assignment_count,
    COALESCE(gf.has_parcel_polygon, false) AS has_parcel_polygon,
    COALESCE(gf.has_parcel_centroid, false) AS has_parcel_centroid,
    pc.property_characteristic_id IS NOT NULL AS has_characteristics,
    pi.parcel_improvement_id IS NOT NULL AS has_improvement,
    pl.parcel_land_id IS NOT NULL AS has_land,
    pa.parcel_assessment_id IS NOT NULL AS has_assessment,
    cor.current_owner_rollup_id IS NOT NULL AS has_owner_rollup,
    COALESCE(etr.effective_tax_rate, tr.component_effective_tax_rate) IS NOT NULL AS has_effective_tax_rate
  FROM current_snapshots cs
  JOIN parcels p
    ON p.parcel_id = cs.parcel_id
  LEFT JOIN current_addresses ca
    ON ca.parcel_id = cs.parcel_id
  LEFT JOIN property_characteristics pc
    ON pc.parcel_year_snapshot_id = cs.parcel_year_snapshot_id
  LEFT JOIN parcel_improvements pi
    ON pi.parcel_id = cs.parcel_id
   AND pi.tax_year = cs.tax_year
  LEFT JOIN parcel_lands pl
    ON pl.parcel_id = cs.parcel_id
   AND pl.tax_year = cs.tax_year
  LEFT JOIN parcel_assessments pa
    ON pa.parcel_id = cs.parcel_id
   AND pa.tax_year = cs.tax_year
  LEFT JOIN exemption_rollup er
    ON er.parcel_id = cs.parcel_id
   AND er.tax_year = cs.tax_year
  LEFT JOIN effective_tax_rates etr
    ON etr.parcel_id = cs.parcel_id
   AND etr.tax_year = cs.tax_year
  LEFT JOIN tax_rollup tr
    ON tr.parcel_id = cs.parcel_id
   AND tr.tax_year = cs.tax_year
  LEFT JOIN current_owner_rollups cor
    ON cor.parcel_id = cs.parcel_id
   AND cor.tax_year = cs.tax_year
  LEFT JOIN geometry_flags gf
    ON gf.parcel_id = cs.parcel_id
   AND gf.tax_year = cs.tax_year
)
SELECT
  sb.county_id,
  sb.parcel_id,
  sb.tax_year,
  sb.account_number,
  sb.cad_property_id,
  sb.situs_address,
  sb.situs_city,
  sb.situs_state,
  sb.situs_zip,
  sb.normalized_address,
  concat_ws(
    ', ',
    sb.situs_address,
    sb.situs_city,
    concat_ws(' ', sb.situs_state, sb.situs_zip)
  ) AS address,
  sb.owner_name,
  sb.owner_name_normalized,
  sb.owner_source_basis,
  sb.owner_confidence_score,
  sb.owner_override_flag,
  sb.cad_owner_name,
  sb.cad_owner_name_normalized,
  sb.property_type_code,
  sb.property_class_code,
  sb.neighborhood_code,
  sb.subdivision_name,
  sb.school_district_name,
  sb.living_area_sf,
  sb.year_built,
  sb.effective_year_built,
  sb.effective_age,
  sb.bedrooms,
  sb.full_baths,
  sb.half_baths,
  sb.stories,
  sb.quality_code,
  sb.condition_code,
  sb.garage_spaces,
  sb.pool_flag,
  sb.land_sf,
  sb.land_acres,
  sb.frontage_sf,
  sb.depth_sf,
  sb.market_value,
  sb.assessed_value,
  sb.capped_value,
  sb.appraised_value,
  sb.certified_value,
  sb.notice_value,
  sb.exemption_value_total,
  sb.exemption_record_count,
  sb.exemption_type_codes,
  sb.raw_exemption_codes,
  sb.homestead_flag,
  sb.over65_flag,
  sb.disabled_flag,
  sb.disabled_veteran_flag,
  sb.freeze_flag,
  sb.effective_tax_rate,
  CASE
    WHEN COALESCE(sb.certified_value, sb.appraised_value, sb.assessed_value, sb.market_value, sb.notice_value) IS NULL
      THEN NULL
    ELSE GREATEST(
      COALESCE(sb.certified_value, sb.appraised_value, sb.assessed_value, sb.market_value, sb.notice_value) - COALESCE(sb.exemption_value_total, 0),
      0
    )
  END AS estimated_taxable_value,
  CASE
    WHEN sb.effective_tax_rate IS NULL
      OR COALESCE(sb.certified_value, sb.appraised_value, sb.assessed_value, sb.market_value, sb.notice_value) IS NULL
      THEN NULL
    ELSE GREATEST(
      COALESCE(sb.certified_value, sb.appraised_value, sb.assessed_value, sb.market_value, sb.notice_value) - COALESCE(sb.exemption_value_total, 0),
      0
    ) * sb.effective_tax_rate
  END AS estimated_annual_tax,
  sb.component_breakdown_json,
  sb.has_parcel_polygon,
  sb.has_parcel_centroid,
  ROUND(
    (
      (
        (CASE WHEN sb.situs_address IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_characteristics THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_improvement THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_land THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_assessment THEN 1 ELSE 0 END) +
        (CASE WHEN sb.exemption_record_count > 0 OR sb.exemption_value_total IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN sb.county_assignment_count > 0 OR sb.school_assignment_count > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_effective_tax_rate THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_owner_rollup THEN 1 ELSE 0 END) +
        (CASE WHEN sb.has_parcel_polygon OR sb.has_parcel_centroid THEN 1 ELSE 0 END)
      )::numeric / 10::numeric
    ) * 100.0,
    2
  ) AS completeness_score,
  (
    sb.situs_address IS NOT NULL
    AND sb.has_assessment
    AND sb.has_effective_tax_rate
    AND sb.has_owner_rollup
  ) AS public_summary_ready_flag,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN sb.situs_address IS NULL THEN 'missing_address' END,
      CASE WHEN NOT sb.has_characteristics THEN 'missing_characteristics' END,
      CASE WHEN NOT sb.has_improvement THEN 'missing_improvement' END,
      CASE WHEN NOT sb.has_land THEN 'missing_land' END,
      CASE WHEN NOT sb.has_assessment THEN 'missing_assessment' END,
      CASE WHEN sb.exemption_record_count = 0 AND sb.exemption_value_total IS NULL THEN 'missing_exemption_data' END,
      CASE WHEN sb.county_assignment_count = 0 THEN 'missing_county_assignment' END,
      CASE WHEN sb.school_assignment_count = 0 THEN 'missing_school_assignment' END,
      CASE WHEN NOT sb.has_effective_tax_rate THEN 'missing_effective_tax_rate' END,
      CASE WHEN NOT sb.has_owner_rollup THEN 'missing_owner_rollup' END,
      CASE
        WHEN sb.owner_name IS NOT NULL
          AND sb.cad_owner_name IS NOT NULL
          AND sb.owner_name IS DISTINCT FROM sb.cad_owner_name
          AND COALESCE(sb.owner_override_flag, false) = false
        THEN 'cad_owner_mismatch'
      END,
      CASE WHEN sb.missing_exemption_amount_flag THEN 'missing_exemption_amount' END,
      CASE
        WHEN sb.assessment_exemption_total_mismatch_flag
        THEN 'assessment_exemption_total_mismatch'
      END,
      CASE
        WHEN sb.assessment_homestead_flag IS NOT NULL
          AND sb.assessment_homestead_flag IS DISTINCT FROM sb.homestead_flag
        THEN 'homestead_flag_mismatch'
      END,
      CASE
        WHEN sb.freeze_without_qualifying_exemption_flag
        THEN 'freeze_without_qualifying_exemption'
      END,
      CASE
        WHEN NOT (sb.has_parcel_polygon OR sb.has_parcel_centroid)
        THEN 'missing_geometry'
      END
    ],
    NULL
  ) AS warning_codes,
  cardinality(
    ARRAY_REMOVE(
      ARRAY[
        CASE WHEN sb.situs_address IS NULL THEN 'missing_address' END,
        CASE WHEN NOT sb.has_assessment THEN 'missing_assessment' END,
        CASE WHEN NOT sb.has_effective_tax_rate THEN 'missing_effective_tax_rate' END,
        CASE WHEN NOT sb.has_owner_rollup THEN 'missing_owner_rollup' END
      ],
      NULL
    )
  ) > 0 AS admin_review_required
FROM summary_basis sb;

CREATE OR REPLACE VIEW v_search_read_model AS
SELECT
  county_id,
  account_number,
  parcel_id,
  situs_address,
  situs_zip,
  normalized_address,
  owner_name
FROM parcel_search_view;

COMMENT ON VIEW parcel_owner_current_view IS 'Derived parcel-year owner summary for app and admin use, preserving CAD snapshot context alongside the selected current owner rollup.';
COMMENT ON VIEW parcel_effective_tax_rate_view IS 'Derived parcel-year tax-rate summary with component breakdown and assignment warning flags for app and admin review.';
COMMENT ON VIEW parcel_tax_estimate_summary IS 'Derived parcel-year tax estimate summary built from canonical assessment, exemption, and effective tax-rate facts only.';
COMMENT ON VIEW parcel_search_view IS 'Internal parcel search-support view sourced from canonical parcel, address, owner, and characteristic tables; the public search contract remains v_search_read_model.';
COMMENT ON VIEW parcel_data_completeness_view IS 'Admin-facing parcel-year completeness scoring and warning view computed from canonical parcel, tax, owner, exemption, and geometry coverage.';
COMMENT ON VIEW parcel_summary_view IS 'Primary derived parcel-year summary view for app and admin screens, combining parcel identity, owner, assessment, exemption, tax, and completeness fields.';
