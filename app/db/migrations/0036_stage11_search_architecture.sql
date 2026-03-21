CREATE INDEX IF NOT EXISTS idx_search_documents_owner_trgm
  ON search_documents
  USING gin (normalized_owner_name gin_trgm_ops);

CREATE OR REPLACE FUNCTION dwellio_refresh_search_documents(
  p_county_id text DEFAULT NULL,
  p_tax_year integer DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  refreshed_count integer := 0;
BEGIN
  WITH source_rows AS (
    SELECT
      psv.parcel_id,
      psv.county_id,
      psv.tax_year,
      psv.account_number,
      psv.normalized_address,
      psv.owner_name_normalized AS normalized_owner_name,
      psv.address AS display_address,
      psv.search_text,
      jsonb_build_object(
        'county_id', psv.county_id,
        'tax_year', psv.tax_year,
        'account_number', psv.account_number,
        'cad_property_id', psv.cad_property_id,
        'situs_address', psv.situs_address,
        'situs_city', psv.situs_city,
        'situs_state', psv.situs_state,
        'situs_zip', psv.situs_zip,
        'address', psv.address,
        'owner_name', psv.owner_name,
        'property_type_code', psv.property_type_code,
        'property_class_code', psv.property_class_code,
        'neighborhood_code', psv.neighborhood_code,
        'subdivision_name', psv.subdivision_name,
        'school_district_name', psv.school_district_name,
        'homestead_flag', psv.homestead_flag
      ) AS document_json
    FROM parcel_search_view psv
    WHERE (p_county_id IS NULL OR psv.county_id = p_county_id)
      AND (p_tax_year IS NULL OR psv.tax_year = p_tax_year)
  ),
  deleted AS (
    DELETE FROM search_documents sd
    WHERE (p_county_id IS NULL OR sd.county_id = p_county_id)
      AND (p_tax_year IS NULL OR sd.tax_year = p_tax_year)
      AND NOT EXISTS (
        SELECT 1
        FROM source_rows sr
        WHERE sr.parcel_id = sd.parcel_id
          AND sr.tax_year = sd.tax_year
      )
    RETURNING 1
  ),
  upserted AS (
    INSERT INTO search_documents (
      parcel_id,
      county_id,
      tax_year,
      account_number,
      normalized_address,
      normalized_owner_name,
      display_address,
      search_text,
      document_json
    )
    SELECT
      parcel_id,
      county_id,
      tax_year,
      account_number,
      normalized_address,
      normalized_owner_name,
      display_address,
      search_text,
      document_json
    FROM source_rows
    ON CONFLICT (parcel_id, tax_year)
    DO UPDATE SET
      county_id = EXCLUDED.county_id,
      account_number = EXCLUDED.account_number,
      normalized_address = EXCLUDED.normalized_address,
      normalized_owner_name = EXCLUDED.normalized_owner_name,
      display_address = EXCLUDED.display_address,
      search_text = EXCLUDED.search_text,
      document_json = EXCLUDED.document_json,
      updated_at = now()
    RETURNING 1
  )
  SELECT COUNT(*) INTO refreshed_count
  FROM upserted;

  RETURN refreshed_count;
END;
$$;

CREATE OR REPLACE VIEW v_search_read_model AS
SELECT
  county_id,
  account_number,
  parcel_id,
  COALESCE(NULLIF(document_json ->> 'situs_address', ''), display_address) AS situs_address,
  NULLIF(document_json ->> 'situs_zip', '') AS situs_zip,
  normalized_address,
  NULLIF(document_json ->> 'owner_name', '') AS owner_name
FROM search_documents;

SELECT dwellio_refresh_search_documents(NULL, NULL);

COMMENT ON FUNCTION dwellio_refresh_search_documents(text, integer) IS 'Rebuilds the search_documents support table from parcel_search_view for the provided county/tax year scope.';
COMMENT ON VIEW v_search_read_model IS 'Public parcel search read model backed by search_documents for interactive lookup latency while preserving the stable v_search_read_model contract.';
