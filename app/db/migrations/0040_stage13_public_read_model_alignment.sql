CREATE OR REPLACE VIEW v_search_read_model AS
SELECT
  county_id,
  tax_year,
  account_number,
  parcel_id,
  COALESCE(NULLIF(document_json ->> 'address', ''), display_address) AS address,
  COALESCE(
    NULLIF(document_json ->> 'situs_address', ''),
    split_part(COALESCE(NULLIF(document_json ->> 'address', ''), display_address), ',', 1)
  ) AS situs_address,
  NULLIF(document_json ->> 'situs_zip', '') AS situs_zip,
  normalized_address,
  NULLIF(document_json ->> 'owner_name', '') AS owner_name
FROM search_documents;

COMMENT ON VIEW v_search_read_model IS 'Public parcel search read model backed by search_documents for interactive lookup latency while preserving the stable v_search_read_model contract, parcel-year identity, and public-safe display fields.';
