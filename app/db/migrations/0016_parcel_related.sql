CREATE TABLE IF NOT EXISTS parcel_aliases (
  parcel_alias_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  alias_type text NOT NULL,
  alias_value text NOT NULL,
  UNIQUE(parcel_id, alias_type, alias_value)
);
CREATE TABLE IF NOT EXISTS parcel_addresses (
  parcel_address_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id uuid NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
  situs_address text,
  situs_city text,
  situs_state text,
  situs_zip text,
  normalized_address text,
  latitude numeric,
  longitude numeric,
  is_current boolean NOT NULL DEFAULT true
);
