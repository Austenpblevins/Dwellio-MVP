ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS account_number text,
  ADD COLUMN IF NOT EXISTS owner_name text,
  ADD COLUMN IF NOT EXISTS consent_to_contact boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN leads.account_number IS 'Requested parcel account number captured for lead-funnel attribution and parcel-year context even when parcel resolution is incomplete.';
COMMENT ON COLUMN leads.owner_name IS 'Submitted owner label from the lead funnel; stored additively without replacing first_name/last_name workflow fields.';
COMMENT ON COLUMN leads.consent_to_contact IS 'Public lead-contact consent flag captured from the canonical POST /lead route.';
