# Instant Quote API Contract

Route:
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`

Supported response shape:
- `supported`
- `county_id`
- `tax_year`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `data_freshness_label`
- `account_number`
- `basis_code`
- `subject`
- `estimate`
- `explanation`
- `disclaimers`

Unsupported response shape:
- `supported = false`
- all standard parcel-year fallback metadata above
- `basis_code`
- `subject`
- `explanation`
- `unsupported_reason`
- `next_step_cta`

Public-safe exclusions:
- raw confidence score
- target assessed value
- target PSF
- sample variance internals
- internal support flags
- MLS or listing artifacts

Estimate object:
- `savings_range_low`
- `savings_range_high`
- `savings_midpoint_display`
- `estimate_bucket`
- `estimate_strength_label`
- `tax_protection_limited`
- `tax_protection_note`

Explanation object:
- `methodology`
- `estimate_strength_label`
- `summary`
- `bullets`
- `limitation_note`
