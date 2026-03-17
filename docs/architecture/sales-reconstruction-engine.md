# Sales Reconstruction Engine (Final Reconciled)

Role: normalize sale evidence into `parcel_sales` while keeping restricted MLS/listing data separate.

Pattern:
`restricted/raw -> normalized parcel_sales -> neighborhood_stats -> comp generation -> quote-safe outputs`

Key fields for `parcel_sales`:
- sale_date
- sale_price
- list_price
- days_on_market
- sale_price_psf
- time_adjusted_price
- validity_code
- arms_length_flag
- restricted_flag
