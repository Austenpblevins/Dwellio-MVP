# DWELLIO_CODEX_CONTEXT.md

## Purpose

This is the compact canonical context file for Codex. Use it as the implementation reference alongside:
- DWELLIO_MASTER_SPEC.md as the compatibility entry point to the canonical source-of-truth set
- DWELLIO_BUILD_PLAN.md
- AGENT_RULES.md

This file is intentionally shorter than the full master spec and captures the highest-priority architecture, data, and implementation rules.

## Product Goal

Dwellio is a Texas property tax protest platform that must:
1. resolve a homeowner address to a county appraisal parcel
2. estimate a defensible protest value using both market-value and unequal-appraisal logic
3. convert that into a tax savings estimate and protest recommendation
4. capture and qualify leads
5. manage signed protest cases through informal and ARB stages
6. generate evidence packets
7. learn from outcomes to improve win probability, reduction prediction, and case selection

## MVP Scope

Counties:
- Harris
- Fort Bend

Property type:
- single-family residential only

Required MVP capabilities:
- address search
- parcel match
- instant quote
- protest recommendation
- lead capture
- signed representation flow
- protest case creation
- nightly ETL
- precomputed quote cache

## Core Architecture Rule

This is not a generic consumer AVM.

For each parcel-year:
- compute a market-value model
- compute an unequal-appraisal model
- final defensible value = min(market_value_model_output, unequal_appraisal_model_output)

The most important output is the lowest supportable protest value.

## Data Architecture

Use:
raw -> staging -> canonical -> derived/read models

Do not collapse these layers.

### Raw
Store county bulk files, restricted feeds, and source files as received.

### Staging
Use county-specific staging tables.

### Canonical
Normalize into parcel-centered production tables.

### Derived
Precompute:
- parcel_features
- neighborhood stats
- comp candidates
- valuation runs
- savings estimates
- quote explanations
- quote read model

## Public Quote Path Rule

Do not run heavy comp analysis in the public request path.

Public quote flow:
address input
-> parcel resolution
-> fetch precomputed quote-ready row
-> return quote + explanation + recommendation

## County Adapter Rule

Each county gets its own adapter.
Required now:
- Harris adapter
- Fort Bend adapter

No universal parser.

## MLS / Restricted Data Rule

If MLS data is used:
- raw MLS stays in restricted tables
- agent remarks stay restricted
- listing history stays restricted
- public APIs only expose derived/public-safe outputs

Required pattern:
restricted raw -> derived features -> model -> public-safe quote output

## ETL Pipeline

Implement nightly jobs in this order:
1. fetch county source files
2. archive raw files to object storage
3. checksum + register import batch
4. unzip / parse / encoding cleanup
5. load staging tables
6. normalize to canonical parcel warehouse
7. repair geometry / geocode
8. ingest sales
9. build features
10. build comp candidates
11. run valuation models
12. score savings
13. refresh quote cache / read model

All ETL must be idempotent.

## Comp Selection Rules

### Market-sale comps
Filters:
- same county
- same broad property type = SFR
- trailing 12 months
- same neighborhood if enough sales, otherwise adjacent neighborhoods or same school district
- living area within ±25%
- year built within ±15 years
- exclude obvious non-arms-length transfers

Similarity score:
0.30 * abs(sf_diff_pct)
+ 0.15 * min(distance_miles / 5, 1)
+ 0.15 * min(age_diff / 20, 1)
+ 0.10 * abs(bed_diff)
+ 0.10 * abs(bath_diff)
+ 0.10 * abs(lot_diff_pct)
+ 0.05 * story_penalty
+ 0.05 * quality_penalty

### Equity comps
Filters:
- same county and tax year
- same property type
- same neighborhood or neighborhood group
- living area within ±20%
- year built within ±10 years
- homestead matched where possible
- similar condition / quality where available

## Valuation Model Rules

### Market model
market_estimate_nbhd = subject_living_area_sf * neighborhood_median_sale_psf_12m

market_estimate_comp =
  weighted_median(adjusted_comp_value_psf) * subject_living_area_sf

market_value_point =
  0.40 * market_estimate_nbhd
+ 0.60 * market_estimate_comp

### Equity model
equity_value_point =
  median(adjusted_comp_psf of top 15 equity comps) * subject_living_area_sf

### Defensible value
defensible_value_point = min(market_value_point, equity_value_point)

## Protest Decision Tree Rules

Persist every rule result.

Required rules:
1. value gap detection
2. neighborhood equalization
3. comp validation
4. minimum savings threshold
5. homestead cap check
6. condition / functional obsolescence flags
7. final confidence threshold

Initial residential minimum gap threshold:
- 5%

Initial minimum savings threshold:
- 300 to 500

## Savings Model Rules

projected_reduction_point =
  max(0, current_notice_value - defensible_value_point)

gross_tax_savings_point =
  projected_reduction_point * effective_tax_rate

Use rule-based success probability for MVP:
- 0.80 high
- 0.65 medium-high
- 0.50 moderate
- 0.30 low

expected_tax_savings_point =
  gross_tax_savings_point * success_probability

estimated_contingency_fee =
  expected_tax_savings_point * contingency_rate

## Required Public APIs
- GET /search?address={query}
- GET /quote/{county_id}/{tax_year}/{account_number}
- GET /quote/{county_id}/{tax_year}/{account_number}/explanation
- POST /lead

## Required MVP Table Groups

Foundations:
- counties
- appraisal_districts
- tax_years
- county_configs
- source_systems
- county_field_mappings
- workflow_statuses
- hearing_types
- job_runs

Ingestion:
- import_batches
- raw_files
- ingest_errors
- stg_county_property_raw
- stg_county_tax_rates_raw
- stg_sales_raw
- stg_gis_raw

Parcel warehouse:
- parcels
- parcel_addresses
- parcel_improvements
- parcel_lands
- parcel_assessments
- parcel_exemptions

Tax and sales:
- taxing_units
- parcel_taxing_units
- effective_tax_rates
- sales_raw
- parcel_sales
- neighborhood_stats

Quote engine:
- parcel_features
- comp_candidate_pools
- comp_candidates
- valuation_runs
- parcel_savings_estimates
- quote_explanations
- decision_tree_results
- protest_recommendations

Business:
- leads
- lead_events
- clients
- representation_agreements
- protest_cases
- case_outcomes

## Acceptance Standard

The first production milestone is complete when:
- Harris and Fort Bend parcel data can be ingested
- address search returns parcel candidates
- parcel-year quotes work from precomputed data
- quote returns defensible value, savings range, confidence, and recommendation
- leads can be created
- signed agreements can create protest cases
- case outcomes can be stored
- nightly ETL is idempotent and observable
- restricted MLS data is segregated from public output
