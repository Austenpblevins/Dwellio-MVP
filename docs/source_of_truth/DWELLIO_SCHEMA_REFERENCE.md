# DWELLIO_SCHEMA_REFERENCE.md

## Purpose

This file is the schema reference for Dwellio. It is designed to help Codex and human developers quickly understand:

- every major table group
- what each table is for
- which phase it belongs to
- which tables are MVP-critical
- the most important foreign-key relationships
- the expected data flow between schema layers

This file is not a replacement for the SQL migrations.
It is the schema map and implementation guide.

---

# 1. Schema Design Principles

Dwellio uses a layered data architecture:

1. **Raw**
2. **Staging**
3. **Canonical**
4. **Derived / read models**
5. **Operational / workflow**
6. **Analytics / learning**

Core rules:

- Do not expose raw or restricted records directly to public APIs.
- Do not overwrite historical valuation runs.
- Treat `parcel + tax_year` as a core business identity.
- Keep county-specific parsing logic out of canonical tables.
- Use derived tables for fast quoting.
- Persist outcomes for long-term model improvement.

---

# 2. Phase Tags

Use these phase tags throughout implementation.

- **MVP** = must exist for initial Harris + Fort Bend launch
- **P2** = phase 2 operational enhancement
- **P3** = phase 3 scale / statewide / ML enhancement

---

# 3. Schema Domains Overview

## A. Reference / Admin
Defines counties, tax years, hearing types, workflow statuses, code mappings.

## B. Raw File / Ingestion
Tracks imports, raw files, staging rows, errors, and ETL job runs.

## C. Parcel / Property Master
Stores parcel identity, addresses, ownership, improvements, land, assessments, exemptions, photos.

## D. Tax / Jurisdiction
Stores taxing units, parcel-to-tax-unit mapping, effective tax rates, and county appeal rules.

## E. Sales / MLS / Market
Stores raw and normalized sales, MLS-restricted tables, neighborhood statistics, market segmentation, and adjustment factors.

## F. Comp Engine / Feature Store
Stores feature-engineered subject rows, comp candidate pools, comp adjustments, and model input features.

## G. Valuation / Quote
Stores valuation runs, quote runs, savings estimates, decision-tree outcomes, explanations, and recommendations.

## H. Leads / CRM / Clients
Stores leads, lead events, clients, parcel relationships, and representation agreements.

## I. Protest Operations
Stores protest cases, assignments, notes, hearings, participants, status history, and final outcomes.

## J. Evidence / Packet Generation
Stores evidence packet structures, selected comp sets, condition reports, photos, and argument records.

## K. Billing / Financials
Stores invoices, payments, collections, and profitability records.

## L. Analytics / Learning / Internal Ops
Stores training datasets, neighborhood performance, model registries, attribution, and internal user/audit records.

---

# 4. Full Table Reference

---

## A. Reference / Admin Tables

### 1. `states` — P2
Purpose:
- optional geographic normalization for future expansion beyond Texas

Key fields:
- `state_code` (PK)
- `state_name`

---

### 2. `counties` — MVP
Purpose:
- county master list
- referenced by most other domain tables

Key fields:
- `county_id` (PK, recommended text like `harris`, `fort_bend`)
- `county_name`
- `timezone`
- `is_active`

Important relationships:
- parent for appraisal districts
- parent for county configs
- parent for tax, parcel, staging, and job tables

---

### 3. `appraisal_districts` — MVP
Purpose:
- map county to appraisal district/CAD

Key fields:
- `appraisal_district_id` (PK)
- `county_id` (FK)
- `district_name`
- `short_name`
- `website_url`
- `bulk_download_url`

---

### 4. `tax_years` — MVP
Purpose:
- normalize annual settings

Key fields:
- `tax_year` (PK)
- `valuation_date`
- `protest_deadline_default`
- `certified_roll_date`

---

### 5. `county_configs` — MVP
Purpose:
- store county-specific logic and adapter metadata

Key fields:
- `county_config_id`
- `county_id`
- `tax_year`
- `appraisal_district_id`
- `parser_module_name`
- `protest_deadline_rule`
- `homestead_cap_percent`
- `data_refresh_frequency`

Important note:
- county-specific business rules should live here or in county adapters, not scattered throughout generic services

---

### 6. `property_type_codes` — P2
Purpose:
- normalize property types across counties

Examples:
- `sfr`
- `condo`
- `mf_small`
- `commercial`

---

### 7. `class_code_mappings` — P2
Purpose:
- map county source class codes into normalized property types

---

### 8. `exemption_types` — P2
Purpose:
- normalized exemption catalog

Examples:
- homestead
- over65
- disabled_vet

---

### 9. `hearing_types` — MVP
Purpose:
- normalize hearing stage types

Examples:
- informal
- arb

---

### 10. `workflow_statuses` — MVP
Purpose:
- reusable statuses for lead/client/case workflow

---

## B. Raw File / Ingestion Tables

### 11. `source_systems` — MVP
Purpose:
- define data sources and whether they are restricted

Examples:
- HCAD_BULK
- FBCAD_EXPORT
- MLS_INTERNAL
- ATTOM
- DEED_FEED

Important field:
- `restricted_flag`

---

### 12. `import_batches` — MVP
Purpose:
- one row per import event

Key fields:
- `import_batch_id`
- `source_system_id`
- `county_id`
- `tax_year`
- `source_filename`
- `source_checksum`
- `status`
- `row_count`
- `error_count`

---

### 13. `raw_files` — MVP
Purpose:
- metadata for archived raw files in object storage

Key fields:
- `storage_path`
- `original_filename`
- `checksum`

---

### 14. `ingest_errors` — MVP
Purpose:
- failed row logging and parser diagnostics

---

### 15. `county_field_mappings` — MVP
Purpose:
- source-to-canonical field map by county

This is one of the most important statewide scalability tables.

---

### 16. `job_runs` — MVP
Purpose:
- ETL/job orchestration tracking

Used by:
- nightly jobs
- ETL observability
- rerun debugging

---

### 17. `stg_county_property_raw` — MVP
Purpose:
- generic county property staging

---

### 18. `stg_county_tax_rates_raw` — MVP
Purpose:
- generic county tax-rate staging

---

### 19. `stg_sales_raw` — MVP
Purpose:
- generic sales staging

---

### 20. `stg_gis_raw` — MVP
Purpose:
- generic GIS staging

---

## C. Parcel / Property Master Tables

### 21. `parcels` — MVP
Purpose:
- parcel identity master
- canonical parcel anchor table

Core fields:
- `parcel_id` (PK)
- `county_id`
- `appraisal_district_id`
- `tax_year`
- `account_number`
- `cad_property_id`
- `situs_address`
- `situs_city`
- `situs_zip`
- `owner_name`
- `property_type_code`
- `property_class_code`
- `neighborhood_code`
- `subdivision_name`
- `school_district_name`
- `latitude`
- `longitude`
- `geom`

Critical note:
- this is the primary identity anchor for quote and protest workflows

---

### 22. `parcel_aliases` — P2
Purpose:
- alternate identifiers for parcel lookup

Examples:
- geo account
- quick ref
- alternate CAD ID

---

### 23. `parcel_addresses` — MVP
Purpose:
- normalized address components for search and display

---

### 24. `parcel_geometries` — P2
Purpose:
- alternate or historical geometry sources

---

### 25. `parcel_ownership_history` — P2
Purpose:
- owner history tracking

---

### 26. `parcel_improvements` — MVP
Purpose:
- annual building characteristics

Important fields:
- `living_area_sf`
- `year_built`
- `effective_year_built`
- `effective_age`
- `bedrooms`
- `full_baths`
- `half_baths`
- `stories`
- `quality_code`
- `condition_code`
- `garage_spaces`
- `pool_flag`

Used by:
- feature generation
- comp filtering
- valuation models

---

### 27. `parcel_lands` — MVP
Purpose:
- annual site/land characteristics

Important fields:
- `land_sf`
- `land_acres`
- `frontage_sf`
- `depth_sf`

---

### 28. `parcel_amenities` — P2
Purpose:
- flexible amenity storage

Examples:
- guest house
- covered patio
- outbuilding

---

### 29. `parcel_assessments` — MVP
Purpose:
- annual valuation snapshot

Important fields:
- `land_value`
- `improvement_value`
- `market_value`
- `assessed_value`
- `capped_value`
- `appraised_value`
- `exemption_value_total`
- `notice_value`
- `certified_value`
- `prior_year_market_value`
- `prior_year_assessed_value`

Critical note:
- this table drives savings logic and protest analysis

---

### 30. `parcel_exemptions` — MVP
Purpose:
- annual detailed exemption tracking

---

### 31. `parcel_taxing_units` — MVP
Purpose:
- join parcels to taxing units

---

### 32. `parcel_photos` — P2
Purpose:
- subject property photos

---

## D. Tax / Jurisdiction Tables

### 33. `taxing_units` — MVP
Purpose:
- tax authority master

Examples:
- county
- city
- school district
- MUD

---

### 34. `tax_rate_histories` — P2
Purpose:
- track rate changes over time

---

### 35. `effective_tax_rates` — MVP
Purpose:
- precomputed parcel-year effective tax rate

Critical note:
- strongly recommended for quote performance

---

### 36. `tax_jurisdictions` — P3
Purpose:
- more generalized statewide tax jurisdiction support

---

### 37. `appeal_rules` — P3
Purpose:
- county-specific protest rules and workflow differences

---

### 38. `hearing_statistics` — P3
Purpose:
- county/panel/hearing-level performance patterns

---

## E. Sales / MLS / Market Tables

### 39. `sales_raw` — MVP
Purpose:
- raw sale records from any source, including restricted data

Critical note:
- never expose directly through public API

---

### 40. `parcel_sales` — MVP
Purpose:
- normalized transaction records for the valuation engine

Important fields:
- `sale_date`
- `sale_price`
- `list_price`
- `days_on_market`
- `sale_price_psf`
- `time_adjusted_price`
- `validity_code`
- `arms_length_flag`
- `restricted_flag`

---

### 41. `mls_listing_histories` — P2
Purpose:
- restricted MLS listing history

---

### 42. `agent_remarks_raw` — P3
Purpose:
- restricted raw agent remarks

---

### 43. `neighborhood_stats` — MVP
Purpose:
- precomputed neighborhood market metrics

Important fields:
- `sale_count`
- `median_sale_psf`
- `p25_sale_psf`
- `p75_sale_psf`
- `median_dom`
- `median_list_to_sale`
- `price_std_dev`

Critical note:
- this is one of the key performance tables for instant quoting

---

### 44. `market_areas` — P2
Purpose:
- broader market segmentation

---

### 45. `parcel_market_areas` — P2
Purpose:
- assign parcel to market area

---

### 46. `time_adjustment_factors` — P2
Purpose:
- store periodic market trend adjustments

---

### 47. `adjustment_factors` — P2
Purpose:
- bounded adjustment values for size, age, garage, pool, etc.

---

### 48. `neighborhood_sale_psf_curves` — P3
Purpose:
- richer PPSF trend modeling by neighborhood

---

### 49. `condition_premiums` — P3
Purpose:
- estimate adjustment behavior by condition class

---

## F. Comp Engine / Feature Store Tables

### 50. `comp_candidate_pools` — MVP
Purpose:
- header record for a parcel’s comp pool generation run

---

### 51. `comp_candidates` — MVP
Purpose:
- ranked candidate comps for both market and equity universes

Critical note:
- do not run live full-universe comp search in quote requests
- quote path should read from persisted candidates

---

### 52. `comp_adjustments` — P2
Purpose:
- comp-level adjustment lines for auditability and packet generation

---

### 53. `parcel_features` — MVP
Purpose:
- subject feature store
- one of the most important derived tables

Expected fields:
- physical attributes
- current values
- exemption flags
- neighborhood stats
- derived ratios
- YOY changes

Critical note:
- most quote and modeling logic should read from this table

---

### 54. `market_features` — P2
Purpose:
- structured market-model input features

---

### 55. `equity_features` — P2
Purpose:
- structured unequal-appraisal model features

---

### 56. `appeal_features` — P2
Purpose:
- protest and outcome-related model features

---

## G. Valuation / Quote Tables

### 57. `valuation_runs` — MVP
Purpose:
- versioned valuation outputs

Critical note:
- do not overwrite prior valuation runs
- store low / point / high ranges
- store confidence score
- store model version

---

### 58. `valuation_run_inputs` — P2
Purpose:
- audit trail of important model inputs

---

### 59. `quote_runs` — P2
Purpose:
- quote generation event log

---

### 60. `parcel_savings_estimates` — MVP
Purpose:
- savings output for parcel-year tied to a valuation run

Important fields:
- `projected_reduction_low/point/high`
- `effective_tax_rate`
- `gross_tax_savings_low/point/high`
- `success_probability`
- `expected_tax_savings_low/point/high`
- `estimated_contingency_fee`

---

### 61. `quote_explanations` — MVP
Purpose:
- structured explanation payload for quote UI/API

Recommended storage:
- JSON

---

### 62. `protest_recommendations` — MVP
Purpose:
- final recommendation result

Expected values:
- accept
- reject
- manual_review

---

### 63. `lead_scores` — P2
Purpose:
- expected economics / triage before full conversion

---

### 64. `decision_tree_results` — MVP
Purpose:
- persist every protest decision-tree rule outcome

Critical note:
- this makes the system explainable and debuggable

---

### 65. `arb_win_probability_runs` — P2
Purpose:
- second-stage protest success / reduction model outputs

---

## H. Leads / CRM / Clients Tables

### 66. `leads` — MVP
Purpose:
- pre-client lead capture

---

### 67. `lead_events` — MVP
Purpose:
- lead activity history

Examples:
- quote_viewed
- form_started
- form_submitted
- agreement_started

---

### 68. `clients` — MVP
Purpose:
- signed customer master

---

### 69. `client_parcels` — P2
Purpose:
- client-to-parcel relationships

---

### 70. `representation_agreements` — MVP
Purpose:
- signed agency/representation agreement records

---

## I. Protest Operations Tables

### 71. `protest_cases` — MVP
Purpose:
- parcel-year protest case record

Critical note:
- this becomes the central operational table after conversion

---

### 72. `case_assignments` — P2
Purpose:
- assign analyst/agent responsibility

---

### 73. `case_notes` — P2
Purpose:
- internal notes

---

### 74. `hearings` — P2
Purpose:
- hearing scheduling and stage records

---

### 75. `hearing_panels` — P3
Purpose:
- panel metadata for performance analysis

---

### 76. `hearing_participants` — P3
Purpose:
- attendance and participation tracking

---

### 77. `case_outcomes` — MVP
Purpose:
- final protest result

Critical note:
- this is one of the most important long-term moat tables
- should feed the training dataset

---

### 78. `case_status_history` — P2
Purpose:
- workflow history

---

## J. Evidence / Packet Generation Tables

### 79. `evidence_packets` — P2
Purpose:
- packet header for informal or ARB packet

---

### 80. `evidence_packet_items` — P2
Purpose:
- packet contents

---

### 81. `evidence_comp_sets` — P2
Purpose:
- selected comp group for a packet

---

### 82. `evidence_comp_set_items` — P2
Purpose:
- comp rows included in evidence packet

---

### 83. `condition_reports` — P2
Purpose:
- structured condition/obsolescence intake

Critical note:
- high-value for strong protest cases

---

### 84. `condition_report_photos` — P2
Purpose:
- condition evidence photos

---

### 85. `evidence_arguments` — P2
Purpose:
- structured protest arguments

Examples:
- market value
- unequal appraisal
- condition

---

## K. Billing / Financials Tables

### 86. `invoices` — P3
Purpose:
- invoice headers

---

### 87. `invoice_line_items` — P3
Purpose:
- invoice details

---

### 88. `payments` — P3
Purpose:
- payment records

---

### 89. `collections` — P2
Purpose:
- contingency collection tracking

---

### 90. `profitability_snapshots` — P3
Purpose:
- expected vs actual economics

---

## L. Analytics / Learning / Internal Ops Tables

### 91. `appeal_training_dataset` — P2
Purpose:
- central ML / learning dataset

Critical note:
- one of the most important long-term tables
- should include parcel, value, comp, hearing, and outcome features

---

### 92. `neighborhood_performance_stats` — P2
Purpose:
- neighborhood-level win rate and average reduction statistics

---

### 93. `model_versions` — P3
Purpose:
- model registry

---

### 94. `model_metrics` — P3
Purpose:
- model evaluation metrics

---

### 95. `marketing_attribution` — P3
Purpose:
- link lead economics to marketing source

---

### 96. `users` — P3
Purpose:
- internal platform users

---

### 97. `user_audit_logs` — P3
Purpose:
- internal audit trail

---

# 5. MVP-Critical Relationship Map

This is the minimum relational backbone Codex must keep correct.

## Core chain
`counties`
-> `appraisal_districts`
-> `parcels`
-> `parcel_assessments`
-> `parcel_improvements`
-> `parcel_features`
-> `valuation_runs`
-> `parcel_savings_estimates`
-> `protest_recommendations`

## Search chain
`parcels`
-> `parcel_addresses`

## Tax chain
`parcels`
-> `parcel_taxing_units`
-> `taxing_units`
-> `effective_tax_rates`

## Sales / quote chain
`parcels`
-> `parcel_sales`
-> `neighborhood_stats`
-> `comp_candidate_pools`
-> `comp_candidates`
-> `valuation_runs`
-> `quote_explanations`

## Lead / client / case chain
`parcels`
-> `leads`
-> `clients`
-> `representation_agreements`
-> `protest_cases`
-> `case_outcomes`

---

# 6. Recommended MVP Build Set

These are the tables that should exist first.

## Foundations
- counties
- appraisal_districts
- tax_years
- county_configs
- source_systems
- county_field_mappings
- workflow_statuses
- hearing_types
- job_runs

## Ingestion
- import_batches
- raw_files
- ingest_errors
- stg_county_property_raw
- stg_county_tax_rates_raw
- stg_sales_raw
- stg_gis_raw

## Parcel warehouse
- parcels
- parcel_addresses
- parcel_improvements
- parcel_lands
- parcel_assessments
- parcel_exemptions

## Tax / sales
- taxing_units
- parcel_taxing_units
- effective_tax_rates
- sales_raw
- parcel_sales
- neighborhood_stats

## Quote engine
- parcel_features
- comp_candidate_pools
- comp_candidates
- valuation_runs
- parcel_savings_estimates
- quote_explanations
- decision_tree_results
- protest_recommendations

## Business flow
- leads
- lead_events
- clients
- representation_agreements
- protest_cases
- case_outcomes

---

# 7. Key Read Models and Derived Outputs

Codex should plan for these read-focused outputs.

## Quote read model / view
Should expose:
- parcel identity
- notice value
- defensible value
- expected savings
- confidence
- recommendation
- explanation JSON

## Search read path
Should resolve parcels by:
- address text
- ZIP
- county
- optionally geometry proximity

---

# 8. Most Important Indexing Priorities

These are the first indexes that matter most.

## Parcels
- `(county_id, tax_year, account_number)`
- `(county_id, situs_zip, situs_address)`
- `gist(geom)`

## Assessments / improvements
- `(parcel_id, tax_year)`

## Sales
- `(parcel_id, sale_date desc)`
- `(county_id, sale_date desc)`

## Neighborhood stats
- `(county_id, tax_year, neighborhood_code, property_type_code/ property_type, period_months)`

## Comp candidates
- `(subject_parcel_id, comp_type, rank_num)`

## Valuation / savings
- `(parcel_id, created_at desc)` or `(parcel_id, updated_at desc)` as appropriate

## Protest cases
- `(parcel_id, tax_year)`

---

# 9. Schema Usage Rules

1. Public APIs must only read public-safe tables and derived outputs.
2. Raw restricted tables must never be serialized directly in public responses.
3. Historical runs must not be overwritten.
4. Decision-tree outcomes must be persisted.
5. Case outcomes must be mapped into training-ready data later.
6. County-specific parsing belongs in adapters and staging transforms, not in canonical table design.

---

# 10. Final Note for Codex

This schema reference is a navigation file, not the migration source.
Use:
- `DWELLIO_MASTER_SPEC.md` for the full architecture and formulas
- `DWELLIO_BUILD_PLAN.md` for execution order
- `AGENT_RULES.md` for non-negotiable implementation rules
- this file for quick schema lookup and table intent
