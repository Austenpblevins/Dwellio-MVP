# DWELLIO_BUILD_PLAN.md

## Purpose

This file defines the exact execution order for Codex. Execute tasks in sequence. Do not skip ahead unless an earlier phase is blocked for a documented reason.

## Phase 0 — Planning lock
Deliverables:
- confirm DWELLIO_MASTER_SPEC.md is present as the compatibility map to the canonical source-of-truth set
- confirm AGENT_RULES.md is present
- create repo scaffold
- create roadmap and task tracking markdown inside repo

Acceptance:
- repository structure exists
- docs folder exists
- migrations folder exists
- tests folder exists

## Phase 1 — Foundation migrations
Tasks:
1. create migrations for extensions
2. create migrations for counties
3. create migrations for appraisal_districts
4. create migrations for tax_years
5. create migrations for county_configs
6. create migrations for source_systems
7. create migrations for county_field_mappings
8. create migrations for workflow_statuses
9. create migrations for hearing_types
10. create migrations for job_runs

Acceptance:
- migrations apply cleanly in Supabase
- Harris and Fort Bend seed reference rows exist
- 2026 tax year exists

## Phase 2 — Ingestion layer
Tasks:
1. create import_batches
2. create raw_files
3. create ingest_errors
4. create generic staging tables
5. create county adapter base classes
6. create Harris adapter scaffold
7. create Fort Bend adapter scaffold
8. create import logging utilities

Acceptance:
- import batches can be created
- raw file metadata can be stored
- staging rows can be inserted
- county adapter interface is consistent

## Phase 3 — Parcel warehouse
Tasks:
1. create parcels
2. create parcel_aliases
3. create parcel_addresses
4. create parcel_geometries
5. create parcel_ownership_history
6. create parcel_improvements
7. create parcel_lands
8. create parcel_amenities
9. create parcel_assessments
10. create parcel_exemptions
11. create parcel_photos

Acceptance:
- a manual parcel insert works
- annual assessment insert works
- address lookup fields exist
- geometry field exists

## Phase 4 — Tax and sales layer
Tasks:
1. create taxing_units
2. create tax_rate_histories
3. create parcel_taxing_units
4. create effective_tax_rates
5. create sales_raw
6. create parcel_sales
7. create mls_listing_histories
8. create neighborhood_stats
9. create market_areas
10. create parcel_market_areas
11. create time_adjustment_factors
12. create adjustment_factors

Acceptance:
- parcel can be assigned taxing units
- effective tax rate can be computed
- sales can be inserted
- neighborhood stats can be queried

## Phase 5 — Feature and comp layer
Tasks:
1. create parcel_features
2. create comp_candidate_pools
3. create comp_candidates
4. create comp_adjustments
5. create market_features
6. create equity_features
7. create appeal_features
8. implement feature engineering job
9. implement comp candidate generation job

Acceptance:
- parcel_features row can be built for a parcel-year
- comp pools can be generated
- comp rankings persist

## Phase 6 — Valuation and savings engine
Tasks:
1. create valuation_runs
2. create valuation_run_inputs
3. create quote_runs
4. create parcel_savings_estimates
5. create quote_explanations
6. create protest_recommendations
7. create lead_scores
8. create decision_tree_results
9. create arb_win_probability_runs
10. implement market model service
11. implement equity model service
12. implement decision tree service
13. implement savings engine service
14. implement explanation builder
15. create quote read model/view

Acceptance:
- parcel-year valuation run can be created
- savings estimate can be created
- recommendation can be created
- quote view can return a full quote payload

## Phase 7 — Public APIs
Tasks:
1. implement `/search`
2. implement `/quote/{county_id}/{tax_year}/{account_number}`
3. implement `/quote/{county_id}/{tax_year}/{account_number}/explanation`
4. implement `/lead`
5. add typed request/response schemas
6. add integration tests

Acceptance:
- search returns candidate parcels
- quote endpoint returns complete payload
- lead endpoint stores lead successfully
- tests pass

## Phase 8 — Leads, clients, and case operations
Tasks:
1. create leads
2. create lead_events
3. create clients
4. create client_parcels
5. create representation_agreements
6. create protest_cases
7. create case_assignments
8. create case_notes
9. create hearings
10. create hearing_panels
11. create hearing_participants
12. create case_outcomes
13. create case_status_history
14. implement internal APIs for case lifecycle

Acceptance:
- lead can convert to client
- agreement can be stored
- protest case can be created
- outcome can be stored

## Phase 9 — Evidence and packet generation
Tasks:
1. create evidence_packets
2. create evidence_packet_items
3. create evidence_comp_sets
4. create evidence_comp_set_items
5. create condition_reports
6. create condition_report_photos
7. create evidence_arguments
8. implement packet generator scaffolding

Acceptance:
- packet header can be created
- comp sets can be attached
- condition report can be stored

## Phase 10 — Billing and collections
Tasks:
1. create invoices
2. create invoice_line_items
3. create payments
4. create collections
5. create profitability_snapshots

Acceptance:
- invoice can be created for case
- payment can be stored
- collection status can be tracked

## Phase 11 — Analytics and moat layer
Tasks:
1. create appeal_training_dataset
2. create neighborhood_performance_stats
3. create model_versions
4. create model_metrics
5. create marketing_attribution
6. create users
7. create user_audit_logs
8. build outcome-to-training export job

Acceptance:
- closed case can produce training row
- neighborhood performance can be aggregated
- model version metadata can be stored

## Phase 12 — Harris and Fort Bend ETL implementation
Tasks:
1. implement Harris fetch
2. implement Harris parse skeletons
3. implement Harris normalize
4. implement Fort Bend fetch
5. implement Fort Bend parse skeletons
6. implement Fort Bend normalize
7. implement object storage integration
8. implement batch logging
9. implement idempotency checks

Acceptance:
- Harris raw file can be registered
- Fort Bend raw file can be registered
- staging load works
- canonical rows are created

## Phase 13 — Nightly jobs
Tasks:
1. job_fetch_sources
2. job_load_staging
3. job_normalize
4. job_geocode_repair
5. job_sales_ingestion
6. job_features
7. job_comp_candidates
8. job_score_models
9. job_score_savings
10. job_refresh_quote_cache
11. job_packet_refresh

Acceptance:
- jobs run in order
- each writes job_runs rows
- failures are logged cleanly
- reruns are safe

## Phase 14 — Deployment hardening
Tasks:
1. environment config
2. Supabase deployment docs
3. local development docs
4. test runner docs
5. seed scripts
6. basic logging/monitoring docs

Acceptance:
- developer can follow docs and run locally
- developer can apply migrations to Supabase
- integration tests run

## Phase 15 — Milestone verification
Milestone is complete when:
- Harris and Fort Bend parcel data can be ingested
- address search returns parcel candidates
- quote output returns defensible value, savings range, confidence, and recommendation
- lead can be created
- signed agreement can create protest case
- case outcome can be stored
- nightly ETL is observable and idempotent
- restricted MLS data remains segregated
