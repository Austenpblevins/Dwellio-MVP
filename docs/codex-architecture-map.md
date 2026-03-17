
# Dwellio Codex Architecture Map

This document provides a **high-level architecture map of the Dwellio system**.
It helps Codex understand how the system components connect and ensures that
future development remains consistent with the intended design.

This file is especially useful for:
- Codex repository audits
- architecture reconciliation
- onboarding new developers
- debugging pipeline dependencies

---

# 1. System Overview

Dwellio is a **property tax protest automation platform** designed to:

1. Search properties
2. Estimate defensible protest values
3. Estimate tax savings
4. Determine protest viability
5. Generate protest packets
6. Track protest outcomes

The system is **parcel-year centric**, meaning every dataset references a parcel
for a specific tax year.

---

# 2. System Layers

The system consists of six primary layers.

User Interface Layer  
API Layer  
Domain Service Layer  
ETL / Data Pipeline Layer  
Database Layer  
External Data Sources

---

# 3. High-Level System Diagram

External Data Sources
    |
    v
County Adapters (app/county_adapters)
    |
    v
ETL Jobs (app/jobs)
    |
    v
Database Tables (app/db/migrations)
    |
    v
Domain Services (app/services)
    |
    v
Read Models (app/db/views)
    |
    v
API Layer (app/api)
    |
    v
Frontend / Clients

---

# 4. External Data Sources

These systems supply data to Dwellio.

County Appraisal District data
Real property roll files
Tax rate files
Deed records
Synthetic or MLS-derived sales datasets
Geocoding services

---

# 5. ETL Pipeline Architecture

The nightly ETL pipeline runs in the following order.

job_fetch_sources.py
job_load_staging.py
job_normalize.py
job_geocode_repair.py
job_sales_ingestion.py
job_features.py
job_comp_candidates.py
job_score_models.py
job_score_savings.py
job_refresh_quote_cache.py
job_packet_refresh.py

These jobs should be **idempotent**.

---

# 6. Core Database Tables

Key table families.

Parcel tables

parcels
parcel_improvements
parcel_assessments

Sales tables

parcel_sales
sales_reconstruction

Neighborhood analytics

neighborhood_stats
market_stats

Comparable sales

comp_candidates

Valuation runs

valuation_runs

Savings calculations

parcel_savings_estimates

Decision outputs

decision_tree_results

Protest operations

protest_recommendations
protest_cases
case_outcomes

---

# 7. Read Model Views

The system exposes precomputed views to the API.

v_search_read_model

Provides property search results.

v_quote_read_model

Provides full quote output including:

estimated value
estimated savings
confidence score
explanation summary

These views allow the API to respond quickly without recomputing valuations.

---

# 8. Domain Service Layer

Domain services perform valuation calculations.

address_resolver
comp_scoring
market_model
equity_model
decision_tree
arb_probability
savings_engine
explanation_builder
packet_generator

---

# 9. API Layer

Public endpoints

GET /search
GET /quote/{county}/{year}/{account}
GET /quote/{county}/{year}/{account}/explanation
POST /lead

Internal endpoints

cases
admin

---

# 10. Repository Dependency Tree

docs/
  source_of_truth/
  architecture/
  runbooks/
  codex/

app/
  api/
  services/
  models/
  jobs/
  db/
    migrations/
    views/
  county_adapters/
  utils/

sql/

tests/

---

# 11. Data Flow Summary

The data lifecycle follows this pattern.

External Data Sources
    -> county adapters
    -> ETL ingestion
    -> normalized database tables
    -> valuation models
    -> read model views
    -> API responses

---

# 12. Key Architecture Constraints

The following constraints must remain true.

1. All valuation logic occurs in the **service layer**.
2. APIs must read from **read models**.
3. ETL pipelines write to **core tables**.
4. No direct external data access from APIs.
5. Restricted sales data must never be exposed publicly.

---

# 13. Future Scaling Considerations

The architecture is designed to support:

multi-county expansion
additional valuation models
machine learning scoring
parallel ETL pipelines
batch protest generation

---

# 14. Using This File With Codex

When performing architecture work, instruct Codex:

"""
Read docs/codex-architecture-map.md before making changes.
Ensure all changes remain consistent with the system diagram
and dependency tree defined in this document.
"""
