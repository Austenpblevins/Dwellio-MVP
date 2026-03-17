
# Dwellio Architecture State

This document tracks the **current implementation state of the Dwellio system**.
It helps prevent duplicate work, architectural drift, and accidental rewrites of
completed components when using Codex or collaborating with other developers.

Codex should read this document before implementing new features.

---

# 1. Purpose of This File

This document records:

• completed system components  
• components currently in development  
• components planned but not yet implemented  

It acts as a **status ledger** for the architecture.

---

# 2. System Status Legend

Each component is assigned one of the following states:

COMPLETE  
IN_PROGRESS  
PLANNED  
NOT_STARTED

---

# 3. Repository Infrastructure

Repository Structure — COMPLETE

Core folders exist:

app/
docs/
sql/
tests/

Version Control Setup — COMPLETE

Git repository initialized
GitHub Desktop workflow configured

Codex Integration Docs — COMPLETE

codex-build-playbook.md
codex-session-start.md
codex-task-prompts.md
codex-architecture-map.md

---

# 4. Database Layer

Migration Framework — COMPLETE

Location

app/db/migrations/

Schema Definition — COMPLETE

sql/dwellio_full_schema.sql

Core Tables — COMPLETE

parcels  
parcel_improvements  
parcel_assessments

Sales Tables — COMPLETE

parcel_sales  
sales_reconstruction

Neighborhood Analytics — COMPLETE

neighborhood_stats  
market_stats

Comparable Sales — COMPLETE

comp_candidates

Valuation Runs — COMPLETE

valuation_runs

Savings Calculations — COMPLETE

parcel_savings_estimates

Decision Results — COMPLETE

decision_tree_results

Protest Tables — COMPLETE

protest_recommendations  
protest_cases  
case_outcomes

Read Models — COMPLETE

v_search_read_model  
v_quote_read_model

---

# 5. Domain Model Layer

Pydantic Models — IN_PROGRESS

Location

app/models/

Models planned

ParcelModel
QuoteModel
SavingsEstimateModel
DecisionTreeResultModel
LeadModel

---

# 6. Domain Services

Service Layer — PLANNED

Location

app/services/

Services planned

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

# 7. ETL Pipeline

ETL Framework — PLANNED

Location

app/jobs/

Jobs planned

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

Pipeline runner

runner.py

---

# 8. County Data Adapters

County Adapter Framework — PLANNED

Location

app/county_adapters/

Adapters planned

harris/
fort_bend/

Responsibilities

• ingest county appraisal roll files
• normalize tax data
• extract parcel identifiers
• parse exemption data
• load staging tables

---

# 9. API Layer

API Framework — PLANNED

Location

app/api/

Public endpoints planned

GET /search  
GET /quote/{county}/{year}/{account}  
GET /quote/{county}/{year}/{account}/explanation  
POST /lead

Internal endpoints

cases
admin

---

# 10. Testing Layer

Testing Framework — NOT_STARTED

Location

tests/

Planned test types

Unit tests

address normalization
comp scoring
market model
equity model
decision tree
savings engine

Integration tests

search endpoint
quote endpoint
lead submission

---

# 11. Documentation Layer

Source-of-Truth Docs — COMPLETE

docs/source_of_truth/

Architecture Docs — COMPLETE

docs/architecture/

Runbooks — COMPLETE

docs/runbooks/

Codex Control Docs — COMPLETE

docs/

codex-build-playbook.md
codex-session-start.md
codex-task-prompts.md
codex-architecture-map.md

---

# 12. Key System Constraints

These constraints must remain true.

1. Python-first backend architecture
2. Parcel-year centric data model
3. APIs read only from read-model views
4. MLS data must remain restricted
5. ETL jobs populate core tables
6. Services compute valuation outputs

---

# 13. How Codex Should Use This File

Before implementing a task, Codex should:

1. read this document
2. verify component status
3. avoid rebuilding completed components
4. only implement components marked PLANNED or IN_PROGRESS

---

# 14. Maintenance Instructions

Whenever a component is implemented:

Update its status in this document.

Example

Domain Services — COMPLETE
