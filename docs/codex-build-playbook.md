
# Dwellio Codex Build Playbook

This document is the final step‑by‑step guide for building the Dwellio system using Codex.
It explains exactly how to structure the repo, how to interact with Codex, and the order
in which the system should be built.

This guide assumes you already have the corrected Dwellio repository structure and
architecture documents in place.

---

# 1. Purpose of this playbook

This file acts as:

• the master guide for building Dwellio
• a Codex prompting reference
• a step‑by‑step checklist
• a build order reference
• a tool usage guide

You can keep this document open while building the system.

---

# 2. Software used during the build

You will use the following tools.

VS Code  
Primary development environment

GitHub Desktop  
Safe version control

Git  
Underlying version control engine

Node.js  
Required for development tooling and Codex CLI support

Python  
Backend language for the Dwellio system

Codex IDE extension  
Used inside VS Code to interact with the model

Codex Cloud / Codex App  
Used for larger architecture reviews and multi‑file reasoning

Supabase CLI  
Used to run database migrations and interact with the database

---

# 3. Core architecture rules

These rules must never change unless you intentionally redesign the system.

1. Python is the backend language.
2. The system is parcel‑year centric.
3. Public APIs read from precomputed read models.
4. MLS / listing data must remain restricted.
5. defensible_value = min(market_value_model_output, unequal_appraisal_model_output).
6. All valuation runs must persist results.
7. Do not create duplicate schemas or architectures.

---

# 4. Repository structure overview

The repository should resemble the following layout.

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

docs/
  source_of_truth/
  architecture/
  runbooks/
  codex/

sql/

tests/

---

# 5. Files Codex should read first

Before doing any work, Codex must read these files:

docs/source_of_truth/AGENT_RULES.md  
docs/source_of_truth/CANONICAL_CONTEXT.md  
docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md  
docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md  
docs/source_of_truth/DWELLIO_BUILD_PLAN.md  
docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md  
docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md  

docs/runbooks/ARCHITECTURE_STATE.md  
docs/runbooks/ARCHITECTURE_MAP.md  
docs/runbooks/CANONICAL_PRECEDENCE.md  

docs/codex/codex-system-context.md  
docs/codex/codex-guardrails.md  

---

# 6. Master Codex session prompt

Use this prompt at the beginning of most Codex sessions.

"""
You are working inside the Dwellio repository.

Read these files first:
docs/codex/codex-system-context.md
docs/codex/codex-guardrails.md
docs/source_of_truth/*
docs/runbooks/ARCHITECTURE_STATE.md
docs/runbooks/ARCHITECTURE_MAP.md
docs/runbooks/CANONICAL_PRECEDENCE.md

Rules:
- Python backend only
- parcel‑year centric design
- public APIs must read from read models
- restricted MLS data must not leak
- defensible_value = min(market, equity)

Do not create duplicate architectures.
Do not change architecture silently.
"""

---

# 7. Codex workflow pattern

Each phase should follow the same 3‑step pattern.

Step 1 — Audit  
Ask Codex to review files and explain issues.

Step 2 — Implement  
Ask Codex to make the changes.

Step 3 — Validate  
Ask Codex to confirm the architecture still matches the spec.

---

# 8. System build order

The safest order for building the system is:

1. Repository verification
2. Database migrations
3. Read model views
4. Pydantic models
5. Domain services
6. ETL jobs
7. API layer
8. Testing
9. Documentation sync
10. Final audit

---

# 9. Database phase

Focus folders

app/db/migrations  
app/db/views  
sql/

Tasks

Validate:

• foreign keys  
• indexes  
• migration ordering  
• schema alignment with documentation

Run database reset using:

supabase db reset

Commit after database reconciliation.

---

# 10. Models phase

Focus folder

app/models

Tasks

Ensure models match:

• database tables  
• read model views  
• API response structures

Add strict typing where possible.

---

# 11. Services phase

Focus folder

app/services

These services should exist:

address_resolver  
comp_scoring  
market_model  
equity_model  
decision_tree  
arb_probability  
savings_engine  
explanation_builder  
packet_generator

Ensure formulas match documentation.

---

# 12. ETL phase

Focus folder

app/jobs

Key jobs

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
runner.py

All ETL jobs must be idempotent.

---

# 13. API phase

Focus folder

app/api

Public endpoints

GET /search  
GET /quote/{county}/{year}/{account}  
GET /quote/{county}/{year}/{account}/explanation  
POST /lead

Internal endpoints

cases  
admin

Public APIs must read from read‑model views.

---

# 14. Testing phase

Focus folder

tests/

Priority tests

address normalization  
comp scoring  
market model  
equity model  
decision tree  
savings engine  
search endpoint  
quote endpoint

Run tests with:

pytest

---

# 15. Documentation synchronization

Review:

docs/architecture  
docs/runbooks  
docs/codex

Ensure:

• file names match repo structure  
• API routes are correct  
• schema names match migrations

Do not edit source_of_truth files unless required.

---

# 16. Final architecture audit

Ask Codex to audit the entire repository.

Look for:

• duplicate schemas  
• duplicate prompt libraries  
• conflicting documentation  
• stale API references

Implement only safe fixes.

---

# 17. Safe commit strategy

Commit after each stable phase.

Suggested commits:

Initial repository baseline  
Database migrations reconciled  
Models aligned with schema  
Services aligned with valuation logic  
ETL jobs reconciled  
API layer implemented  
Tests added  
Docs synchronized  
Final architecture audit

---

# 18. If Codex goes off track

Use this reset prompt.

"""
Stop coding.

Audit the repository against:
docs/source_of_truth/*
docs/runbooks/CANONICAL_PRECEDENCE.md

Explain:

1. current architecture
2. requested task
3. whether recent work drifted from architecture
4. safest next step

Do not edit files.
"""

---

# 19. Simplified build summary

1. verify repo
2. stabilize database
3. align models
4. implement services
5. reconcile ETL jobs
6. wire API layer
7. add tests
8. sync docs
9. run final audit

---

# 20. Final advice

Never ask Codex to build everything at once.

Build the system layer by layer and commit stable checkpoints frequently.
