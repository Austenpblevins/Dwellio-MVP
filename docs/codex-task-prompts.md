
# Codex Task Prompt Library

This document contains **structured prompts for each major Dwellio subsystem**.

Use these prompts when instructing Codex to work on specific parts of the system.

---

# 1. Repository Audit Prompt

```
Inspect the entire repository and summarize:

1. system architecture layers
2. current build status for:
   - database
   - models
   - services
   - ETL jobs
   - APIs
   - tests
3. stale or duplicate files
4. architecture inconsistencies
5. the safest next implementation step

Do not modify files yet.
```

---

# 2. Database Migration Audit

```
Focus only on:

app/db/migrations/
app/db/views/
sql/dwellio_full_schema.sql

Tasks:

• validate migration ordering
• validate foreign keys
• confirm indexes
• check read-model compatibility
• detect schema drift against architecture docs

Return:

1. mismatches
2. recommended fixes
3. affected files

Do not edit files yet.
```

---

# 3. Model Reconciliation Prompt

```
Compare:

app/models/
app/db/migrations/
app/db/views/

Find:

• missing model fields
• stale fields
• mismatched field names
• API response models that do not match read models

Return recommended changes before editing.
```

---

# 4. Domain Service Audit

```
Review:

app/services/

Check alignment with:

docs/architecture/domain-scoring-formulas.md
docs/architecture/valuation-savings-recommendation-engine.md

Verify the following services:

address_resolver
comp_scoring
market_model
equity_model
decision_tree
arb_probability
savings_engine
explanation_builder
packet_generator

Return inconsistencies and safe improvement suggestions.
Do not modify code yet.
```

---

# 5. ETL Pipeline Audit

```
Review:

app/jobs/
app/county_adapters/

Ensure ETL pipeline produces the following tables correctly:

parcel_sales
neighborhood_stats
comp_candidates
valuation_runs
parcel_savings_estimates
decision_tree_results

Return:

• job dependencies
• missing steps
• schema mismatches
```

---

# 6. API Implementation Prompt

```
Focus on:

app/api/

Implement or validate endpoints:

GET /search
GET /quote/{county}/{year}/{account}
GET /quote/{county}/{year}/{account}/explanation
POST /lead

Rules:

• public endpoints read from read models
• do not expose restricted MLS data
• maintain parcel-year context
```

---

# 7. Testing Prompt

```
Create unit tests for:

• address normalization
• comp scoring
• market model
• equity model
• decision tree
• savings engine

Create integration tests for:

• search endpoint
• quote endpoint
• lead submission
```

---

# 8. Documentation Sync Prompt

```
Compare documentation with code.

Focus on:

docs/architecture/
docs/runbooks/
docs/codex/

Find:

• outdated terminology
• outdated API routes
• repo structure mismatches
```

---

# 9. Final Repository Audit

```
Perform a full repository reconciliation audit.

Check:

docs/
app/
sql/
tests/

Return:

• architecture mismatches
• duplicate files
• final safe improvements
```
