
# Codex Guardrails

These guardrails prevent architecture drift.

Codex must follow them when modifying code.

---

## Highest Authority Docs

Always read these before implementing code:

docs/source_of_truth/AGENT_RULES.md  
docs/source_of_truth/DWELLIO_MASTER_SPEC.md  
docs/source_of_truth/DWELLIO_BUILD_PLAN.md  
docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md  

---

## Hard Rules

### Do not break parcel-year design
All valuation outputs align to parcel_id + tax_year.

### Do not expose restricted sales data
MLS / listing data must never appear in public APIs.

### Do not run heavy comp generation in quote endpoint
Quote endpoint must rely on precomputed read models.

### Do not overwrite valuation history
valuation_runs must remain historical.

### Do not collapse architecture layers
raw → staging → canonical → derived → read models must remain intact.

### Do not change migrations order
Migrations must remain ordered and atomic.

---

## Safe Improvements

Allowed improvements:

- typing improvements
- index improvements
- better logging
- better naming
- stronger constraints

---

## Unsafe Changes (Require explanation)

- changing valuation formulas
- changing API contracts
- changing schema relationships
- collapsing architecture layers
