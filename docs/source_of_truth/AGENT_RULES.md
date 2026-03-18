# AGENT_RULES.md

## Mission
Build Dwellio as a production-oriented Texas property tax protest platform. Do not optimize for appearance over correctness. Do not turn the project into a toy quote calculator.

## Source of Truth
- DWELLIO_MASTER_SPEC.md is the compatibility entry point for the canonical source-of-truth set.
- Treat it as a pointer to CANONICAL_CONTEXT.md, PLATFORM_IMPLEMENTATION_SPEC.md, QUOTE_ENGINE_PRODUCT_SPEC.md, DWELLIO_BUILD_PLAN.md, DWELLIO_SCHEMA_REFERENCE.md, AGENT_RULES.md, and DWELLIO_CODEX_CONTEXT.md.
- DWELLIO_BUILD_PLAN.md is the required execution order.
- If code conflicts with the spec, update the code to match the spec unless a later explicit decision document overrides it.

## Non-Negotiable Architecture Rules
1. Use layered data flow:
   raw -> staging -> canonical -> derived/read models

2. Keep county adapters separate.
   - no universal parser for all counties
   - Harris and Fort Bend must remain isolated implementations under a shared interface

3. Keep restricted MLS data separate from public-safe outputs.
   - never expose raw MLS records in public APIs
   - never expose agent remarks in public APIs
   - enforce restricted/public-safe separation in schema and service code

4. Preserve valuation history.
   - do not overwrite prior valuation runs
   - use versioned valuation output tables

5. Persist rule outcomes.
   - store decision-tree results
   - store recommendation outputs
   - store quote explanations as structured JSON

6. Public quote path must be fast.
   - do not run heavy comp analysis in the public request path
   - rely on precomputed features, comp candidates, valuation runs, and savings estimates

7. The platform is parcel-year centric.
   - schema and services must treat parcel + tax_year as a primary business identity

8. The core protest value is:
   - defensible_value = min(market_value_model_output, unequal_appraisal_model_output)

## Implementation Rules
1. Favor explicit, typed, testable code.
2. Use Pydantic models for request and response schemas.
3. Use structured logging.
4. ETL jobs must be idempotent.
5. Write integration tests for search, quote, lead creation, and case creation.
6. Document assumptions in code comments only where necessary.
7. Keep migration files ordered and atomic.
8. Seed only minimal development data.
9. Do not hardcode county-specific business rules outside county configs or county adapters unless unavoidable.
10. Do not skip indexes for high-frequency lookup paths.

## Scope Discipline
1. MVP supports:
   - Harris
   - Fort Bend
   - SFR only

2. Do not implement statewide county support before Harris and Fort Bend are stable.
3. Do not implement commercial logic in MVP.
4. Do not introduce black-box ML before transparent hybrid models work.
5. Do not overbuild packet generation before quote and case flows work.

## Required Deliverables
Every major implementation phase should include:
- code
- migrations
- tests
- docs
- clear file organization

## When Blocked
If a source-specific parser detail is unavailable:
- scaffold the interface
- implement the ingestion contract
- leave a clear TODO with expected input/output shape
- do not invent unreliable fake parsing logic and present it as complete

## Success Standard
A milestone is only complete when:
- code compiles
- migrations apply
- tests pass
- docs are updated
- the result matches the canonical source-of-truth set referenced by DWELLIO_MASTER_SPEC.md
