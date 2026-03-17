# Dwellio Visual Architecture Diagram

## Should you delete the individual SQL files after downloading the single SQL file?

No.

Keep both:
- `app/db/migrations/*.sql` as the source of truth for ordered schema evolution
- `dwellio_full_schema.sql` as a convenience bundle for review, onboarding, and one-shot reference

Use the migration files for normal development.
Use the single SQL file for:
- Codex context
- architecture review
- quick schema reading
- easier local bootstrap

---

## System + Data Pipeline Diagram

```mermaid
flowchart TD
    A[County bulk files / restricted feeds] --> B[import_batches + raw_files]
    B --> C[staging tables]
    C --> D[county adapters]
    D --> E[canonical parcel warehouse]

    E --> E1[parcels]
    E --> E2[parcel_addresses]
    E --> E3[parcel_improvements]
    E --> E4[parcel_lands]
    E --> E5[parcel_assessments]
    E --> E6[effective_tax_rates]

    B --> F[sales_raw / mls_listing_histories]
    F --> G[parcel_sales]
    G --> H[neighborhood_stats]

    E --> I[parcel_features]
    G --> J[comp_candidate_pools]
    H --> J
    I --> J
    J --> K[comp_candidates]

    H --> L[market_model]
    K --> L
    I --> M[equity_model]

    L --> N[valuation_runs]
    M --> N

    N --> O[parcel_savings_estimates]
    N --> P[decision_tree_results]
    N --> Q[quote_explanations]
    N --> R[protest_recommendations]

    E --> S[v_search_read_model]
    N --> T[v_quote_read_model]
    O --> T
    Q --> T
    R --> T

    S --> U[/search API]
    T --> V[/quote API]
    T --> W[/quote explanation API]
    V --> X[lead capture]
    X --> Y[leads / clients / agreements]
    Y --> Z[protest_cases]
    Z --> AA[case_outcomes]
    AA --> AB[training / moat layer]
```

---

## Dependency Tree

### Source-of-truth docs
- `docs/source_of_truth/AGENT_RULES.md`
- `docs/source_of_truth/DWELLIO_BUILD_PLAN.md`
- `docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md`
- `docs/source_of_truth/DWELLIO_MASTER_SPEC.md`
- `docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md`

### Architecture docs
- `docs/architecture/implementation-spec.md`
- `docs/architecture/schema-reference.md`
- `docs/architecture/sales-reconstruction-engine.md`
- `docs/architecture/comp-engine.md`
- `docs/architecture/neighborhood-and-market-stats.md`
- `docs/architecture/valuation-savings-recommendation-engine.md`
- `docs/architecture/job-runner-architecture.md`
- `docs/architecture/api-contracts.md`
- `docs/architecture/frontend-page-spec.md`
- `docs/architecture/domain-scoring-formulas.md`
- `docs/architecture/testing-observability-security.md`

### Database
- `app/db/migrations/*.sql`
- `app/db/views/quote_read_model.sql`

### Models
- `app/models/common.py`
- `app/models/parcel.py`
- `app/models/assessment.py`
- `app/models/sales.py`
- `app/models/features.py`
- `app/models/quote.py`
- `app/models/case.py`

### Services
- `app/services/address_resolver.py`
- `app/services/comp_scoring.py`
- `app/services/market_model.py`
- `app/services/equity_model.py`
- `app/services/decision_tree.py`
- `app/services/arb_probability.py`
- `app/services/savings_engine.py`
- `app/services/explanation_builder.py`
- `app/services/packet_generator.py`

### ETL / jobs
- `app/jobs/runner.py`
- `app/jobs/job_fetch_sources.py`
- `app/jobs/job_load_staging.py`
- `app/jobs/job_normalize.py`
- `app/jobs/job_geocode_repair.py`
- `app/jobs/job_sales_ingestion.py`
- `app/jobs/job_features.py`
- `app/jobs/job_comp_candidates.py`
- `app/jobs/job_score_models.py`
- `app/jobs/job_score_savings.py`
- `app/jobs/job_refresh_quote_cache.py`
- `app/jobs/job_packet_refresh.py`

### API
- `app/api/search.py`
- `app/api/quote.py`
- `app/api/leads.py`
- `app/api/cases.py`
- `app/api/admin.py`

### County adapters
- `app/county_adapters/common/*`
- `app/county_adapters/harris/*`
- `app/county_adapters/fort_bend/*`

### Utilities
- `app/utils/logging.py`
- `app/utils/text_normalization.py`
- `app/utils/hashing.py`
- `app/utils/storage.py`
- `app/utils/math_utils.py`
- `app/utils/date_utils.py`

### Tests / infra
- `tests/*`
- `infra/*`

---

## Folder / File Index

```text
[missing] dwellio_final_master_repo
```
