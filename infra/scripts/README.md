# Infra Scripts

- `run_migrations.py`: applies ordered SQL migrations from `app/db/migrations`.
- `run_api.sh`: starts the FastAPI service.
- `run_job.sh`: runs one ETL/job module from the Stage 0 job registry.
- `register_manual_import.py`: registers a real manually downloaded county file into `import_batches` and `raw_files` for historical backfill.
- `run_historical_backfill.py`: runs bounded historical backfill registration, staging, and normalize flows from adapter-ready county files while preserving idempotent duplicate-file reuse.
- `report_data_readiness.py`: reports county/tax-year raw, canonical, and derived readiness for QA year selection and backfill verification.
- `report_readiness_metrics.py`: reports internal/admin county-year readiness KPIs, freshness, validation regression signals, and alertable operational metrics.
- `report_historical_validation.py`: ranks candidate tax years for reproducible historical QA and summarizes what validation surfaces are actually ready.
- `convert_2025_real_sources.py`: converts the real 2025 Harris and Fort Bend county export files into the adapter-ready local files PR1 expects for historical/live-source pilot validation.
- `verify_ingestion_to_searchable.py`: smoke-verifies that a county-year can be traced from ingestion into admin visibility and searchable read models.
