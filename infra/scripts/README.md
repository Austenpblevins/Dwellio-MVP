# Infra Scripts

- `run_migrations.py`: applies ordered SQL migrations from `app/db/migrations`.
- `run_api.sh`: starts the FastAPI service.
- `run_job.sh`: runs one ETL/job module from the Stage 0 job registry.
- `register_manual_import.py`: registers a real manually downloaded county file into `import_batches` and `raw_files` for historical backfill.
- `run_historical_backfill.py`: runs bounded historical backfill registration, staging, and normalize flows from adapter-ready county files while preserving idempotent duplicate-file reuse.
- `report_data_readiness.py`: reports county/tax-year raw, canonical, and derived readiness for QA year selection and backfill verification.
- `report_readiness_metrics.py`: reports internal/admin county-year readiness KPIs, freshness, validation regression signals, and alertable operational metrics.
- `report_quote_quality_monitor.py`: reports weekly Harris/Fort Bend quote-quality trends, leakage, denominator drift, validation freshness, and watchlists.
- `run_weekly_quote_quality_monitor.py`: scheduler-friendly wrapper for `report_quote_quality_monitor.py` that writes durable artifacts, a manifest, and sends alert payloads to `DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL` when alert conditions are present.
- `report_historical_validation.py`: ranks candidate tax years for reproducible historical QA and summarizes what validation surfaces are actually ready.
- `prepare_manual_county_files.py`: prepares manually downloaded Harris and Fort Bend county raw files into adapter-ready property-roll and tax-rate files for any supported tax year and writes per-dataset audit manifests.
- `convert_2025_real_sources.py`: compatibility wrapper around the reusable manual prep pipeline for the legacy 2025-only flow.
- `verify_ingestion_to_searchable.py`: smoke-verifies that a county-year can be traced from ingestion into admin visibility and searchable read models.
