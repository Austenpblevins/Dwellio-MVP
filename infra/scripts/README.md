# Infra Scripts

- `run_migrations.py`: applies ordered SQL migrations from `app/db/migrations`.
- `run_api.sh`: starts the FastAPI service.
- `run_job.sh`: runs one ETL/job module from the Stage 0 job registry.
- `register_manual_import.py`: registers a real manually downloaded county file into `import_batches` and `raw_files` for historical backfill.
- `report_data_readiness.py`: reports county/tax-year raw, canonical, and derived readiness for QA year selection and backfill verification.
