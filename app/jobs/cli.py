from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path

from app.jobs import (
    job_comp_candidates,
    job_features,
    job_fetch_sources,
    job_inspect_ingestion,
    job_geocode_repair,
    job_load_staging,
    job_normalize,
    job_packet_refresh,
    job_refresh_quote_cache,
    job_refresh_instant_quote,
    job_rollback_publish,
    job_run_ingestion,
    job_sales_ingestion,
    job_score_models,
    job_score_savings,
    job_set_tax_rate_adoption_status,
    job_validate_instant_quote,
)
from app.jobs.runner import execute_job
from app.services.instant_quote_tax_rate_basis import (
    TAX_RATE_ADOPTION_STATUS_SOURCES,
    TAX_RATE_BASIS_STATUSES,
)

JobCallable = Callable[..., None]

JOB_REGISTRY: dict[str, JobCallable] = {
    "job_fetch_sources": job_fetch_sources.run,
    "job_run_ingestion": job_run_ingestion.run,
    "job_inspect_ingestion": job_inspect_ingestion.run,
    "job_load_staging": job_load_staging.run,
    "job_normalize": job_normalize.run,
    "job_rollback_publish": job_rollback_publish.run,
    "job_geocode_repair": job_geocode_repair.run,
    "job_sales_ingestion": job_sales_ingestion.run,
    "job_features": job_features.run,
    "job_comp_candidates": job_comp_candidates.run,
    "job_score_models": job_score_models.run,
    "job_score_savings": job_score_savings.run,
    "job_set_tax_rate_adoption_status": job_set_tax_rate_adoption_status.run,
    "job_refresh_instant_quote": job_refresh_instant_quote.run,
    "job_validate_instant_quote": job_validate_instant_quote.run,
    "job_refresh_quote_cache": job_refresh_quote_cache.run,
    "job_packet_refresh": job_packet_refresh.run,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Dwellio job by name.")
    parser.add_argument("job_name", choices=sorted(JOB_REGISTRY.keys()))
    parser.add_argument("--county-id", default=None)
    parser.add_argument("--tax-year", default=None, type=int)
    parser.add_argument("--dataset-type", default=None)
    parser.add_argument("--import-batch-id", default=None)
    parser.add_argument("--account-number", action="append", default=None)
    parser.add_argument("--account-numbers-file", default=None)
    parser.add_argument(
        "--tax-rate-basis-status",
        default=None,
        choices=sorted(TAX_RATE_BASIS_STATUSES),
    )
    parser.add_argument("--tax-rate-basis-status-reason", default=None)
    parser.add_argument("--tax-rate-basis-status-note", default=None)
    parser.add_argument(
        "--tax-rate-basis-status-source",
        default=None,
        choices=sorted(TAX_RATE_ADOPTION_STATUS_SOURCES),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    job_callable = JOB_REGISTRY[args.job_name]
    job_kwargs = {
        "county_id": args.county_id,
        "tax_year": args.tax_year,
    }
    if args.dataset_type is not None:
        job_kwargs["dataset_type"] = args.dataset_type
    if args.import_batch_id is not None:
        job_kwargs["import_batch_id"] = args.import_batch_id
    account_numbers: list[str] = list(args.account_number or [])
    if args.account_numbers_file is not None:
        account_numbers.extend(
            account_number
            for account_number in (
                line.strip()
                for line in Path(args.account_numbers_file).read_text(encoding="utf-8").splitlines()
            )
            if account_number
        )
    if account_numbers:
        job_kwargs["account_numbers"] = account_numbers
    if args.tax_rate_basis_status is not None:
        job_kwargs["tax_rate_basis_status"] = args.tax_rate_basis_status
    if args.tax_rate_basis_status_reason is not None:
        job_kwargs["tax_rate_basis_status_reason"] = args.tax_rate_basis_status_reason
    if args.tax_rate_basis_status_note is not None:
        job_kwargs["tax_rate_basis_status_note"] = args.tax_rate_basis_status_note
    if args.tax_rate_basis_status_source is not None:
        job_kwargs["tax_rate_basis_status_source"] = args.tax_rate_basis_status_source
    if args.dry_run:
        job_kwargs["dry_run"] = True

    execute_job(
        args.job_name,
        job_callable,
        **job_kwargs,
    )


if __name__ == "__main__":
    main()
