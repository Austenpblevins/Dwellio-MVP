from __future__ import annotations

import argparse
import json

from app.ingestion.historical_backfill import HistoricalBackfillOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run bounded historical backfill registration, staging, and normalize flows "
            "for Harris/Fort Bend adapter-ready county files."
        )
    )
    parser.add_argument("--counties", nargs="+", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    parser.add_argument(
        "--dataset-types",
        nargs="+",
        default=["property_roll", "tax_rates"],
    )
    parser.add_argument("--ready-root", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results = HistoricalBackfillOrchestrator().run(
        counties=args.counties,
        tax_years=args.tax_years,
        dataset_types=args.dataset_types,
        ready_root=args.ready_root,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            [
                {
                    "county_id": result.county_id,
                    "tax_year": result.tax_year,
                    "dataset_type": result.dataset_type,
                    "manifest_path": result.manifest_path,
                    "source_file_path": result.source_file_path,
                    "source_checksum": result.source_checksum,
                    "import_batch_id": result.import_batch_id,
                    "skipped_duplicate": result.skipped_duplicate,
                    "existing_status": result.existing_status,
                    "existing_publish_state": result.existing_publish_state,
                    "staging_job_run_id": (
                        None if result.staging_result is None else result.staging_result.job_run_id
                    ),
                    "normalize_job_run_id": (
                        None
                        if result.normalize_result is None
                        else result.normalize_result.job_run_id
                    ),
                    "publish_version": (
                        None
                        if result.normalize_result is None
                        else result.normalize_result.publish_version
                    ),
                }
                for result in results
            ],
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
