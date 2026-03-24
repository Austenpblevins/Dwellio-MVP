from __future__ import annotations

import argparse
import json

from app.ingestion.manual_backfill import register_manual_import


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Register a manually downloaded county dataset into import_batches/raw_files so it can flow through the standard staging and normalize jobs."
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument("--dataset-type", required=True)
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--source-url", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = register_manual_import(
        county_id=args.county_id,
        tax_year=args.tax_year,
        dataset_type=args.dataset_type,
        source_file_path=args.source_file,
        source_url=args.source_url,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {
                "county_id": result.county_id,
                "tax_year": result.tax_year,
                "dataset_type": result.dataset_type,
                "import_batch_id": result.import_batch_id,
                "raw_file_id": result.raw_file_id,
                "storage_path": result.storage_path,
                "source_system_code": result.source_system_code,
                "file_format": result.file_format,
                "source_filename": result.source_filename,
                "checksum": result.checksum,
                "dry_run": args.dry_run,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
