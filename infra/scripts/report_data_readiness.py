from __future__ import annotations

import argparse
import json

from app.services.data_readiness import DataReadinessService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report county/tax-year ingestion and derived-data readiness for historical backfill planning."
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    service = DataReadinessService()
    payload = []
    for tax_year in args.tax_years:
        readiness = service.build_tax_year_readiness(
            county_id=args.county_id,
            tax_year=tax_year,
        )
        payload.append(
            {
                "county_id": readiness.county_id,
                "tax_year": readiness.tax_year,
                "tax_year_known": readiness.tax_year_known,
                "datasets": [
                    {
                        "dataset_type": item.dataset_type,
                        "source_system_code": item.source_system_code,
                        "access_method": item.access_method,
                        "availability_status": item.availability_status,
                        "availability_notes": item.availability_notes,
                        "raw_file_count": item.raw_file_count,
                        "latest_import_batch_id": item.latest_import_batch_id,
                        "latest_import_status": item.latest_import_status,
                        "latest_publish_state": item.latest_publish_state,
                        "staged": item.staged,
                        "canonical_published": item.canonical_published,
                    }
                    for item in readiness.datasets
                ],
                "derived": {
                    "parcel_summary_ready": readiness.derived.parcel_summary_ready,
                    "parcel_year_trend_ready": readiness.derived.parcel_year_trend_ready,
                    "neighborhood_stats_ready": readiness.derived.neighborhood_stats_ready,
                    "neighborhood_year_trend_ready": readiness.derived.neighborhood_year_trend_ready,
                    "search_support_ready": readiness.derived.search_support_ready,
                    "feature_ready": readiness.derived.feature_ready,
                    "comp_ready": readiness.derived.comp_ready,
                    "valuation_ready": readiness.derived.valuation_ready,
                    "savings_ready": readiness.derived.savings_ready,
                    "decision_tree_ready": readiness.derived.decision_tree_ready,
                    "explanation_ready": readiness.derived.explanation_ready,
                    "recommendation_ready": readiness.derived.recommendation_ready,
                    "quote_ready": readiness.derived.quote_ready,
                    "parcel_summary_row_count": readiness.derived.parcel_summary_row_count,
                    "parcel_year_trend_row_count": readiness.derived.parcel_year_trend_row_count,
                    "neighborhood_stats_row_count": readiness.derived.neighborhood_stats_row_count,
                    "neighborhood_year_trend_row_count": readiness.derived.neighborhood_year_trend_row_count,
                    "search_document_row_count": readiness.derived.search_document_row_count,
                    "parcel_feature_row_count": readiness.derived.parcel_feature_row_count,
                    "comp_pool_row_count": readiness.derived.comp_pool_row_count,
                    "valuation_run_row_count": readiness.derived.valuation_run_row_count,
                    "savings_row_count": readiness.derived.savings_row_count,
                    "decision_tree_row_count": readiness.derived.decision_tree_row_count,
                    "explanation_row_count": readiness.derived.explanation_row_count,
                    "recommendation_row_count": readiness.derived.recommendation_row_count,
                    "quote_row_count": readiness.derived.quote_row_count,
                },
            }
        )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
