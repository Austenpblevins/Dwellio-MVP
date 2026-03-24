from __future__ import annotations

import argparse
import json

from app.services.data_readiness import DataReadinessService
from app.services.historical_validation import HistoricalValidationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rank prior tax years for repeatable historical validation and summarize what validation surfaces are ready."
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    parser.add_argument("--current-tax-year", default=None, type=int)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    readiness_service = DataReadinessService()
    validation_service = HistoricalValidationService()
    readiness_items = [
        readiness_service.build_tax_year_readiness(
            county_id=args.county_id,
            tax_year=tax_year,
        )
        for tax_year in args.tax_years
    ]
    ranked = validation_service.rank_validation_years(
        readiness_items,
        current_tax_year=args.current_tax_year or max(args.tax_years),
    )
    payload = [
        {
            "county_id": candidate.county_id,
            "tax_year": candidate.tax_year,
            "readiness_score": candidate.readiness_score,
            "recommended_for_qa": candidate.recommended_for_qa,
            "caveats": candidate.caveats,
            "capabilities": {
                "parcel_summary_validation_ready": candidate.capabilities.parcel_summary_validation_ready,
                "parcel_trend_validation_ready": candidate.capabilities.parcel_trend_validation_ready,
                "neighborhood_stats_validation_ready": candidate.capabilities.neighborhood_stats_validation_ready,
                "neighborhood_trend_validation_ready": candidate.capabilities.neighborhood_trend_validation_ready,
                "feature_validation_ready": candidate.capabilities.feature_validation_ready,
                "comp_validation_ready": candidate.capabilities.comp_validation_ready,
                "valuation_validation_ready": candidate.capabilities.valuation_validation_ready,
                "savings_validation_ready": candidate.capabilities.savings_validation_ready,
                "decision_tree_validation_ready": candidate.capabilities.decision_tree_validation_ready,
                "explanation_validation_ready": candidate.capabilities.explanation_validation_ready,
                "recommendation_validation_ready": candidate.capabilities.recommendation_validation_ready,
                "quote_read_model_validation_ready": candidate.capabilities.quote_read_model_validation_ready,
            },
        }
        for candidate in ranked
    ]
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
