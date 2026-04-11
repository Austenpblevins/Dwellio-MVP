from __future__ import annotations

import argparse
import json

from app.services.admin_readiness import AdminReadinessService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report internal county-year readiness KPIs, freshness, and alertable metrics."
    )
    parser.add_argument("--county-id", required=True)
    parser.add_argument("--tax-years", nargs="+", required=True, type=int)
    return parser


def build_payload(*, county_id: str, tax_years: list[int]) -> dict[str, object]:
    dashboard = AdminReadinessService().build_dashboard(county_id=county_id, tax_years=tax_years)
    return {
        "county_id": dashboard.county_id,
        "tax_years": dashboard.tax_years,
        "kpi_summary": dashboard.kpi_summary.model_dump(mode="json"),
        "readiness_rows": [
            {
                "county_id": row.county_id,
                "tax_year": row.tax_year,
                "overall_status": row.overall_status,
                "readiness_score": row.readiness_score,
                "blockers": row.blockers,
                "operational": row.operational.model_dump(mode="json"),
                "derived_monitoring": {
                    "instant_quote_supportable_row_rate": row.derived.instant_quote_supportable_row_rate,
                    "instant_quote_support_rate_all_sfr_flagged_denominator_count": (
                        row.derived.instant_quote_support_rate_all_sfr_flagged_denominator_count
                    ),
                    "instant_quote_support_rate_all_sfr_flagged_supportable_count": (
                        row.derived.instant_quote_support_rate_all_sfr_flagged_supportable_count
                    ),
                    "instant_quote_support_rate_all_sfr_flagged": (
                        row.derived.instant_quote_support_rate_all_sfr_flagged
                    ),
                    "instant_quote_support_rate_strict_sfr_eligible_denominator_count": (
                        row.derived.instant_quote_support_rate_strict_sfr_eligible_denominator_count
                    ),
                    "instant_quote_support_rate_strict_sfr_eligible_supportable_count": (
                        row.derived.instant_quote_support_rate_strict_sfr_eligible_supportable_count
                    ),
                    "instant_quote_support_rate_strict_sfr_eligible": (
                        row.derived.instant_quote_support_rate_strict_sfr_eligible
                    ),
                    "instant_quote_high_value_support_rate": row.derived.instant_quote_high_value_support_rate,
                    "instant_quote_special_district_heavy_support_rate": (
                        row.derived.instant_quote_special_district_heavy_support_rate
                    ),
                    "instant_quote_monitored_zero_savings_quote_share": (
                        row.derived.instant_quote_monitored_zero_savings_quote_share
                    ),
                    "instant_quote_monitored_extreme_savings_watchlist_count": (
                        row.derived.instant_quote_monitored_extreme_savings_watchlist_count
                    ),
                    "instant_quote_monitored_extreme_savings_flagged_count": (
                        row.derived.instant_quote_monitored_extreme_savings_flagged_count
                    ),
                },
                "datasets": [
                    {
                        "dataset_type": dataset.dataset_type,
                        "stage_status": dataset.stage_status,
                        "latest_import_batch_id": dataset.latest_import_batch_id,
                        "latest_import_status": dataset.latest_import_status,
                        "latest_publish_state": dataset.latest_publish_state,
                        "freshness_status": dataset.freshness_status,
                        "freshness_age_days": dataset.freshness_age_days,
                        "freshness_sla_days": dataset.freshness_sla_days,
                        "recent_failed_job_count": dataset.recent_failed_job_count,
                        "validation_error_count": dataset.validation_error_count,
                        "validation_regression": dataset.validation_regression,
                        "blockers": dataset.blockers,
                    }
                    for dataset in row.datasets
                ],
            }
            for row in dashboard.readiness_rows
        ],
    }


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(county_id=args.county_id, tax_years=args.tax_years)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
