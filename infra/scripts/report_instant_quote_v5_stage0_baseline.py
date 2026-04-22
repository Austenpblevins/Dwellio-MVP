from __future__ import annotations

import argparse
import json
import os
from typing import get_args

from app.core.config import get_settings
from app.models.quote import InstantQuoteUnsupportedReason
from app.services.instant_quote_validation import InstantQuoteValidationService

DEFAULT_COUNTIES = ("harris", "fort_bend")

DOCUMENTED_WARNING_CODES = [
    {
        "code": "prior_year_assessment_basis_fallback",
        "effect": "Adds a public disclaimer when current-year assessed basis falls back to prior year.",
    },
    {
        "code": "missing_exemption_amount",
        "effect": "Reduces confidence because exemption normalization is incomplete for the parcel-year.",
    },
    {
        "code": "homestead_flag_mismatch",
        "effect": "Signals disagreement between assessment and exemption layers and reduces confidence.",
    },
    {
        "code": "freeze_without_qualifying_exemption",
        "effect": "Can suppress public savings output through tax-limitation uncertainty handling.",
    },
    {
        "code": "assessment_exemption_total_mismatch",
        "effect": "Can suppress public savings output through tax-limitation uncertainty handling.",
    },
    {
        "code": "tax_rate_basis_fallback_applied",
        "effect": "Refresh-level warning that the selected tax-rate basis fell back off the requested year.",
    },
    {
        "code": "no_usable_tax_rate_basis",
        "effect": "Refresh-level blocker that leaves tax-rate support unavailable for a county-year.",
    },
    {
        "code": "tax_rate_basis_current_year_unofficial_or_proposed",
        "effect": "Refresh-level warning that current-year rates are not fully adopted.",
    },
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report the V5 Stage 0 instant-quote baseline using current validation surfaces."
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DWELLIO_DATABASE_URL before building the validation report.",
    )
    return parser


def build_payload(*, county_ids: list[str], tax_year: int) -> dict[str, object]:
    service = InstantQuoteValidationService()
    county_payloads: list[dict[str, object]] = []
    quote_version: str | None = None
    current_public_savings_model: str | None = None
    for county_id in county_ids:
        report = service.build_report(county_id=county_id, tax_year=tax_year)
        if quote_version is None:
            quote_version = report.quote_version
        if current_public_savings_model is None:
            current_public_savings_model = report.current_public_savings_model
        county_payloads.append(
            {
                "county_id": report.county_id,
                "tax_year": report.tax_year,
                "quote_version": report.quote_version,
                "current_public_savings_model": report.current_public_savings_model,
                "support_rate_all_sfr_flagged": report.support_rate_all_sfr_flagged,
                "support_rate_all_sfr_flagged_denominator_count": (
                    report.support_rate_all_sfr_flagged_denominator_count
                ),
                "support_rate_all_sfr_flagged_supportable_count": (
                    report.support_rate_all_sfr_flagged_supportable_count
                ),
                "support_rate_strict_sfr_eligible": report.support_rate_strict_sfr_eligible,
                "monitored_zero_savings_sample_row_count": (
                    report.monitored_zero_savings_sample_row_count
                ),
                "monitored_zero_savings_quote_count": report.monitored_zero_savings_quote_count,
                "monitored_zero_savings_supported_quote_count": (
                    report.monitored_zero_savings_supported_quote_count
                ),
                "monitored_zero_savings_quote_share": report.monitored_zero_savings_quote_share,
                "blocker_distribution": report.blocker_distribution,
                "tax_rate_basis_year": report.tax_rate_basis_year,
                "tax_rate_basis_reason": report.tax_rate_basis_reason,
                "tax_rate_basis_status": report.tax_rate_basis_status,
                "tax_rate_basis_warning_codes": report.tax_rate_basis_warning_codes,
                "denominator_shift_warning_codes": report.denominator_shift_warning_codes,
                "monitored_extreme_savings_flagged_count": (
                    report.monitored_extreme_savings_flagged_count
                ),
            }
        )
    return {
        "quote_version": quote_version,
        "current_public_savings_model": current_public_savings_model,
        "instant_quote_v5_enabled": get_settings().instant_quote_v5_enabled,
        "documented_unsupported_reasons": list(get_args(InstantQuoteUnsupportedReason)),
        "documented_warning_codes": DOCUMENTED_WARNING_CODES,
        "counties": county_payloads,
    }


def main() -> None:
    args = build_parser().parse_args()
    if args.database_url:
        os.environ["DWELLIO_DATABASE_URL"] = args.database_url
        get_settings.cache_clear()
    payload = build_payload(county_ids=args.county_ids, tax_year=args.tax_year)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
