from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from time import sleep

from app.core.config import get_settings
from app.db.connection import get_connection
from app.services.instant_quote import InstantQuoteService

DEFAULT_COUNTIES = ("harris", "fort_bend")
DEFAULT_STAGE6_ARTIFACT = Path(
    "docs/architecture/instant-quote-v5-stage6-product-state-rollout-20260422.json"
)
DEFAULT_ROLLOUT_COUNTIES = "fort_bend"
DEFAULT_ROLLOUT_STATES = "total_exemption_low_cash,near_total_exemption_low_cash"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Stage 7 flagged savings rollout validation by comparing flag-off and flag-on "
            "responses for Stage 6 cohort accounts."
        )
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument(
        "--stage6-artifact",
        default=str(DEFAULT_STAGE6_ARTIFACT),
        help="Path to the Stage 6 handoff artifact used to seed Stage 7 cohort validation.",
    )
    parser.add_argument(
        "--rollout-county-ids",
        default=DEFAULT_ROLLOUT_COUNTIES,
        help="Comma-separated county ids enabled for Stage 7 translated savings rollout.",
    )
    parser.add_argument(
        "--rollout-states",
        default=DEFAULT_ROLLOUT_STATES,
        help="Comma-separated public rollout states eligible for Stage 7 translated savings rollout.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DWELLIO_DATABASE_URL before running the Stage 7 validation.",
    )
    return parser


def build_payload(
    *,
    county_ids: list[str],
    tax_year: int,
    stage6_artifact: Path,
    rollout_county_ids: str,
    rollout_states: str,
) -> dict[str, object]:
    stage6 = json.loads(stage6_artifact.read_text())
    county_payloads: list[dict[str, object]] = []
    target_cohorts = (
        "high_opportunity_low_cash",
        "total_exemption_low_cash",
        "opportunity_only_tax_profile_incomplete",
        "school_limited_non_school_possible",
        "shadow_quoteable_public_refined_review",
    )

    for county_id in county_ids:
        stage6_county = _stage6_county_payload(stage6=stage6, county_id=county_id)
        cohorts: dict[str, list[str]] = {}
        for cohort_name in target_cohorts:
            rows = stage6_county.get("cohorts", {}).get(cohort_name) or []
            cohorts[cohort_name] = [
                str(row.get("account_number"))
                for row in rows[:3]
                if str(row.get("account_number") or "").strip()
            ]
        if county_id == "harris":
            cohorts["missing_assessment_basis_control"] = ["1372390030007"]
        county_payloads.append(
            {
                "county_id": county_id,
                "tax_year": tax_year,
                "cohorts": {
                    cohort_name: _compare_accounts_for_cohort(
                        county_id=county_id,
                        tax_year=tax_year,
                        account_numbers=account_numbers,
                        rollout_county_ids=rollout_county_ids,
                        rollout_states=rollout_states,
                    )
                    for cohort_name, account_numbers in cohorts.items()
                },
            }
        )
    return {
        "current_public_savings_model": "reduction_estimate_times_effective_tax_rate",
        "stage6_handoff_artifact": str(stage6_artifact),
        "rollout_configuration": {
            "enabled": True,
            "county_ids": rollout_county_ids,
            "rollout_states": rollout_states,
        },
        "counties": county_payloads,
    }


def _compare_accounts_for_cohort(
    *,
    county_id: str,
    tax_year: int,
    account_numbers: list[str],
    rollout_county_ids: str,
    rollout_states: str,
) -> list[dict[str, object]]:
    return [
        _compare_single_account(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
            rollout_county_ids=rollout_county_ids,
            rollout_states=rollout_states,
        )
        for account_number in account_numbers
    ]


def _compare_single_account(
    *,
    county_id: str,
    tax_year: int,
    account_number: str,
    rollout_county_ids: str,
    rollout_states: str,
) -> dict[str, object]:
    current_path = _execute_quote(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
        savings_translation_enabled=False,
        rollout_county_ids=rollout_county_ids,
        rollout_states=rollout_states,
    )
    flagged_path = _execute_quote(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
        savings_translation_enabled=True,
        rollout_county_ids=rollout_county_ids,
        rollout_states=rollout_states,
    )
    current_midpoint = current_path["savings_midpoint_display"]
    flagged_midpoint = flagged_path["savings_midpoint_display"]
    midpoint_delta = (
        None
        if current_midpoint is None or flagged_midpoint is None
        else float(flagged_midpoint) - float(current_midpoint)
    )
    return {
        "account_number": account_number,
        "current_path": current_path,
        "flagged_path": flagged_path,
        "midpoint_delta": midpoint_delta,
        "translation_changed_public_estimate": current_midpoint != flagged_midpoint,
    }


def _execute_quote(
    *,
    county_id: str,
    tax_year: int,
    account_number: str,
    savings_translation_enabled: bool,
    rollout_county_ids: str,
    rollout_states: str,
) -> dict[str, object]:
    os.environ["DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED"] = (
        "true" if savings_translation_enabled else "false"
    )
    os.environ["DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS"] = rollout_county_ids
    os.environ["DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES"] = rollout_states
    get_settings.cache_clear()

    request_started_at = datetime.now(timezone.utc)
    response = InstantQuoteService().get_quote(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
    )
    request_log = _latest_request_log_row(
        county_id=county_id,
        tax_year=response.tax_year,
        account_number=account_number,
        not_before=request_started_at,
    )
    estimate = response.estimate
    return {
        "supported": response.supported,
        "unsupported_reason": response.unsupported_reason,
        "basis_code": response.basis_code,
        "savings_midpoint_display": estimate.savings_midpoint_display if estimate else None,
        "savings_range_low": estimate.savings_range_low if estimate else None,
        "savings_range_high": estimate.savings_range_high if estimate else None,
        "tax_protection_note": estimate.tax_protection_note if estimate else None,
        "summary": response.explanation.summary,
        "limitation_note": response.explanation.limitation_note,
        "next_step_cta": response.next_step_cta,
        "public_rollout_state": request_log.get("public_rollout_state"),
        "public_rollout_reason_code": request_log.get("public_rollout_reason_code"),
        "savings_translation_mode": request_log.get("savings_translation_mode"),
        "savings_translation_reason_code": request_log.get("savings_translation_reason_code"),
        "savings_translation_applied_flag": request_log.get("savings_translation_applied_flag"),
        "selected_public_savings_estimate_raw": request_log.get(
            "selected_public_savings_estimate_raw"
        ),
        "shadow_savings_estimate_raw": request_log.get("shadow_savings_estimate_raw"),
        "shadow_opportunity_vs_savings_state": request_log.get(
            "shadow_opportunity_vs_savings_state"
        ),
        "shadow_tax_profile_status": request_log.get("shadow_tax_profile_status"),
        "shadow_limiting_reason_codes": request_log.get("shadow_limiting_reason_codes") or [],
    }


def _latest_request_log_row(
    *,
    county_id: str,
    tax_year: int,
    account_number: str,
    not_before: datetime,
) -> dict[str, object]:
    for _attempt in range(20):
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      public_rollout_state,
                      public_rollout_reason_code,
                      savings_translation_mode,
                      savings_translation_reason_code,
                      savings_translation_applied_flag,
                      selected_public_savings_estimate_raw,
                      shadow_savings_estimate_raw,
                      shadow_opportunity_vs_savings_state,
                      shadow_tax_profile_status,
                      shadow_limiting_reason_codes
                    FROM instant_quote_request_logs
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND account_number = %s
                      AND created_at >= %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (county_id, tax_year, account_number, not_before),
                )
                row = cursor.fetchone()
        if row is not None:
            return dict(row)
        sleep(0.1)
    return {}


def _stage6_county_payload(*, stage6: dict[str, object], county_id: str) -> dict[str, object]:
    for payload in stage6.get("counties") or []:
        candidate = payload or {}
        if str(candidate.get("county_id")) == county_id:
            return dict(candidate)
    raise LookupError(f"Stage 6 county payload not found for {county_id}.")


def main() -> None:
    args = build_parser().parse_args()
    if args.database_url:
        os.environ["DWELLIO_DATABASE_URL"] = args.database_url
    get_settings.cache_clear()
    payload = build_payload(
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        stage6_artifact=Path(args.stage6_artifact),
        rollout_county_ids=args.rollout_county_ids,
        rollout_states=args.rollout_states,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
