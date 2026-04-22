from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import sleep
from datetime import datetime, timezone
from uuid import uuid4

from app.core.config import get_settings
from app.db.connection import get_connection
from app.services.instant_quote import InstantQuoteService

DEFAULT_COUNTIES = ("harris", "fort_bend")
DEFAULT_STAGE5_ARTIFACT = Path(
    "docs/architecture/instant-quote-v5-stage5-shadow-comparison-20260422.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Stage 6 product-state rollout validation using the Stage 5 shadow artifact "
            "plus live isolated-db account lookups."
        )
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument(
        "--stage5-artifact",
        default=str(DEFAULT_STAGE5_ARTIFACT),
        help="Path to the Stage 5 handoff artifact used to seed Stage 6 cohort examples.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DWELLIO_DATABASE_URL before running the Stage 6 validation.",
    )
    return parser


def build_payload(*, county_ids: list[str], tax_year: int, stage5_artifact: Path) -> dict[str, object]:
    stage5 = json.loads(stage5_artifact.read_text())
    service = InstantQuoteService()
    county_payloads: list[dict[str, object]] = []
    with get_connection() as connection:
        service._prepare_request_session(connection)  # type: ignore[attr-defined]
        for county_id in county_ids:
            stage5_county = _stage5_county_payload(stage5=stage5, county_id=county_id)
            cohort_accounts = _collect_cohort_accounts(
                service=service,
                connection=connection,
                county_id=county_id,
                tax_year=tax_year,
                stage5_county=stage5_county,
            )
            county_payloads.append(
                {
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "target_shadow_state_counts": _target_shadow_state_counts(
                        connection=connection,
                        county_id=county_id,
                        tax_year=tax_year,
                    ),
                    "cohorts": {
                        cohort_name: [
                            _execute_quote_and_capture_rollout(
                                service=service,
                                county_id=county_id,
                                tax_year=tax_year,
                                account_number=account_number,
                            )
                            for account_number in account_numbers
                        ]
                        for cohort_name, account_numbers in cohort_accounts.items()
                    },
                }
            )
    return {
        "current_public_savings_model": "reduction_estimate_times_effective_tax_rate",
        "stage5_handoff_artifact": str(stage5_artifact),
        "counties": county_payloads,
    }


def _collect_cohort_accounts(
    *,
    service: InstantQuoteService,
    connection: object,
    county_id: str,
    tax_year: int,
    stage5_county: dict[str, object],
) -> dict[str, list[str]]:
    cohorts: dict[str, list[str]] = {
        "high_opportunity_low_cash": _scan_current_state_accounts(
            service=service,
            connection=connection,
            county_id=county_id,
            tax_year=tax_year,
            target_states={"supported_opportunity_low_cash", "strong_opportunity_low_cash"},
            limit=3,
        ),
        "total_exemption_low_cash": _accounts_from_stage5_rows(
            rows=stage5_county.get("high_opportunity_low_cash_rows") or [],
            target_state="total_exemption_low_cash",
            limit=3,
        ),
        "near_total_exemption_low_cash": _shadow_state_accounts(
            connection=connection,
            county_id=county_id,
            tax_year=tax_year,
            target_state="near_total_exemption_low_cash",
            limit=3,
        ),
        "opportunity_only_tax_profile_incomplete": _accounts_from_stage5_rows(
            rows=stage5_county.get("opportunity_only_profile_examples") or [],
            target_state="opportunity_only_tax_profile_incomplete",
            limit=3,
        ),
        "school_limited_non_school_possible": _shadow_state_accounts(
            connection=connection,
            county_id=county_id,
            tax_year=tax_year,
            target_state="school_limited_non_school_possible",
            limit=3,
        ),
        "shadow_quoteable_public_refined_review": _stage5_shadow_quoteable_refined_review_accounts(
            rows=stage5_county.get("sample_rows") or [],
            limit=3,
        ),
    }
    return cohorts


def _accounts_from_stage5_rows(
    *,
    rows: list[dict[str, object]],
    target_state: str,
    limit: int,
) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for row in rows:
        account_number = str(row.get("account_number") or "").strip()
        if (
            account_number
            and account_number not in seen
            and str(row.get("shadow_opportunity_vs_savings_state")) == target_state
        ):
            seen.add(account_number)
            selected.append(account_number)
        if len(selected) >= limit:
            break
    return selected


def _stage5_shadow_quoteable_refined_review_accounts(
    *,
    rows: list[dict[str, object]],
    limit: int,
) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for row in rows:
        account_number = str(row.get("account_number") or "").strip()
        if (
            account_number
            and account_number not in seen
            and not bool(row.get("supported"))
            and str(row.get("unsupported_reason")) == "low_confidence_refined_review"
            and row.get("shadow_savings_estimate_raw") is not None
        ):
            seen.add(account_number)
            selected.append(account_number)
        if len(selected) >= limit:
            break
    return selected


def _shadow_state_accounts(
    *,
    connection: object,
    county_id: str,
    tax_year: int,
    target_state: str,
    limit: int,
) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT account_number
            FROM (
              SELECT
                subject_cache.account_number,
                md5(subject_cache.account_number) AS sort_key,
                ROW_NUMBER() OVER (
                  PARTITION BY subject_cache.account_number
                  ORDER BY tax_profile.generated_at DESC, tax_profile.parcel_id
                ) AS row_number
              FROM instant_quote_tax_profile AS tax_profile
              JOIN instant_quote_subject_cache AS subject_cache
                ON subject_cache.parcel_id = tax_profile.parcel_id
               AND subject_cache.county_id = tax_profile.county_id
               AND subject_cache.tax_year = tax_profile.tax_year
              WHERE tax_profile.county_id = %s
                AND tax_profile.tax_year = %s
                AND tax_profile.opportunity_vs_savings_state = %s
            ) AS ranked_accounts
            WHERE row_number = 1
            ORDER BY sort_key, account_number
            LIMIT %s
            """,
            (county_id, tax_year, target_state, limit),
        )
        rows = cursor.fetchall()
    return [str(row["account_number"]) for row in rows]


def _scan_current_state_accounts(
    *,
    service: InstantQuoteService,
    connection: object,
    county_id: str,
    tax_year: int,
    target_states: set[str],
    limit: int,
    max_scan_rows: int = 4000,
) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT account_number
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
              AND support_blocker_code IS NULL
            ORDER BY md5(account_number), account_number
            LIMIT %s
            """,
            (county_id, tax_year, max_scan_rows),
        )
        rows = cursor.fetchall()

    selected: list[str] = []
    for row in rows:
        account_number = str(row["account_number"])
        subject_row = service._fetch_subject_row(  # type: ignore[attr-defined]
            connection=connection,
            county_id=county_id,
            requested_tax_year=tax_year,
            account_number=account_number,
        )
        if subject_row is None:
            continue
        _, telemetry = service._build_response(  # type: ignore[attr-defined]
            connection=connection,
            request_id=uuid4(),
            subject_row=subject_row,
            requested_tax_year=tax_year,
        )
        if telemetry.get("opportunity_vs_savings_state") in target_states:
            selected.append(account_number)
        if len(selected) >= limit:
            break
    return selected


def _target_shadow_state_counts(
    *,
    connection: object,
    county_id: str,
    tax_year: int,
) -> dict[str, int]:
    target_states = (
        "total_exemption_low_cash",
        "near_total_exemption_low_cash",
        "opportunity_only_tax_profile_incomplete",
        "school_limited_non_school_possible",
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT opportunity_vs_savings_state, COUNT(*)::integer AS count
            FROM instant_quote_tax_profile
            WHERE county_id = %s
              AND tax_year = %s
              AND opportunity_vs_savings_state = ANY(%s)
            GROUP BY opportunity_vs_savings_state
            ORDER BY opportunity_vs_savings_state
            """,
            (county_id, tax_year, list(target_states)),
        )
        rows = cursor.fetchall()
    counts = {state: 0 for state in target_states}
    for row in rows:
        counts[str(row["opportunity_vs_savings_state"])] = int(row["count"])
    return counts


def _execute_quote_and_capture_rollout(
    *,
    service: InstantQuoteService,
    county_id: str,
    tax_year: int,
    account_number: str,
) -> dict[str, object]:
    request_started_at = datetime.now(timezone.utc)
    response = service.get_quote(
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
        "account_number": account_number,
        "supported": response.supported,
        "unsupported_reason": response.unsupported_reason,
        "basis_code": response.basis_code,
        "savings_midpoint_display": estimate.savings_midpoint_display if estimate else None,
        "tax_protection_note": estimate.tax_protection_note if estimate else None,
        "summary": response.explanation.summary,
        "limitation_note": response.explanation.limitation_note,
        "bullets": response.explanation.bullets,
        "next_step_cta": response.next_step_cta,
        "public_rollout_state": request_log.get("public_rollout_state"),
        "public_rollout_reason_code": request_log.get("public_rollout_reason_code"),
        "public_rollout_applied_flag": request_log.get("public_rollout_applied_flag"),
        "internal_opportunity_vs_savings_state": request_log.get("opportunity_vs_savings_state"),
        "shadow_opportunity_vs_savings_state": request_log.get("shadow_opportunity_vs_savings_state"),
        "shadow_tax_profile_status": request_log.get("shadow_tax_profile_status"),
        "shadow_limiting_reason_codes": request_log.get("shadow_limiting_reason_codes") or [],
        "shadow_savings_estimate_raw": request_log.get("shadow_savings_estimate_raw"),
        "shadow_fallback_tax_profile_used_flag": request_log.get(
            "shadow_fallback_tax_profile_used_flag"
        ),
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
                      public_rollout_applied_flag,
                      opportunity_vs_savings_state,
                      shadow_opportunity_vs_savings_state,
                      shadow_tax_profile_status,
                      shadow_limiting_reason_codes,
                      shadow_savings_estimate_raw,
                      shadow_fallback_tax_profile_used_flag
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


def _stage5_county_payload(*, stage5: dict[str, object], county_id: str) -> dict[str, object]:
    for payload in stage5.get("counties") or []:
        candidate = payload or {}
        if str(candidate.get("county_id")) == county_id:
            return dict(candidate)
    raise LookupError(f"Stage 5 county payload not found for {county_id}.")


def main() -> None:
    args = build_parser().parse_args()
    if args.database_url:
        os.environ["DWELLIO_DATABASE_URL"] = args.database_url
        get_settings.cache_clear()
    payload = build_payload(
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        stage5_artifact=Path(args.stage5_artifact),
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
