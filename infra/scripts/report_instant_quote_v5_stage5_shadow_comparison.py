from __future__ import annotations

import argparse
import json
import os
from uuid import uuid4

from app.core.config import get_settings
from app.db.connection import get_connection
from app.services.instant_quote import InstantQuoteService
from app.services.instant_quote_tax_profile import INSTANT_QUOTE_TAX_PROFILE_VERSION

DEFAULT_COUNTIES = ("harris", "fort_bend")
DEFAULT_SAMPLE_SIZE = 100


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Stage 5 instant-quote shadow savings comparison using the current public path "
            "and the materialized Stage 4 tax profile side by side."
        )
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DWELLIO_DATABASE_URL before running the shadow comparison.",
    )
    return parser


def build_payload(*, county_ids: list[str], tax_year: int, sample_size: int) -> dict[str, object]:
    service = InstantQuoteService()
    county_payloads: list[dict[str, object]] = []
    with get_connection() as connection:
        service._prepare_request_session(connection)  # type: ignore[attr-defined]
        for county_id in county_ids:
            accounts = _sample_accounts(
                connection=connection,
                county_id=county_id,
                tax_year=tax_year,
                limit=sample_size,
            )
            comparison_rows = [
                _build_comparison_row(
                    service=service,
                    connection=connection,
                    county_id=county_id,
                    tax_year=tax_year,
                    account_number=account_number,
                )
                for account_number in accounts
            ]
            county_payloads.append(
                {
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "sample_account_count": len(comparison_rows),
                    "sample_rows": comparison_rows,
                    "comparison_summary": _comparison_summary(comparison_rows),
                    "fallback_profile_summary": _fallback_profile_summary(
                        connection=connection,
                        county_id=county_id,
                        tax_year=tax_year,
                    ),
                    "profile_status_distribution": _profile_status_distribution(
                        connection=connection,
                        county_id=county_id,
                        tax_year=tax_year,
                    ),
                    "top_shadow_delta_rows": sorted(
                        [
                            row
                            for row in comparison_rows
                            if row["shadow_savings_delta_raw"] is not None
                        ],
                        key=lambda row: abs(float(row["shadow_savings_delta_raw"])),
                        reverse=True,
                    )[:10],
                    "opportunity_only_candidates": [
                        row
                        for row in comparison_rows
                        if row["shadow_savings_estimate_raw"] is None
                        and row["shadow_tax_profile_status"] in {"opportunity_only", "unsupported"}
                        and (row["current_savings_estimate_raw"] or 0.0) > 0
                    ][:10],
                    "opportunity_only_profile_examples": _profile_status_examples(
                        service=service,
                        connection=connection,
                        county_id=county_id,
                        tax_year=tax_year,
                        tax_profile_status="opportunity_only",
                        limit=10,
                    ),
                    "high_opportunity_low_cash_rows": [
                        row
                        for row in comparison_rows
                        if row["shadow_opportunity_vs_savings_state"]
                        in {
                            "strong_opportunity_low_cash",
                            "supported_opportunity_low_cash",
                            "school_limited_non_school_possible",
                            "near_total_exemption_low_cash",
                            "total_exemption_low_cash",
                            "opportunity_only_tax_profile_incomplete",
                        }
                    ][:10],
                }
            )
    return {
        "current_public_savings_model": "reduction_estimate_times_effective_tax_rate",
        "shadow_profile_version": INSTANT_QUOTE_TAX_PROFILE_VERSION,
        "sample_size_per_county": sample_size,
        "counties": county_payloads,
    }


def _sample_accounts(
    *,
    connection: object,
    county_id: str,
    tax_year: int,
    limit: int,
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
            (county_id, tax_year, limit),
        )
        rows = cursor.fetchall()
    return [str(row["account_number"]) for row in rows]


def _build_comparison_row(
    *,
    service: InstantQuoteService,
    connection: object,
    county_id: str,
    tax_year: int,
    account_number: str,
) -> dict[str, object]:
    subject_row = service._fetch_subject_row(  # type: ignore[attr-defined]
        connection=connection,
        county_id=county_id,
        requested_tax_year=tax_year,
        account_number=account_number,
    )
    if subject_row is None:
        raise LookupError(f"Instant quote subject missing for {county_id}/{tax_year}/{account_number}.")
    response, telemetry = service._build_response(  # type: ignore[attr-defined]
        connection=connection,
        request_id=uuid4(),
        subject_row=subject_row,
        requested_tax_year=tax_year,
    )
    telemetry.update(
        service._build_shadow_savings_payload(  # type: ignore[attr-defined]
            connection=connection,
            subject_row=subject_row,
            telemetry=telemetry,
        )
    )
    return {
        "account_number": account_number,
        "parcel_id": str(subject_row["parcel_id"]),
        "supported": bool(response.supported),
        "unsupported_reason": response.unsupported_reason,
        "basis_code": response.basis_code,
        "confidence_label": telemetry.get("confidence_label"),
        "reduction_estimate_raw": telemetry.get("reduction_estimate_raw"),
        "current_savings_estimate_raw": telemetry.get("savings_estimate_raw"),
        "current_savings_estimate_display": telemetry.get("savings_estimate_display"),
        "current_estimate_bucket": telemetry.get("public_estimate_bucket"),
        "shadow_savings_estimate_raw": telemetry.get("shadow_savings_estimate_raw"),
        "shadow_savings_delta_raw": telemetry.get("shadow_savings_delta_raw"),
        "shadow_tax_profile_status": telemetry.get("shadow_tax_profile_status"),
        "shadow_tax_profile_quality_score": telemetry.get("shadow_tax_profile_quality_score"),
        "shadow_marginal_model_type": telemetry.get("shadow_marginal_model_type"),
        "shadow_marginal_tax_rate_total": telemetry.get("shadow_marginal_tax_rate_total"),
        "shadow_opportunity_vs_savings_state": telemetry.get(
            "shadow_opportunity_vs_savings_state"
        ),
        "shadow_limiting_reason_codes": telemetry.get("shadow_limiting_reason_codes") or [],
        "shadow_fallback_tax_profile_used_flag": telemetry.get(
            "shadow_fallback_tax_profile_used_flag"
        ),
        "support_blocker_code": subject_row.get("support_blocker_code"),
    }


def _profile_status_examples(
    *,
    service: InstantQuoteService,
    connection: object,
    county_id: str,
    tax_year: int,
    tax_profile_status: str,
    limit: int,
) -> list[dict[str, object]]:
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
                AND tax_profile.profile_version = %s
                AND tax_profile.tax_profile_status = %s
            ) AS ranked_accounts
            WHERE row_number = 1
            ORDER BY sort_key, account_number
            LIMIT %s
            """,
            (
                county_id,
                tax_year,
                INSTANT_QUOTE_TAX_PROFILE_VERSION,
                tax_profile_status,
                limit,
            ),
        )
        rows = cursor.fetchall()
    return [
        _build_comparison_row(
            service=service,
            connection=connection,
            county_id=county_id,
            tax_year=tax_year,
            account_number=str(row["account_number"]),
        )
        for row in rows
    ]


def _comparison_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    supported_rows = [row for row in rows if row["supported"]]
    current_zero_supported_display = [
        row
        for row in supported_rows
        if float(row["current_savings_estimate_display"] or 0.0) <= 0.0
    ]
    current_zero_supported_raw = [
        row for row in supported_rows if float(row["current_savings_estimate_raw"] or 0.0) <= 0.0
    ]
    shadow_quoteable_rows = [
        row for row in rows if row["shadow_savings_estimate_raw"] is not None
    ]
    shadow_zero_rows = [
        row for row in shadow_quoteable_rows if float(row["shadow_savings_estimate_raw"] or 0.0) <= 0.0
    ]
    opportunity_only_candidates = [
        row
        for row in rows
        if row["shadow_savings_estimate_raw"] is None
        and row["shadow_tax_profile_status"] in {"opportunity_only", "unsupported"}
        and float(row["current_savings_estimate_raw"] or 0.0) > 0.0
    ]
    high_opportunity_low_cash_rows = [
        row
        for row in rows
        if row["shadow_opportunity_vs_savings_state"]
        in {
            "strong_opportunity_low_cash",
            "supported_opportunity_low_cash",
            "school_limited_non_school_possible",
            "near_total_exemption_low_cash",
            "total_exemption_low_cash",
            "opportunity_only_tax_profile_incomplete",
        }
    ]
    delta_rows = [row for row in rows if row["shadow_savings_delta_raw"] is not None]
    public_supported_shadow_unquoteable_rows = [
        row for row in rows if row["supported"] and row["shadow_savings_estimate_raw"] is None
    ]
    public_unsupported_shadow_quoteable_rows = [
        row for row in rows if not row["supported"] and row["shadow_savings_estimate_raw"] is not None
    ]
    return {
        "supported_public_quote_count": len(supported_rows),
        "current_zero_share_supported": (
            len(current_zero_supported_display) / len(supported_rows) if supported_rows else 0.0
        ),
        "current_zero_share_supported_display": (
            len(current_zero_supported_display) / len(supported_rows) if supported_rows else 0.0
        ),
        "current_zero_share_supported_raw": (
            len(current_zero_supported_raw) / len(supported_rows) if supported_rows else 0.0
        ),
        "shadow_zero_share_quoteable": (
            len(shadow_zero_rows) / len(shadow_quoteable_rows) if shadow_quoteable_rows else 0.0
        ),
        "shadow_zero_share_quoteable_raw": (
            len(shadow_zero_rows) / len(shadow_quoteable_rows) if shadow_quoteable_rows else 0.0
        ),
        "shadow_quoteable_count": len(shadow_quoteable_rows),
        "public_supported_shadow_unquoteable_count": len(public_supported_shadow_unquoteable_rows),
        "public_unsupported_shadow_quoteable_count": len(public_unsupported_shadow_quoteable_rows),
        "opportunity_only_candidate_count": len(opportunity_only_candidates),
        "high_opportunity_low_cash_count": len(high_opportunity_low_cash_rows),
        "mean_shadow_delta_raw": (
            sum(float(row["shadow_savings_delta_raw"]) for row in delta_rows) / len(delta_rows)
            if delta_rows
            else 0.0
        ),
        "max_abs_shadow_delta_raw": (
            max(abs(float(row["shadow_savings_delta_raw"])) for row in delta_rows)
            if delta_rows
            else 0.0
        ),
    }


def _fallback_profile_summary(
    *,
    connection: object,
    county_id: str,
    tax_year: int,
) -> dict[str, object]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              COUNT(*)::integer AS profile_row_count,
              COUNT(*) FILTER (
                WHERE fallback_tax_profile_used_flag
              )::integer AS fallback_tax_profile_count,
              COUNT(*) FILTER (
                WHERE 'missing_assessment_basis' = ANY(profile_warning_codes)
              )::integer AS missing_assessment_basis_warning_count,
              COUNT(*) FILTER (
                WHERE 'school_ceiling_amount_unavailable' = ANY(profile_warning_codes)
              )::integer AS school_ceiling_amount_unavailable_count
            FROM instant_quote_tax_profile
            WHERE county_id = %s
              AND tax_year = %s
              AND profile_version = %s
            """,
            (county_id, tax_year, INSTANT_QUOTE_TAX_PROFILE_VERSION),
        )
        row = cursor.fetchone() or {}
    return {
        "profile_row_count": int(row.get("profile_row_count") or 0),
        "fallback_tax_profile_count": int(row.get("fallback_tax_profile_count") or 0),
        "missing_assessment_basis_warning_count": int(
            row.get("missing_assessment_basis_warning_count") or 0
        ),
        "school_ceiling_amount_unavailable_count": int(
            row.get("school_ceiling_amount_unavailable_count") or 0
        ),
    }


def _profile_status_distribution(
    *,
    connection: object,
    county_id: str,
    tax_year: int,
) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT tax_profile_status, COUNT(*)::integer AS count
            FROM instant_quote_tax_profile
            WHERE county_id = %s
              AND tax_year = %s
              AND profile_version = %s
            GROUP BY tax_profile_status
            ORDER BY count DESC, tax_profile_status ASC
            """,
            (county_id, tax_year, INSTANT_QUOTE_TAX_PROFILE_VERSION),
        )
        rows = cursor.fetchall()
    return {str(row["tax_profile_status"]): int(row["count"]) for row in rows}


def main() -> None:
    args = build_parser().parse_args()
    if args.database_url:
        os.environ["DWELLIO_DATABASE_URL"] = args.database_url
        get_settings.cache_clear()
    payload = build_payload(
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        sample_size=args.sample_size,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
