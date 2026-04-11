from __future__ import annotations

import argparse
import csv
import json
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.county_adapters.common.config_loader import load_county_adapter_config
from app.core.config import get_settings
from app.db.connection import get_connection

DEFAULT_COUNTIES = ("harris", "fort_bend")
DEFAULT_JSON_OUTPUT = Path("/tmp/stage19_weekly_quote_quality_monitor.json")
DEFAULT_MARKDOWN_OUTPUT = Path("/tmp/stage19_weekly_quote_quality_monitor.md")
DEFAULT_ZERO_WATCHLIST_OUTPUT = Path("/tmp/stage19_refresh_watchlist_zero_savings.csv")
DEFAULT_OUTLIER_WATCHLIST_OUTPUT = Path("/tmp/stage19_refresh_watchlist_top_outliers.csv")
DEFAULT_WATCHLIST_SUMMARY_OUTPUT = Path("/tmp/stage19_refresh_watchlist_summary.md")


def pct_change(*, current: int, prior: int | None) -> float | None:
    if prior is None or prior <= 0:
        return None
    return float(current - prior) / float(prior)


def denominator_shift_status(
    *,
    current: int,
    prior: int | None,
    threshold_pct: float,
) -> dict[str, Any]:
    change = pct_change(current=current, prior=prior)
    if change is None:
        return {
            "status": "no_prior_run",
            "triggered": False,
            "threshold_pct": threshold_pct,
            "current": current,
            "prior": prior,
            "pct_change": None,
            "abs_pct_change": None,
        }
    abs_change = abs(change)
    triggered = abs_change > threshold_pct
    return {
        "status": "threshold_exceeded" if triggered else "within_threshold",
        "triggered": triggered,
        "threshold_pct": threshold_pct,
        "current": current,
        "prior": prior,
        "pct_change": change,
        "abs_pct_change": abs_change,
    }


def divergence_drill_result(
    *,
    all_sfr_total: int,
    all_sfr_supported: int,
    strict_sfr_total: int,
    strict_sfr_supported: int,
) -> dict[str, Any]:
    all_rate = all_sfr_supported / all_sfr_total if all_sfr_total else 0.0
    strict_rate = strict_sfr_supported / strict_sfr_total if strict_sfr_total else 0.0
    return {
        "environment": "non_prod_only",
        "all_sfr_total": all_sfr_total,
        "all_sfr_supported": all_sfr_supported,
        "support_rate_all_sfr_flagged": all_rate,
        "strict_sfr_total": strict_sfr_total,
        "strict_sfr_supported": strict_sfr_supported,
        "support_rate_strict_sfr_eligible": strict_rate,
        "diverged": all_rate != strict_rate,
        "pass": all_rate != strict_rate,
    }


def select_watchlist_rows(
    rows: list[dict[str, Any]],
    *,
    kind: str,
    limit_per_county: int,
) -> list[dict[str, Any]]:
    if kind == "zero_savings":
        scoped = [
            row
            for row in rows
            if bool(row.get("supported"))
            and float(row.get("projected_savings_display") or 0.0) <= 0.0
        ]
        key = lambda row: (
            str(row.get("county_id") or ""),
            str(row.get("account_hash") or ""),
            str(row.get("account_number") or ""),
        )
    elif kind == "top_outliers":
        scoped = [row for row in rows if bool(row.get("supported"))]
        key = lambda row: (
            str(row.get("county_id") or ""),
            -float(row.get("projected_savings_display") or 0.0),
            -float(row.get("assessment_basis_value") or 0.0),
            str(row.get("account_number") or ""),
        )
    else:
        raise ValueError(f"Unknown watchlist kind: {kind}")

    selected: list[dict[str, Any]] = []
    per_county_counts: dict[str, int] = {}
    for row in sorted(scoped, key=key):
        county_id = str(row.get("county_id") or "")
        count = per_county_counts.get(county_id, 0)
        if count >= limit_per_county:
            continue
        selected.append(row)
        per_county_counts[county_id] = count + 1
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate weekly Harris/Fort Bend quote-quality monitor and watchlists."
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", type=int, default=get_settings().default_tax_year)
    parser.add_argument("--recent-run-limit", type=int, default=8)
    parser.add_argument("--watchlist-limit", type=int, default=25)
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=get_settings().instant_quote_denominator_shift_alert_threshold,
    )
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN_OUTPUT)
    parser.add_argument("--zero-watchlist-output", type=Path, default=DEFAULT_ZERO_WATCHLIST_OUTPUT)
    parser.add_argument(
        "--outlier-watchlist-output", type=Path, default=DEFAULT_OUTLIER_WATCHLIST_OUTPUT
    )
    parser.add_argument(
        "--watchlist-summary-output", type=Path, default=DEFAULT_WATCHLIST_SUMMARY_OUTPUT
    )
    return parser


def build_payload(
    *,
    county_ids: list[str],
    tax_year: int,
    recent_run_limit: int,
    threshold_pct: float,
    connection_factory: Any = get_connection,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    with connection_factory() as connection:
        with connection.cursor() as cursor:
            county_payloads = [
                _build_county_payload(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                    recent_run_limit=recent_run_limit,
                    threshold_pct=threshold_pct,
                )
                for county_id in county_ids
            ]
            zero_rows = _fetch_watchlist_candidates(
                cursor,
                county_ids=county_ids,
                tax_year=tax_year,
            )
    return {
        "generated_at": generated_at,
        "tax_year": tax_year,
        "county_ids": county_ids,
        "threshold_pct": threshold_pct,
        "counties": county_payloads,
        "combined": _combined_payload(county_payloads),
        "non_prod_divergence_drill": divergence_drill_result(
            all_sfr_total=100,
            all_sfr_supported=75,
            strict_sfr_total=80,
            strict_sfr_supported=75,
        ),
        "watchlist_source_row_count": len(zero_rows),
    }


def build_watchlists(
    *,
    county_ids: list[str],
    tax_year: int,
    limit_per_county: int,
    connection_factory: Any = get_connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with connection_factory() as connection:
        with connection.cursor() as cursor:
            candidates = _fetch_watchlist_candidates(
                cursor,
                county_ids=county_ids,
                tax_year=tax_year,
            )
    return (
        select_watchlist_rows(candidates, kind="zero_savings", limit_per_county=limit_per_county),
        select_watchlist_rows(candidates, kind="top_outliers", limit_per_county=limit_per_county),
    )


def _build_county_payload(
    cursor: Any,
    *,
    county_id: str,
    tax_year: int,
    recent_run_limit: int,
    threshold_pct: float,
) -> dict[str, Any]:
    runs = _fetch_recent_validation_runs(
        cursor,
        county_id=county_id,
        tax_year=tax_year,
        limit=recent_run_limit,
    )
    current = runs[0] if runs else {}
    current_report = current.get("validation_report") or {}
    prior = runs[1] if len(runs) > 1 else {}
    prior_report = prior.get("validation_report") or {}
    all_current = int(current_report.get("total_count_all_sfr_flagged") or 0)
    all_prior = (
        int(prior_report.get("total_count_all_sfr_flagged"))
        if prior_report.get("total_count_all_sfr_flagged") is not None
        else None
    )
    strict_current = int(current_report.get("total_count_strict_sfr_eligible") or 0)
    strict_prior = (
        int(prior_report.get("total_count_strict_sfr_eligible"))
        if prior_report.get("total_count_strict_sfr_eligible") is not None
        else None
    )
    leakage = _excluded_class_leakage_metrics(
        cursor,
        county_id=county_id,
        tax_year=tax_year,
    )
    zero_sample_count = int(current_report.get("monitored_zero_savings_supported_quote_count") or 0)
    zero_count = int(current_report.get("monitored_zero_savings_quote_count") or 0)
    return {
        "county_id": county_id,
        "latest_refresh_run_id": str(current.get("instant_quote_refresh_run_id") or ""),
        "latest_refresh_status": current.get("refresh_status"),
        "latest_refresh_started_at": _iso(current.get("refresh_started_at")),
        "latest_refresh_finished_at": _iso(current.get("refresh_finished_at")),
        "latest_validated_at": _iso(current.get("validated_at")),
        "denominator_shift": {
            "total_count_all_sfr_flagged": all_current,
            "prior_total_count_all_sfr_flagged": all_prior,
            "all_sfr_flagged": denominator_shift_status(
                current=all_current,
                prior=all_prior,
                threshold_pct=threshold_pct,
            ),
            "total_count_strict_sfr_eligible": strict_current,
            "prior_total_count_strict_sfr_eligible": strict_prior,
            "strict_sfr_eligible": denominator_shift_status(
                current=strict_current,
                prior=strict_prior,
                threshold_pct=threshold_pct,
            ),
            "validation_alert": current_report.get("denominator_shift_alert") or {},
        },
        "excluded_class_leakage": leakage,
        "zero_savings": {
            "monitored_supported_quote_count": zero_sample_count,
            "monitored_zero_savings_quote_count": zero_count,
            "monitored_zero_savings_quote_share": (
                zero_count / zero_sample_count if zero_sample_count else 0.0
            ),
            "high_value_split_available": bool(current_report.get("high_value_support_rate") is not None),
            "special_district_heavy_split_available": bool(
                current_report.get("special_district_heavy_support_rate") is not None
            ),
        },
        "supportability": {
            "support_count_all_sfr_flagged": int(
                current_report.get("support_count_all_sfr_flagged") or 0
            ),
            "support_rate_all_sfr_flagged": float(
                current_report.get("support_rate_all_sfr_flagged") or 0.0
            ),
            "support_count_strict_sfr_eligible": int(
                current_report.get("support_count_strict_sfr_eligible") or 0
            ),
            "support_rate_strict_sfr_eligible": float(
                current_report.get("support_rate_strict_sfr_eligible") or 0.0
            ),
            "high_value_support_rate": float(current_report.get("high_value_support_rate") or 0.0),
            "special_district_heavy_support_rate": float(
                current_report.get("special_district_heavy_support_rate") or 0.0
            ),
        },
    }


def _combined_payload(counties: list[dict[str, Any]]) -> dict[str, Any]:
    supported = sum(
        int(county["supportability"]["support_count_all_sfr_flagged"]) for county in counties
    )
    total = sum(
        int(county["denominator_shift"]["total_count_all_sfr_flagged"]) for county in counties
    )
    zero_supported = sum(
        int(county["zero_savings"]["monitored_supported_quote_count"]) for county in counties
    )
    zero_count = sum(
        int(county["zero_savings"]["monitored_zero_savings_quote_count"]) for county in counties
    )
    return {
        "support_count_all_sfr_flagged": supported,
        "total_count_all_sfr_flagged": total,
        "support_rate_all_sfr_flagged": supported / total if total else 0.0,
        "monitored_zero_savings_quote_count": zero_count,
        "monitored_supported_quote_count": zero_supported,
        "monitored_zero_savings_quote_share": (
            zero_count / zero_supported if zero_supported else 0.0
        ),
        "denominator_shift_alert_count": sum(
            1
            for county in counties
            if bool(
                county["denominator_shift"]
                .get("validation_alert", {})
                .get("triggered")
            )
        ),
        "strong_signal_leakage_count": sum(
            int(county["excluded_class_leakage"]["strong_signal_excluded"])
            for county in counties
        ),
    }


def _fetch_recent_validation_runs(
    cursor: Any,
    *,
    county_id: str,
    tax_year: int,
    limit: int,
) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
          instant_quote_refresh_run_id,
          refresh_status,
          refresh_started_at,
          refresh_finished_at,
          validated_at,
          validation_report
        FROM instant_quote_refresh_runs
        WHERE county_id = %s
          AND tax_year = %s
          AND refresh_status = 'completed'
          AND validated_at IS NOT NULL
          AND validation_report IS NOT NULL
        ORDER BY validated_at DESC, refresh_started_at DESC
        LIMIT %s
        """,
        (county_id, tax_year, limit),
    )
    return [dict(row) for row in cursor.fetchall()]


def _excluded_class_leakage_metrics(
    cursor: Any,
    *,
    county_id: str,
    tax_year: int,
) -> dict[str, Any]:
    strict_classes = list(_strict_sfr_class_codes(county_id=county_id))
    cursor.execute(
        """
        WITH excluded AS (
          SELECT
            pys.account_number,
            COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
            COALESCE(pc.homestead_flag, false) AS pc_homestead,
            COALESCE(pc.owner_occupied_flag, false) AS pc_owner_occupied,
            COALESCE(pa.homestead_flag, false) AS assessment_homestead,
            COALESCE(pi.max_living_area_sf, 0) AS max_living_area_sf,
            COALESCE(pi.has_year_built, false) AS has_year_built
          FROM parcel_year_snapshots pys
          JOIN parcels p
            ON p.parcel_id = pys.parcel_id
          JOIN property_characteristics pc
            ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
          LEFT JOIN parcel_assessments pa
            ON pa.parcel_id = pys.parcel_id
           AND pa.tax_year = pys.tax_year
          LEFT JOIN LATERAL (
            SELECT
              max(living_area_sf) AS max_living_area_sf,
              bool_or(year_built IS NOT NULL) AS has_year_built
            FROM improvements i
            WHERE i.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
              AND i.improvement_type = 'primary_structure'
          ) pi ON true
          WHERE pys.county_id = %s
            AND pys.tax_year = %s
            AND pys.is_current IS TRUE
            AND pc.property_type_code IS NULL
            AND NOT (
              UPPER(BTRIM(COALESCE(pc.property_class_code, p.property_class_code, '')))
              = ANY(%s::text[])
            )
        ),
        flagged AS (
          SELECT
            *,
            (
              pc_homestead
              OR pc_owner_occupied
              OR assessment_homestead
              OR max_living_area_sf > 0
              OR has_year_built
            ) AS has_strong_signal
          FROM excluded
        ),
        top_classes AS (
          SELECT
            property_class_code,
            COUNT(*) AS excluded_count,
            COUNT(*) FILTER (WHERE has_strong_signal) AS strong_signal_count
          FROM flagged
          GROUP BY property_class_code
          ORDER BY strong_signal_count DESC, excluded_count DESC, property_class_code ASC
          LIMIT 10
        )
        SELECT
          (SELECT COUNT(*) FROM flagged) AS total_excluded,
          (SELECT COUNT(*) FROM flagged WHERE has_strong_signal) AS strong_signal_excluded,
          COALESCE(
            (SELECT COUNT(*) FROM flagged WHERE has_strong_signal)::numeric
            / NULLIF((SELECT COUNT(*) FROM flagged), 0),
            0
          ) AS leakage_ratio,
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'property_class_code', property_class_code,
                'excluded_count', excluded_count,
                'strong_signal_count', strong_signal_count
              )
              ORDER BY strong_signal_count DESC, excluded_count DESC, property_class_code ASC
            ),
            '[]'::jsonb
          ) AS top_class_codes
        FROM top_classes
        """,
        (county_id, tax_year, strict_classes),
    )
    row = cursor.fetchone() or {}
    return {
        "total_excluded": int(row.get("total_excluded") or 0),
        "strong_signal_excluded": int(row.get("strong_signal_excluded") or 0),
        "leakage_ratio": float(row.get("leakage_ratio") or 0.0),
        "top_class_codes": list(row.get("top_class_codes") or []),
    }


def _fetch_watchlist_candidates(
    cursor: Any,
    *,
    county_ids: list[str],
    tax_year: int,
) -> list[dict[str, Any]]:
    cursor.execute(
        """
        WITH latest_runs AS (
          SELECT DISTINCT ON (county_id)
            county_id,
            refresh_started_at
          FROM instant_quote_refresh_runs
          WHERE county_id = ANY(%s::text[])
            AND tax_year = %s
            AND refresh_status = 'completed'
          ORDER BY county_id, refresh_started_at DESC
        ),
        latest_logs AS (
          SELECT DISTINCT ON (logs.county_id, logs.account_number)
            logs.county_id,
            logs.account_number,
            md5(logs.account_number) AS account_hash,
            logs.supported,
            logs.support_blocker_code,
            logs.unsupported_reason,
            logs.savings_estimate_display AS projected_savings_display,
            logs.savings_estimate_raw AS projected_savings_raw,
            logs.reduction_estimate_display,
            logs.basis_code,
            logs.tax_rate_source_method,
            logs.created_at
          FROM instant_quote_request_logs logs
          JOIN latest_runs
            ON latest_runs.county_id = logs.county_id
          WHERE logs.tax_year = %s
            AND logs.county_id = ANY(%s::text[])
            AND logs.created_at >= latest_runs.refresh_started_at
          ORDER BY logs.county_id, logs.account_number, logs.created_at DESC
        )
        SELECT
          latest_logs.county_id,
          latest_logs.account_number,
          latest_logs.account_hash,
          latest_logs.supported,
          latest_logs.support_blocker_code,
          latest_logs.unsupported_reason,
          latest_logs.projected_savings_display,
          latest_logs.projected_savings_raw,
          latest_logs.reduction_estimate_display,
          latest_logs.basis_code,
          latest_logs.tax_rate_source_method,
          latest_logs.created_at,
          subject.assessed_value,
          subject.assessment_basis_value,
          subject.effective_tax_rate,
          subject.effective_tax_rate_source_method,
          subject.support_blocker_code AS subject_support_blocker_code,
          subject.property_class_code,
          subject.property_type_code,
          subject.effective_tax_rate_basis_year,
          subject.effective_tax_rate_basis_status
        FROM latest_logs
        LEFT JOIN instant_quote_subject_cache subject
          ON subject.county_id = latest_logs.county_id
         AND subject.tax_year = %s
         AND subject.account_number = latest_logs.account_number
        """,
        (county_ids, tax_year, tax_year, county_ids, tax_year),
    )
    return [dict(row) for row in cursor.fetchall()]


def _strict_sfr_class_codes(*, county_id: str) -> tuple[str, ...]:
    config = load_county_adapter_config(county_id)
    dataset_mapping = config.field_mappings.get("property_roll")
    if dataset_mapping is None:
        return ()
    for section_name in ("characteristics", "parcel"):
        section = dataset_mapping.sections.get(section_name)
        if section is None:
            continue
        for field_mapping in section.fields:
            if (
                field_mapping.target_field == "property_type_code"
                and field_mapping.transform == "property_class_to_sfr"
            ):
                return tuple(
                    sorted(
                        {
                            str(code).strip().upper()
                            for code in field_mapping.transform_options.get(
                                "sfr_class_codes", []
                            )
                            if str(code).strip()
                        }
                    )
                )
    return ()


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Weekly Quote Quality Monitor",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Tax year: `{payload['tax_year']}`",
        "",
        "## County Trends",
        "",
    ]
    for county in payload["counties"]:
        denom = county["denominator_shift"]
        leakage = county["excluded_class_leakage"]
        zero = county["zero_savings"]
        lines.extend(
            [
                f"### {county['county_id']}",
                f"- Latest refresh run: `{county['latest_refresh_run_id']}`",
                f"- Validation status: `{county['latest_refresh_status']}` at `{county['latest_validated_at']}`",
                f"- All-SFR denominator: `{denom['total_count_all_sfr_flagged']}` "
                f"(prior `{denom['prior_total_count_all_sfr_flagged']}`, "
                f"status `{denom['all_sfr_flagged']['status']}`)",
                f"- Strict-SFR denominator: `{denom['total_count_strict_sfr_eligible']}` "
                f"(prior `{denom['prior_total_count_strict_sfr_eligible']}`, "
                f"status `{denom['strict_sfr_eligible']['status']}`)",
                f"- Excluded-class leakage: `{leakage['strong_signal_excluded']}` / "
                f"`{leakage['total_excluded']}` = `{leakage['leakage_ratio']:.6f}`",
                f"- Monitored $0 share: `{zero['monitored_zero_savings_quote_share']:.6f}`",
                "",
            ]
        )
    combined = payload["combined"]
    lines.extend(
        [
            "## Combined",
            f"- Support rate all-SFR flagged: `{combined['support_rate_all_sfr_flagged']:.6f}`",
            f"- Monitored $0 share: `{combined['monitored_zero_savings_quote_share']:.6f}`",
            f"- Denominator-shift alert count: `{combined['denominator_shift_alert_count']}`",
            f"- Strong-signal leakage count: `{combined['strong_signal_leakage_count']}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_watchlist_summary(
    *,
    zero_rows: list[dict[str, Any]],
    outlier_rows: list[dict[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Refresh Watchlist Summary",
            "",
            f"- Zero-savings watchlist rows: `{len(zero_rows)}`",
            f"- Top-outlier watchlist rows: `{len(outlier_rows)}`",
            "",
            "## Triage Rubric",
            "- `likely_legitimate_no_reduction`: supported quote, plausible tax rate, no material reduction signal, and no data-completeness blocker.",
            "- `likely_data_quality_issue`: unexpected blocker, missing or stale source metadata, implausible class/type, or denominator/leakage alert nearby.",
            "- `likely_valuation_model_outlier`: high projected savings driven by valuation assumptions rather than tax-rate or source metadata.",
            "- `escalate`: projected savings exceeds the public-safe threshold, effective tax rate/component metadata looks implausible, or a non-SFR class appears in the quoteable SFR cohort.",
            "",
        ]
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "county_id",
        "account_number",
        "assessed_value",
        "assessment_basis_value",
        "projected_savings_display",
        "projected_savings_raw",
        "reduction_estimate_display",
        "effective_tax_rate",
        "effective_tax_rate_source_method",
        "support_blocker_code",
        "unsupported_reason",
        "basis_code",
        "tax_rate_source_method",
        "property_class_code",
        "property_type_code",
        "effective_tax_rate_basis_year",
        "effective_tax_rate_basis_status",
        "created_at",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _iso(value: Any) -> str | None:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return None if value is None else str(value)


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        recent_run_limit=args.recent_run_limit,
        threshold_pct=args.threshold_pct,
    )
    zero_rows, outlier_rows = build_watchlists(
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        limit_per_county=args.watchlist_limit,
    )
    args.json_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    args.markdown_output.write_text(render_markdown(payload))
    write_csv(args.zero_watchlist_output, zero_rows)
    write_csv(args.outlier_watchlist_output, outlier_rows)
    args.watchlist_summary_output.write_text(
        render_watchlist_summary(zero_rows=zero_rows, outlier_rows=outlier_rows)
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
