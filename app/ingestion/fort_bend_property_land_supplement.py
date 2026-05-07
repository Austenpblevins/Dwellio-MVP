from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.db.connection import get_connection


LAND_SF_CONFLICT_TOLERANCE = 1.0
LAND_ACRES_CONFLICT_TOLERANCE = 0.0001
FRONTAGE_CONFLICT_TOLERANCE = 0.5
DEPTH_CONFLICT_TOLERANCE = 0.5


@dataclass(frozen=True)
class PropertyLandSegment:
    account_number: str
    quick_ref_id: str | None
    sequence: float | None
    square_feet: float | None
    acres: float | None
    frontage_sf: float | None
    depth_sf: float | None


@dataclass(frozen=True)
class AggregatedSupplementalLand:
    account_number: str
    segment_count: int
    valid_segment_count: int
    primary_land_sf: float | None
    primary_land_acres: float | None
    primary_frontage_sf: float | None
    primary_depth_sf: float | None
    total_land_sf: float | None
    total_land_acres: float | None


@dataclass(frozen=True)
class CanonicalLandRow:
    parcel_id: str
    account_number: str
    land_sf: float | None
    land_acres: float | None
    frontage_sf: float | None
    depth_sf: float | None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _positive(value: float | None) -> bool:
    return value is not None and value > 0


def parse_property_land_e_file(path: str | Path) -> list[PropertyLandSegment]:
    file_path = Path(path)
    rows: list[PropertyLandSegment] = []
    with file_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            account_number = str(row.get("PropertyNumber") or "").strip()
            if not account_number:
                continue
            rows.append(
                PropertyLandSegment(
                    account_number=account_number,
                    quick_ref_id=str(row.get("QuickRefID") or "").strip() or None,
                    sequence=_as_float(row.get("Sequence")),
                    square_feet=_as_float(row.get("SquareFeet")),
                    acres=_as_float(row.get("Acres")),
                    frontage_sf=_as_float(row.get("EffFront")),
                    depth_sf=_as_float(row.get("EffDepth")),
                )
            )
    return rows


def aggregate_property_land_segments(
    segments: list[PropertyLandSegment],
) -> dict[str, AggregatedSupplementalLand]:
    grouped: dict[str, list[PropertyLandSegment]] = {}
    for segment in segments:
        grouped.setdefault(segment.account_number, []).append(segment)

    aggregated: dict[str, AggregatedSupplementalLand] = {}
    for account_number, account_segments in grouped.items():
        valid = [
            segment
            for segment in account_segments
            if _positive(segment.square_feet) or _positive(segment.acres)
        ]
        if not valid:
            continue

        primary = sorted(
            valid,
            key=lambda segment: (
                segment.square_feet or 0.0,
                segment.acres or 0.0,
                -1.0 * ((segment.sequence or 9_999_999.0)),
            ),
            reverse=True,
        )[0]

        total_land_sf = sum((segment.square_feet or 0.0) for segment in valid)
        total_land_acres = sum((segment.acres or 0.0) for segment in valid)
        aggregated[account_number] = AggregatedSupplementalLand(
            account_number=account_number,
            segment_count=len(account_segments),
            valid_segment_count=len(valid),
            primary_land_sf=round(primary.square_feet, 4) if _positive(primary.square_feet) else None,
            primary_land_acres=round(primary.acres, 8) if _positive(primary.acres) else None,
            primary_frontage_sf=round(primary.frontage_sf, 4)
            if _positive(primary.frontage_sf)
            else None,
            primary_depth_sf=round(primary.depth_sf, 4) if _positive(primary.depth_sf) else None,
            total_land_sf=round(total_land_sf, 4) if _positive(total_land_sf) else None,
            total_land_acres=round(total_land_acres, 8) if _positive(total_land_acres) else None,
        )
    return aggregated


def load_fort_bend_2026_canonical_land(
    *, tax_year: int = 2026
) -> dict[str, CanonicalLandRow]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 120000")
            cursor.execute("SET max_parallel_workers_per_gather = 0")
            cursor.execute(
                """
                SELECT
                  p.parcel_id::text,
                  pys.account_number,
                  pl.land_sf,
                  pl.land_acres,
                  pl.frontage_sf,
                  pl.depth_sf
                FROM parcel_year_snapshots AS pys
                JOIN parcels AS p
                  ON p.parcel_id = pys.parcel_id
                 AND p.county_id = 'fort_bend'
                LEFT JOIN parcel_lands AS pl
                  ON pl.parcel_id = pys.parcel_id
                 AND pl.tax_year = %s
                WHERE pys.county_id = 'fort_bend'
                  AND pys.tax_year = %s
                  AND pys.is_current = true
                """,
                (tax_year, tax_year),
            )
            rows = cursor.fetchall()

    by_account: dict[str, CanonicalLandRow] = {}
    for row in rows:
        account_number = str(row.get("account_number") or "").strip()
        if not account_number:
            continue
        by_account[account_number] = CanonicalLandRow(
            parcel_id=str(row.get("parcel_id")),
            account_number=account_number,
            land_sf=_as_float(row.get("land_sf")),
            land_acres=_as_float(row.get("land_acres")),
            frontage_sf=_as_float(row.get("frontage_sf")),
            depth_sf=_as_float(row.get("depth_sf")),
        )
    return by_account


def _field_conflicts(existing: float | None, source: float | None, tolerance: float) -> bool:
    if not (_positive(existing) and _positive(source)):
        return False
    return abs(float(existing) - float(source)) > tolerance


def build_fill_only_plan(
    canonical_by_account: dict[str, CanonicalLandRow],
    supplemental_by_account: dict[str, AggregatedSupplementalLand],
) -> dict[str, Any]:
    matches = set(canonical_by_account).intersection(supplemental_by_account)
    unmatched_canonical = set(canonical_by_account) - set(supplemental_by_account)
    unmatched_supplemental = set(supplemental_by_account) - set(canonical_by_account)

    fill_counts = {"land_sf": 0, "land_acres": 0, "frontage_sf": 0, "depth_sf": 0}
    preserve_counts = {"land_sf": 0, "land_acres": 0, "frontage_sf": 0, "depth_sf": 0}
    conflict_counts = {"land_sf": 0, "land_acres": 0, "frontage_sf": 0, "depth_sf": 0}

    fill_samples: list[dict[str, Any]] = []
    preserve_samples: list[dict[str, Any]] = []
    conflict_samples: list[dict[str, Any]] = []

    for account_number in sorted(matches):
        canonical = canonical_by_account[account_number]
        supplemental = supplemental_by_account[account_number]
        source_values = {
            "land_sf": supplemental.primary_land_sf,
            "land_acres": supplemental.primary_land_acres,
            "frontage_sf": supplemental.primary_frontage_sf,
            "depth_sf": supplemental.primary_depth_sf,
        }
        existing_values = {
            "land_sf": canonical.land_sf,
            "land_acres": canonical.land_acres,
            "frontage_sf": canonical.frontage_sf,
            "depth_sf": canonical.depth_sf,
        }

        field_tolerances = {
            "land_sf": LAND_SF_CONFLICT_TOLERANCE,
            "land_acres": LAND_ACRES_CONFLICT_TOLERANCE,
            "frontage_sf": FRONTAGE_CONFLICT_TOLERANCE,
            "depth_sf": DEPTH_CONFLICT_TOLERANCE,
        }

        any_fill = False
        any_preserve = False
        any_conflict = False

        for field, source_value in source_values.items():
            if not _positive(source_value):
                continue
            existing_value = existing_values[field]
            if _positive(existing_value):
                preserve_counts[field] += 1
                any_preserve = True
                if _field_conflicts(existing_value, source_value, field_tolerances[field]):
                    conflict_counts[field] += 1
                    any_conflict = True
            else:
                fill_counts[field] += 1
                any_fill = True

        if any_fill and len(fill_samples) < 25:
            fill_samples.append(
                {
                    "account_number": account_number,
                    "existing": existing_values,
                    "supplemental": source_values,
                }
            )
        if any_preserve and len(preserve_samples) < 25:
            preserve_samples.append(
                {
                    "account_number": account_number,
                    "existing": existing_values,
                    "supplemental": source_values,
                }
            )
        if any_conflict and len(conflict_samples) < 25:
            conflict_samples.append(
                {
                    "account_number": account_number,
                    "existing": existing_values,
                    "supplemental": source_values,
                }
            )

    existing_land_sf_positive = sum(
        1 for row in canonical_by_account.values() if _positive(row.land_sf)
    )
    potential_land_sf_fill = sum(
        1
        for account_number, canonical in canonical_by_account.items()
        if (not _positive(canonical.land_sf))
        and _positive(
            supplemental_by_account.get(
                account_number,
                AggregatedSupplementalLand(
                    account_number=account_number,
                    segment_count=0,
                    valid_segment_count=0,
                    primary_land_sf=None,
                    primary_land_acres=None,
                    primary_frontage_sf=None,
                    primary_depth_sf=None,
                    total_land_sf=None,
                    total_land_acres=None,
                ),
            ).primary_land_sf
        )
    )

    return {
        "stage21_accounts_total": len(canonical_by_account),
        "supplemental_accounts_total": len(supplemental_by_account),
        "join_match_accounts": len(matches),
        "join_unmatched_canonical_accounts": len(unmatched_canonical),
        "join_unmatched_supplemental_accounts": len(unmatched_supplemental),
        "fill_counts": fill_counts,
        "preserve_counts": preserve_counts,
        "conflict_counts": conflict_counts,
        "existing_land_sf_positive": existing_land_sf_positive,
        "potential_additional_land_sf_positive_fill_only": potential_land_sf_fill,
        "projected_land_sf_positive_after_fill_only": existing_land_sf_positive
        + potential_land_sf_fill,
        "fill_samples": fill_samples,
        "preserve_samples": preserve_samples,
        "conflict_samples": conflict_samples,
    }
