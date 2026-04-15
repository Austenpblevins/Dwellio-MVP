from __future__ import annotations

from typing import Any

from app.services.exemption_code_dictionary import (
    EXEMPTION_ALIAS_LOOKUP,
    map_raw_exemption_codes,
    normalize_known_exemption_type_code,
    UNKNOWN_EXEMPTION_TYPE_CODE,
)


def normalize_parcel_exemptions(
    exemptions: list[dict[str, Any]] | None,
    *,
    county_id: str | None = None,
) -> list[dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}

    for exemption in exemptions or []:
        raw_codes = collect_raw_exemption_codes(exemption)
        canonical_codes = _resolve_canonical_codes(
            exemption=exemption,
            raw_codes=raw_codes,
            county_id=county_id,
        )
        amount = exemption.get("exemption_amount")
        granted_flag = bool(exemption.get("granted_flag", True))
        split_amount = amount if len(canonical_codes) == 1 else None

        for canonical_code in canonical_codes:
            entry = aggregated.setdefault(
                canonical_code,
                {
                    "exemption_type_code": canonical_code,
                    "exemption_amount": 0 if split_amount is not None else None,
                    "granted_flag": granted_flag,
                    "raw_exemption_codes": [],
                    "source_entry_count": 0,
                    "amount_missing_flag": False,
                },
            )

            if split_amount is not None:
                entry["exemption_amount"] = (entry["exemption_amount"] or 0) + split_amount
            entry["granted_flag"] = bool(entry["granted_flag"] or granted_flag)
            entry["source_entry_count"] += 1
            entry["amount_missing_flag"] = bool(
                entry["amount_missing_flag"] or (granted_flag and split_amount is None)
            )
            entry["raw_exemption_codes"] = merge_raw_exemption_codes(
                entry["raw_exemption_codes"], raw_codes
            )

    return [aggregated[key] for key in sorted(aggregated)]


def normalize_exemption_type_code(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    normalized = normalized.replace("-", "_").replace(" ", "_")
    return EXEMPTION_ALIAS_LOOKUP.get(normalized, normalized)


def collect_raw_exemption_codes(exemption: dict[str, Any]) -> list[str]:
    collected: list[str] = []
    for field_name in ("raw_exemption_codes",):
        value = exemption.get(field_name)
        if isinstance(value, list):
            collected.extend(str(item).strip() for item in value if str(item).strip())

    for field_name in ("raw_exemption_code", "source_exemption_code"):
        value = exemption.get(field_name)
        if value is not None and str(value).strip():
            collected.append(str(value).strip())

    if not collected and exemption.get("exemption_type_code") is not None:
        collected.append(str(exemption["exemption_type_code"]).strip())

    return merge_raw_exemption_codes([], collected)


def _resolve_canonical_codes(
    *,
    exemption: dict[str, Any],
    raw_codes: list[str],
    county_id: str | None,
) -> list[str]:
    explicit = normalize_known_exemption_type_code(exemption.get("exemption_type_code"))
    canonical_codes: list[str] = []
    if explicit is not None:
        canonical_codes.append(explicit)

    if county_id and raw_codes:
        mapped = map_raw_exemption_codes(county_id=county_id, raw_codes=raw_codes)
        for mapping in mapped:
            code = normalize_known_exemption_type_code(mapping.canonical_exemption_type_code)
            if code is None:
                code = UNKNOWN_EXEMPTION_TYPE_CODE
            if code not in canonical_codes:
                canonical_codes.append(code)
    elif not canonical_codes:
        fallback = normalize_known_exemption_type_code(exemption.get("raw_exemption_code"))
        if fallback is not None:
            canonical_codes.append(fallback)

    if not canonical_codes:
        canonical_codes.append(UNKNOWN_EXEMPTION_TYPE_CODE)
    return canonical_codes


def merge_raw_exemption_codes(existing_codes: list[str], new_codes: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for code in [*existing_codes, *new_codes]:
        cleaned = str(code).strip()
        if not cleaned:
            continue
        dedupe_key = cleaned.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(cleaned)
    return merged
