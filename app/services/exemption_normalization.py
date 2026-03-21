from __future__ import annotations

from typing import Any

EXEMPTION_TYPE_ALIASES: dict[str, set[str]] = {
    "homestead": {"homestead", "hs", "hs_amt", "residence_homestead"},
    "over65": {"over65", "over_65", "ov65", "ov65_amt"},
    "disabled_person": {"disabled", "disabled_person", "dp", "dp_amt"},
    "disabled_veteran": {"disabled_vet", "disabled_veteran", "dv", "dv_amt"},
    "freeze_ceiling": {"ceiling", "freeze", "freeze_ceiling", "tax_ceiling"},
    "surviving_spouse": {"surviving_spouse", "surv_spouse"},
    "ag": {"ag", "ag_use", "agricultural"},
}

EXEMPTION_ALIAS_LOOKUP = {
    alias: canonical_code
    for canonical_code, aliases in EXEMPTION_TYPE_ALIASES.items()
    for alias in aliases
}


def normalize_parcel_exemptions(exemptions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}

    for exemption in exemptions or []:
        canonical_code = normalize_exemption_type_code(
            exemption.get("exemption_type_code") or exemption.get("raw_exemption_code")
        )
        if canonical_code is None:
            continue

        raw_codes = collect_raw_exemption_codes(exemption)
        amount = exemption.get("exemption_amount")
        granted_flag = bool(exemption.get("granted_flag", True))

        entry = aggregated.setdefault(
            canonical_code,
            {
                "exemption_type_code": canonical_code,
                "exemption_amount": 0 if amount is not None else None,
                "granted_flag": granted_flag,
                "raw_exemption_codes": [],
                "source_entry_count": 0,
                "amount_missing_flag": False,
            },
        )

        if amount is not None:
            entry["exemption_amount"] = (entry["exemption_amount"] or 0) + amount
        entry["granted_flag"] = bool(entry["granted_flag"] or granted_flag)
        entry["source_entry_count"] += 1
        entry["amount_missing_flag"] = bool(
            entry["amount_missing_flag"] or (granted_flag and amount is None)
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
