from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULT_EXEMPTION_CODE_DICTIONARY_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "exemptions" / "exemption_code_dictionary.csv"
)

UNKNOWN_EXEMPTION_TYPE_CODE = "unknown"

EXEMPTION_TYPE_ALIASES: dict[str, set[str]] = {
    "homestead": {"homestead", "hs", "hs_amt", "residence_homestead"},
    "over65": {"over65", "over_65", "ov65", "ov65_amt"},
    "disabled_person": {"disabled", "disabled_person", "dp", "dp_amt"},
    "disabled_veteran": {"disabled_vet", "disabled_veteran", "dv", "dv_amt"},
    "freeze_ceiling": {"ceiling", "freeze", "freeze_ceiling", "tax_ceiling"},
    "surviving_spouse": {"surviving_spouse", "surv_spouse"},
    "ag": {"ag", "ag_use", "agricultural"},
}

KNOWN_CANONICAL_EXEMPTION_TYPE_CODES = {
    *EXEMPTION_TYPE_ALIASES.keys(),
    UNKNOWN_EXEMPTION_TYPE_CODE,
}

EXEMPTION_ALIAS_LOOKUP = {
    alias: canonical_code
    for canonical_code, aliases in EXEMPTION_TYPE_ALIASES.items()
    for alias in aliases
}


@dataclass(frozen=True)
class ExemptionCodeMapping:
    county_id: str
    raw_exemption_code: str
    canonical_exemption_type_code: str
    description: str | None
    mapping_status: str
    notes: str | None


def normalize_exemption_type_code(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    normalized = normalized.replace("-", "_").replace(" ", "_")
    return EXEMPTION_ALIAS_LOOKUP.get(normalized, normalized)


def normalize_known_exemption_type_code(value: Any) -> str | None:
    normalized = normalize_exemption_type_code(value)
    if normalized is None:
        return None
    if normalized in KNOWN_CANONICAL_EXEMPTION_TYPE_CODES:
        return normalized
    return None


def normalize_raw_exemption_code(value: Any) -> str:
    return str(value or "").strip().upper()


def split_raw_exemption_code_tokens(value: Any) -> list[str]:
    raw = normalize_raw_exemption_code(value)
    if not raw:
        return []
    tokens = [token for token in re.split(r"[^A-Z0-9]+", raw) if token]
    if not tokens:
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def load_exemption_code_dictionary(
    dictionary_path: Path = DEFAULT_EXEMPTION_CODE_DICTIONARY_PATH,
) -> dict[tuple[str, str], tuple[ExemptionCodeMapping, ...]]:
    return _load_exemption_code_dictionary_cached(str(dictionary_path.resolve()))


@lru_cache(maxsize=4)
def _load_exemption_code_dictionary_cached(
    dictionary_path: str,
) -> dict[tuple[str, str], tuple[ExemptionCodeMapping, ...]]:
    path = Path(dictionary_path)
    mappings: dict[tuple[str, str], list[ExemptionCodeMapping]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            county_id = str(row.get("county_id") or "").strip().lower()
            raw_code = normalize_raw_exemption_code(row.get("raw_exemption_code"))
            canonical_code = str(row.get("canonical_exemption_type_code") or "").strip().lower()
            if not county_id or not raw_code or not canonical_code:
                continue
            mapping = ExemptionCodeMapping(
                county_id=county_id,
                raw_exemption_code=raw_code,
                canonical_exemption_type_code=canonical_code,
                description=_optional_text(row.get("description")),
                mapping_status=str(row.get("mapping_status") or "unknown").strip().lower() or "unknown",
                notes=_optional_text(row.get("notes")),
            )
            bucket = mappings.setdefault((county_id, raw_code), [])
            duplicate_key = (
                mapping.canonical_exemption_type_code,
                mapping.mapping_status,
                mapping.description,
                mapping.notes,
            )
            if any(
                (
                    existing.canonical_exemption_type_code,
                    existing.mapping_status,
                    existing.description,
                    existing.notes,
                ) == duplicate_key
                for existing in bucket
            ):
                continue
            bucket.append(mapping)
    return {
        key: tuple(value)
        for key, value in mappings.items()
    }


def map_raw_exemption_codes(
    *,
    county_id: str,
    raw_codes: list[str],
    dictionary_path: Path = DEFAULT_EXEMPTION_CODE_DICTIONARY_PATH,
) -> list[ExemptionCodeMapping]:
    county_key = str(county_id or "").strip().lower()
    dictionary = load_exemption_code_dictionary(dictionary_path=dictionary_path)
    mapped: list[ExemptionCodeMapping] = []
    seen: set[tuple[str, str]] = set()
    for raw_code in raw_codes:
        for token in split_raw_exemption_code_tokens(raw_code):
            dictionary_entries = dictionary.get((county_key, token))
            if dictionary_entries is not None:
                for dictionary_entry in dictionary_entries:
                    key = (dictionary_entry.canonical_exemption_type_code, dictionary_entry.raw_exemption_code)
                    if key in seen:
                        continue
                    seen.add(key)
                    mapped.append(dictionary_entry)
                continue

            alias_code = normalize_known_exemption_type_code(token)
            if alias_code is not None:
                key = (alias_code, token)
                if key in seen:
                    continue
                seen.add(key)
                mapped.append(
                    ExemptionCodeMapping(
                        county_id=county_key,
                        raw_exemption_code=token,
                        canonical_exemption_type_code=alias_code,
                        description=None,
                        mapping_status="alias",
                        notes="Global alias fallback mapping.",
                    )
                )
                continue

            key = (UNKNOWN_EXEMPTION_TYPE_CODE, token)
            if key in seen:
                continue
            seen.add(key)
            mapped.append(
                ExemptionCodeMapping(
                    county_id=county_key,
                    raw_exemption_code=token,
                    canonical_exemption_type_code=UNKNOWN_EXEMPTION_TYPE_CODE,
                    description=None,
                    mapping_status="unknown",
                    notes="No dictionary mapping found; preserved as unknown canonical type.",
                )
            )
    return mapped


def _optional_text(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None
