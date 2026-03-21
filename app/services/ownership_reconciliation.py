from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from app.utils.hashing import sha256_text


def normalize_owner_name(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.upper().strip()
    cleaned = re.sub(r"[^A-Z0-9 ]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or None


@dataclass(frozen=True)
class OwnerPeriodCandidate:
    owner_name: str
    owner_name_normalized: str | None
    start_date: date | None
    end_date: date | None
    source_basis: str
    deed_record_id: str | None
    source_system_id: str | None
    confidence_score: float
    is_current: bool
    metadata_json: dict[str, Any]
    mailing_address: str | None = None


@dataclass(frozen=True)
class CurrentOwnerRollupCandidate:
    owner_name: str
    owner_name_normalized: str | None
    owner_names_json: list[str]
    mailing_address: str | None
    mailing_city: str | None
    mailing_state: str | None
    mailing_zip: str | None
    source_basis: str
    source_record_hash: str | None
    source_system_id: str | None
    owner_period_id: str | None
    confidence_score: float
    override_flag: bool
    metadata_json: dict[str, Any]


def build_normalized_deed_record(*, county_id: str, row: dict[str, Any]) -> dict[str, Any]:
    grantor_parties = build_deed_parties(row.get("grantors"), role="grantor")
    grantee_parties = build_deed_parties(row.get("grantees"), role="grantee")
    other_parties = build_deed_parties(row.get("other_parties"), role="other")
    deed_parties = [*grantor_parties, *grantee_parties, *other_parties]

    metadata_json = dict(row.get("metadata_json") or {})
    metadata_json.update(
        {
            "county_id": county_id,
            "linked_account_number": row.get("account_number"),
            "linked_cad_property_id": row.get("cad_property_id"),
            "linked_aliases": list(row.get("alias_values") or []),
            "source_dataset_type": "deeds",
        }
    )

    deed_record = {
        "instrument_number": row.get("instrument_number"),
        "volume_page": row.get("volume_page"),
        "recording_date": row.get("recording_date"),
        "execution_date": row.get("execution_date"),
        "consideration_amount": row.get("consideration_amount"),
        "document_type": row.get("document_type"),
        "transfer_type": row.get("transfer_type"),
        "grantor_summary": summarize_party_names(grantor_parties) or row.get("grantor_summary"),
        "grantee_summary": summarize_party_names(grantee_parties) or row.get("grantee_summary"),
        "metadata_json": metadata_json,
    }

    canonical_payload = {
        "deed_record": deed_record,
        "deed_parties": deed_parties,
        "linked_account_number": row.get("account_number"),
        "linked_cad_property_id": row.get("cad_property_id"),
        "linked_alias_values": list(row.get("alias_values") or []),
    }
    canonical_payload["source_record_hash"] = sha256_text(
        json.dumps(canonical_payload, sort_keys=True, default=str)
    )
    return canonical_payload


def build_deed_parties(parties: Any, *, role: str) -> list[dict[str, Any]]:
    normalized_parties: list[dict[str, Any]] = []
    for index, party in enumerate(parties or [], start=1):
        if isinstance(party, str):
            party_name = party.strip()
            mailing_address = None
        else:
            party_name = str(
                (party or {}).get("party_name") or (party or {}).get("name") or ""
            ).strip()
            mailing_address = (party or {}).get("mailing_address")
        if not party_name:
            continue
        normalized_parties.append(
            {
                "party_role": role,
                "party_name": party_name,
                "normalized_name": normalize_owner_name(party_name),
                "party_order": index,
                "mailing_address": mailing_address,
            }
        )
    return normalized_parties


def summarize_party_names(parties: list[dict[str, Any]]) -> str | None:
    names = [party["party_name"] for party in parties if party.get("party_name")]
    if not names:
        return None
    return " & ".join(names)


def score_owner_period(
    *,
    source_basis: str,
    linked_parcel: bool,
    has_recording_date: bool,
    grantee_count: int,
    has_instrument_number: bool,
) -> tuple[float, list[str]]:
    score = 0.55 if source_basis == "cad_owner_snapshot" else 0.70
    reasons = [f"source_basis:{source_basis}"]
    if linked_parcel:
        score += 0.10
        reasons.append("linked_parcel")
    if has_recording_date:
        score += 0.05
        reasons.append("has_recording_date")
    if grantee_count > 0:
        score += 0.05
        reasons.append("has_grantee_party")
    if has_instrument_number:
        score += 0.05
        reasons.append("has_instrument_number")
    return min(round(score, 4), 0.98), reasons


def build_owner_periods(
    *,
    parcel_id: str,
    county_id: str,
    cad_owner_name: str | None,
    source_system_id: str | None,
    deed_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    sorted_records = sorted(
        deed_records,
        key=lambda record: (
            record.get("effective_date") or date.min,
            record.get("instrument_number") or "",
        ),
    )

    candidates: list[OwnerPeriodCandidate] = []
    for record in sorted_records:
        owner_name = record.get("grantee_summary") or summarize_party_names(
            record.get("grantee_parties", [])
        )
        if not owner_name:
            continue
        confidence_score, reasons = score_owner_period(
            source_basis="deed_grantee",
            linked_parcel=record.get("parcel_id") is not None,
            has_recording_date=record.get("effective_date") is not None,
            grantee_count=len(record.get("grantee_parties", [])),
            has_instrument_number=bool(record.get("instrument_number")),
        )
        candidates.append(
            OwnerPeriodCandidate(
                owner_name=owner_name,
                owner_name_normalized=normalize_owner_name(owner_name),
                start_date=record.get("effective_date"),
                end_date=None,
                source_basis="deed_grantee",
                deed_record_id=record.get("deed_record_id"),
                source_system_id=record.get("source_system_id"),
                confidence_score=confidence_score,
                is_current=False,
                metadata_json={
                    "score_reasons": reasons,
                    "supporting_deed_record_ids": [record.get("deed_record_id")],
                    "document_type": record.get("document_type"),
                    "transfer_type": record.get("transfer_type"),
                },
                mailing_address=record.get("mailing_address"),
            )
        )

    merged: list[OwnerPeriodCandidate] = []
    for candidate in candidates:
        if merged and merged[-1].owner_name_normalized == candidate.owner_name_normalized:
            previous = merged[-1]
            supporting_ids = [
                *previous.metadata_json.get("supporting_deed_record_ids", []),
                *candidate.metadata_json.get("supporting_deed_record_ids", []),
            ]
            merged[-1] = OwnerPeriodCandidate(
                owner_name=previous.owner_name,
                owner_name_normalized=previous.owner_name_normalized,
                start_date=previous.start_date or candidate.start_date,
                end_date=None,
                source_basis=previous.source_basis,
                deed_record_id=previous.deed_record_id,
                source_system_id=previous.source_system_id,
                confidence_score=max(previous.confidence_score, candidate.confidence_score),
                is_current=False,
                metadata_json={
                    **previous.metadata_json,
                    "supporting_deed_record_ids": supporting_ids,
                },
                mailing_address=previous.mailing_address or candidate.mailing_address,
            )
            continue
        merged.append(candidate)

    periods: list[dict[str, Any]] = []
    for index, candidate in enumerate(merged):
        next_start = merged[index + 1].start_date if index + 1 < len(merged) else None
        end_date = next_start - timedelta(days=1) if next_start is not None else None
        is_current = index == len(merged) - 1
        periods.append(
            {
                "parcel_id": parcel_id,
                "county_id": county_id,
                "owner_name": candidate.owner_name,
                "owner_name_normalized": candidate.owner_name_normalized,
                "start_date": candidate.start_date,
                "end_date": end_date,
                "source_basis": candidate.source_basis,
                "deed_record_id": candidate.deed_record_id,
                "source_system_id": candidate.source_system_id,
                "confidence_score": candidate.confidence_score,
                "is_current": is_current,
                "metadata_json": {
                    **candidate.metadata_json,
                    "mailing_address": candidate.mailing_address,
                },
            }
        )

    if periods:
        return periods

    if cad_owner_name:
        confidence_score, reasons = score_owner_period(
            source_basis="cad_owner_snapshot",
            linked_parcel=True,
            has_recording_date=False,
            grantee_count=0,
            has_instrument_number=False,
        )
        return [
            {
                "parcel_id": parcel_id,
                "county_id": county_id,
                "owner_name": cad_owner_name,
                "owner_name_normalized": normalize_owner_name(cad_owner_name),
                "start_date": None,
                "end_date": None,
                "source_basis": "cad_owner_snapshot",
                "deed_record_id": None,
                "source_system_id": source_system_id,
                "confidence_score": confidence_score,
                "is_current": True,
                "metadata_json": {"score_reasons": reasons, "supporting_deed_record_ids": []},
            }
        ]

    return []


def build_current_owner_rollup(
    *,
    tax_year: int,
    cad_owner_name: str | None,
    cad_owner_name_normalized: str | None,
    cad_source_system_id: str | None,
    owner_periods: list[dict[str, Any]],
    manual_override: dict[str, Any] | None,
) -> CurrentOwnerRollupCandidate | None:
    snapshot_date = date(tax_year, 12, 31)

    if manual_override is not None:
        payload = dict(manual_override.get("override_payload") or {})
        owner_name = payload.get("owner_name") or cad_owner_name
        if owner_name is None:
            return None
        return CurrentOwnerRollupCandidate(
            owner_name=owner_name,
            owner_name_normalized=normalize_owner_name(owner_name),
            owner_names_json=[owner_name],
            mailing_address=payload.get("mailing_address"),
            mailing_city=payload.get("mailing_city"),
            mailing_state=payload.get("mailing_state", "TX"),
            mailing_zip=payload.get("mailing_zip"),
            source_basis="manual_override",
            source_record_hash=None,
            source_system_id=None,
            owner_period_id=None,
            confidence_score=float(payload.get("confidence_score") or 1.0),
            override_flag=True,
            metadata_json={
                "manual_override_id": (
                    None
                    if manual_override.get("manual_override_id") is None
                    else str(manual_override.get("manual_override_id"))
                ),
                "override_reason": manual_override.get("reason"),
                "cad_owner_name": cad_owner_name,
            },
        )

    active_periods = [
        period
        for period in owner_periods
        if (period.get("start_date") is None or period["start_date"] <= snapshot_date)
        and (period.get("end_date") is None or period["end_date"] >= snapshot_date)
    ]
    if active_periods:
        selected = sorted(
            active_periods,
            key=lambda period: (
                period.get("confidence_score") or 0,
                period.get("start_date") or date.min,
            ),
            reverse=True,
        )[0]
        metadata_json = dict(selected.get("metadata_json") or {})
        metadata_json.update(
            {
                "cad_owner_name": cad_owner_name,
                "cad_owner_name_normalized": cad_owner_name_normalized,
                "selected_for_tax_year": tax_year,
            }
        )
        return CurrentOwnerRollupCandidate(
            owner_name=selected["owner_name"],
            owner_name_normalized=selected.get("owner_name_normalized"),
            owner_names_json=[selected["owner_name"]],
            mailing_address=metadata_json.get("mailing_address"),
            mailing_city=None,
            mailing_state="TX",
            mailing_zip=None,
            source_basis=selected["source_basis"],
            source_record_hash=None,
            source_system_id=selected.get("source_system_id"),
            owner_period_id=selected.get("parcel_owner_period_id"),
            confidence_score=float(selected.get("confidence_score") or 0),
            override_flag=False,
            metadata_json=metadata_json,
        )

    if cad_owner_name:
        confidence_score, reasons = score_owner_period(
            source_basis="cad_owner_snapshot",
            linked_parcel=True,
            has_recording_date=False,
            grantee_count=0,
            has_instrument_number=False,
        )
        return CurrentOwnerRollupCandidate(
            owner_name=cad_owner_name,
            owner_name_normalized=cad_owner_name_normalized or normalize_owner_name(cad_owner_name),
            owner_names_json=[cad_owner_name],
            mailing_address=None,
            mailing_city=None,
            mailing_state="TX",
            mailing_zip=None,
            source_basis="cad_owner_snapshot",
            source_record_hash=None,
            source_system_id=cad_source_system_id,
            owner_period_id=None,
            confidence_score=confidence_score,
            override_flag=False,
            metadata_json={"score_reasons": reasons, "cad_owner_name": cad_owner_name},
        )

    return None
