from __future__ import annotations

from datetime import date

from app.services.ownership_reconciliation import (
    build_current_owner_rollup,
    build_normalized_deed_record,
    build_owner_periods,
    normalize_owner_name,
)


def test_build_normalized_deed_record_preserves_linking_metadata() -> None:
    normalized = build_normalized_deed_record(
        county_id="harris",
        row={
            "instrument_number": "INST-1",
            "recording_date": "2026-02-15",
            "account_number": "1001001001001",
            "cad_property_id": "HCAD-1001",
            "alias_values": ["ALT-1"],
            "grantors": [{"name": "Alex Seller"}],
            "grantees": [{"name": "Taylor Buyer", "mailing_address": "101 Main St"}],
        },
    )

    assert normalized["linked_account_number"] == "1001001001001"
    assert normalized["linked_cad_property_id"] == "HCAD-1001"
    assert normalized["deed_record"]["metadata_json"]["linked_aliases"] == ["ALT-1"]
    assert normalized["deed_parties"][0]["party_role"] == "grantor"
    assert normalized["deed_parties"][1]["normalized_name"] == "TAYLOR BUYER"
    assert normalized["source_record_hash"]


def test_build_owner_periods_merges_consecutive_same_owner_records() -> None:
    periods = build_owner_periods(
        parcel_id="parcel-1",
        county_id="harris",
        cad_owner_name="Legacy CAD Owner",
        source_system_id="source-1",
        deed_records=[
            {
                "deed_record_id": "deed-1",
                "parcel_id": "parcel-1",
                "source_system_id": "source-1",
                "instrument_number": "INST-1",
                "effective_date": date(2025, 1, 10),
                "document_type": "Warranty Deed",
                "transfer_type": "arms_length",
                "grantee_summary": "Taylor Buyer",
                "grantee_parties": [{"party_name": "Taylor Buyer"}],
                "mailing_address": "101 Main St",
            },
            {
                "deed_record_id": "deed-2",
                "parcel_id": "parcel-1",
                "source_system_id": "source-1",
                "instrument_number": "INST-2",
                "effective_date": date(2025, 6, 1),
                "document_type": "Correction Deed",
                "transfer_type": "arms_length",
                "grantee_summary": "Taylor Buyer",
                "grantee_parties": [{"party_name": "Taylor Buyer"}],
                "mailing_address": "101 Main St",
            },
            {
                "deed_record_id": "deed-3",
                "parcel_id": "parcel-1",
                "source_system_id": "source-1",
                "instrument_number": "INST-3",
                "effective_date": date(2026, 2, 1),
                "document_type": "Warranty Deed",
                "transfer_type": "arms_length",
                "grantee_summary": "Casey Purchaser",
                "grantee_parties": [{"party_name": "Casey Purchaser"}],
                "mailing_address": "202 Oak Ave",
            },
        ],
    )

    assert len(periods) == 2
    assert periods[0]["owner_name"] == "Taylor Buyer"
    assert periods[0]["start_date"] == date(2025, 1, 10)
    assert periods[0]["end_date"] == date(2026, 1, 31)
    assert periods[0]["is_current"] is False
    assert periods[1]["owner_name"] == "Casey Purchaser"
    assert periods[1]["is_current"] is True
    assert periods[0]["metadata_json"]["supporting_deed_record_ids"] == ["deed-1", "deed-2"]


def test_build_current_owner_rollup_prefers_active_period_over_future_period_and_cad() -> None:
    rollup = build_current_owner_rollup(
        tax_year=2026,
        cad_owner_name="Legacy CAD Owner",
        cad_owner_name_normalized=normalize_owner_name("Legacy CAD Owner"),
        cad_source_system_id="source-1",
        owner_periods=[
            {
                "parcel_owner_period_id": "period-1",
                "owner_name": "Taylor Buyer",
                "owner_name_normalized": "TAYLOR BUYER",
                "start_date": date(2026, 2, 1),
                "end_date": None,
                "source_basis": "deed_grantee",
                "source_system_id": "source-1",
                "confidence_score": 0.9,
                "metadata_json": {"mailing_address": "101 Main St"},
            },
            {
                "parcel_owner_period_id": "period-2",
                "owner_name": "Future Buyer",
                "owner_name_normalized": "FUTURE BUYER",
                "start_date": date(2027, 1, 5),
                "end_date": None,
                "source_basis": "deed_grantee",
                "source_system_id": "source-1",
                "confidence_score": 0.95,
                "metadata_json": {"mailing_address": "999 Future St"},
            },
        ],
        manual_override=None,
    )

    assert rollup is not None
    assert rollup.owner_name == "Taylor Buyer"
    assert rollup.owner_period_id == "period-1"
    assert rollup.source_basis == "deed_grantee"
    assert rollup.override_flag is False


def test_build_current_owner_rollup_uses_manual_override() -> None:
    rollup = build_current_owner_rollup(
        tax_year=2026,
        cad_owner_name="Legacy CAD Owner",
        cad_owner_name_normalized=normalize_owner_name("Legacy CAD Owner"),
        cad_source_system_id="source-1",
        owner_periods=[],
        manual_override={
            "manual_override_id": "override-1",
            "reason": "Verified probate packet",
            "override_payload": {
                "owner_name": "Pat Override",
                "mailing_address": "500 Override Ln",
                "confidence_score": 1.0,
            },
        },
    )

    assert rollup is not None
    assert rollup.owner_name == "Pat Override"
    assert rollup.override_flag is True
    assert rollup.metadata_json["manual_override_id"] == "override-1"
