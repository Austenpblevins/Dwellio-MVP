from __future__ import annotations

from app.services.exemption_normalization import (
    normalize_exemption_type_code,
    normalize_parcel_exemptions,
)


def test_normalize_parcel_exemptions_preserves_raw_codes_and_merges_aliases() -> None:
    normalized = normalize_parcel_exemptions(
        [
            {
                "exemption_type_code": "homestead",
                "raw_exemption_code": "HS",
                "exemption_amount": 100000,
            },
            {"exemption_type_code": "HS", "raw_exemption_code": "HS_AMT", "exemption_amount": 5000},
            {
                "exemption_type_code": "over65",
                "raw_exemption_code": "OV65",
                "exemption_amount": 10000,
            },
        ]
    )

    assert [item["exemption_type_code"] for item in normalized] == ["homestead", "over65"]

    homestead = normalized[0]
    assert homestead["exemption_amount"] == 105000
    assert homestead["raw_exemption_codes"] == ["HS", "HS_AMT"]
    assert homestead["source_entry_count"] == 2
    assert homestead["amount_missing_flag"] is False

    over65 = normalized[1]
    assert over65["raw_exemption_codes"] == ["OV65"]


def test_normalize_parcel_exemptions_marks_missing_amount_when_granted() -> None:
    normalized = normalize_parcel_exemptions(
        [
            {
                "exemption_type_code": "freeze",
                "raw_exemption_code": "CEILING",
                "exemption_amount": None,
            },
        ]
    )

    assert normalized == [
        {
            "exemption_type_code": "freeze_ceiling",
            "exemption_amount": None,
            "granted_flag": True,
            "raw_exemption_codes": ["CEILING"],
            "source_entry_count": 1,
            "amount_missing_flag": True,
        }
    ]


def test_normalize_exemption_type_code_handles_known_aliases() -> None:
    assert normalize_exemption_type_code("hs_amt") == "homestead"
    assert normalize_exemption_type_code("OV65") == "over65"
    assert normalize_exemption_type_code("disabled-vet") == "disabled_veteran"


def test_normalize_parcel_exemptions_uses_county_dictionary_and_splits_composite_codes() -> None:
    normalized = normalize_parcel_exemptions(
        [
            {
                "raw_exemption_code": "RES VTX",
                "exemption_amount": None,
            }
        ],
        county_id="harris",
    )

    assert [item["exemption_type_code"] for item in normalized] == [
        "disabled_veteran",
        "homestead",
    ]
    assert normalized[0]["amount_missing_flag"] is True
    assert normalized[1]["amount_missing_flag"] is True


def test_normalize_parcel_exemptions_preserves_unmapped_code_as_unknown() -> None:
    normalized = normalize_parcel_exemptions(
        [{"raw_exemption_code": "UNSEEN_CODE_123"}],
        county_id="harris",
    )

    assert normalized == [
        {
            "exemption_type_code": "unknown",
            "exemption_amount": None,
            "granted_flag": True,
            "raw_exemption_codes": ["UNSEEN_CODE_123"],
            "source_entry_count": 1,
            "amount_missing_flag": True,
        }
    ]
