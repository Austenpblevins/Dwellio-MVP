from __future__ import annotations

from app.services.exemption_code_dictionary import (
    map_raw_exemption_codes,
    split_raw_exemption_code_tokens,
)


def test_split_raw_exemption_code_tokens_handles_composite_values() -> None:
    assert split_raw_exemption_code_tokens(" RES VTX / TOT ") == ["RES", "VTX", "TOT"]


def test_map_raw_exemption_codes_uses_county_dictionary() -> None:
    mapped = map_raw_exemption_codes(county_id="harris", raw_codes=["RES", "V14"])
    canonical = [entry.canonical_exemption_type_code for entry in mapped]
    assert canonical == ["homestead", "disabled_veteran"]
    assert [entry.mapping_status for entry in mapped] == ["exact", "exact"]


def test_map_raw_exemption_codes_marks_unknown_codes_without_dropping() -> None:
    mapped = map_raw_exemption_codes(county_id="fort_bend", raw_codes=["NOREALCODE"])
    assert len(mapped) == 1
    assert mapped[0].canonical_exemption_type_code == "unknown"
    assert mapped[0].mapping_status == "unknown"
    assert mapped[0].raw_exemption_code == "NOREALCODE"


def test_map_raw_exemption_codes_supports_compound_multi_mapping() -> None:
    mapped = map_raw_exemption_codes(county_id="fort_bend", raw_codes=["DVO65", "DVTD", "DP65"])
    canonical = [entry.canonical_exemption_type_code for entry in mapped]
    assert canonical == [
        "disabled_veteran",
        "over65",
        "disabled_veteran",
        "freeze_ceiling",
        "disabled_person",
        "over65",
    ]
