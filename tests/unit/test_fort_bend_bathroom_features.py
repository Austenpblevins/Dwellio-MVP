from __future__ import annotations

import csv

from app.services.fort_bend_bathroom_features import (
    FORT_BEND_BATHROOM_NORMALIZATION_RULE_VERSION,
    FortBendBathroomFeatureService,
)


def test_derive_feature_record_exact_supported() -> None:
    service = FortBendBathroomFeatureService()

    record = service._derive_feature_record(
        county_id="fort_bend",
        tax_year=2026,
        account_to_parcel_id={"100": "parcel-1"},
        source_file_version="segments:sha256:test",
        source_file_name="WebsiteResidentialSegs.csv",
        selected_candidate={
            "account_number": "100",
            "quick_ref_id": "R100",
            "improvement_num": "1",
            "source_row_count": 1,
            "plumbing_values": {2.0},
            "half_bath_values": {1.0},
            "quarter_bath_values": set(),
            "invalid_negative_fields": set(),
        },
    )

    assert record.parcel_id == "parcel-1"
    assert record.full_baths_derived == 2.0
    assert record.half_baths_derived == 1.0
    assert record.quarter_baths_derived == 0.0
    assert record.bathroom_equivalent_derived == 2.5
    assert record.bathroom_count_status == "exact_supported"
    assert record.bathroom_count_confidence == "high"
    assert record.normalization_rule_version == FORT_BEND_BATHROOM_NORMALIZATION_RULE_VERSION


def test_derive_feature_record_reconciles_fractional_plumbing() -> None:
    service = FortBendBathroomFeatureService()

    record = service._derive_feature_record(
        county_id="fort_bend",
        tax_year=2026,
        account_to_parcel_id={},
        source_file_version="segments:sha256:test",
        source_file_name="WebsiteResidentialSegs.csv",
        selected_candidate={
            "account_number": "100",
            "quick_ref_id": "R100",
            "improvement_num": "1",
            "source_row_count": 1,
            "plumbing_values": {2.5},
            "half_bath_values": set(),
            "quarter_bath_values": {0.0},
            "invalid_negative_fields": set(),
        },
    )

    assert record.full_baths_derived == 2.0
    assert record.half_baths_derived == 1.0
    assert record.bathroom_equivalent_derived == 2.5
    assert record.bathroom_count_status == "reconciled_fractional_plumbing"
    assert "fractional_plumbing_source" in record.bathroom_flags
    assert "half_bath_imputed_from_fractional_plumbing" in record.bathroom_flags


def test_derive_feature_record_keeps_quarter_bath_explicit() -> None:
    service = FortBendBathroomFeatureService()

    record = service._derive_feature_record(
        county_id="fort_bend",
        tax_year=2026,
        account_to_parcel_id={},
        source_file_version="segments:sha256:test",
        source_file_name="WebsiteResidentialSegs.csv",
        selected_candidate={
            "account_number": "100",
            "quick_ref_id": "R100",
            "improvement_num": "1",
            "source_row_count": 1,
            "plumbing_values": {3.0},
            "half_bath_values": {1.0},
            "quarter_bath_values": {1.0},
            "invalid_negative_fields": set(),
        },
    )

    assert record.full_baths_derived == 3.0
    assert record.half_baths_derived == 1.0
    assert record.quarter_baths_derived == 1.0
    assert record.bathroom_equivalent_derived == 3.75
    assert record.bathroom_count_status == "quarter_bath_present"
    assert record.bathroom_count_confidence == "medium"
    assert "quarter_bath_present" in record.bathroom_flags


def test_derive_feature_record_marks_multiple_plumbing_values_ambiguous() -> None:
    service = FortBendBathroomFeatureService()

    record = service._derive_feature_record(
        county_id="fort_bend",
        tax_year=2026,
        account_to_parcel_id={},
        source_file_version="segments:sha256:test",
        source_file_name="WebsiteResidentialSegs.csv",
        selected_candidate={
            "account_number": "100",
            "quick_ref_id": "R100",
            "improvement_num": "1",
            "source_row_count": 2,
            "plumbing_values": {2.0, 3.0},
            "half_bath_values": {1.0},
            "quarter_bath_values": set(),
            "invalid_negative_fields": set(),
        },
    )

    assert record.full_baths_derived is None
    assert record.bathroom_count_status == "ambiguous_bathroom_count"
    assert record.bathroom_count_confidence == "low"
    assert "multiple_plumbing_values" in record.bathroom_flags


def test_derive_feature_record_marks_no_source_when_candidate_lacks_bath_data() -> None:
    service = FortBendBathroomFeatureService()

    record = service._derive_feature_record(
        county_id="fort_bend",
        tax_year=2026,
        account_to_parcel_id={},
        source_file_version="segments:sha256:test",
        source_file_name="WebsiteResidentialSegs.csv",
        selected_candidate={
            "account_number": "100",
            "quick_ref_id": "R100",
            "improvement_num": "1",
            "source_row_count": 1,
            "plumbing_values": set(),
            "half_bath_values": set(),
            "quarter_bath_values": set(),
            "invalid_negative_fields": set(),
        },
    )

    assert record.full_baths_derived is None
    assert record.half_baths_derived is None
    assert record.quarter_baths_derived is None
    assert record.bathroom_equivalent_derived is None
    assert record.bathroom_count_status == "no_bathroom_source"
    assert record.bathroom_count_confidence == "none"


def test_build_feature_rows_emits_no_source_row_for_accessory_only_source_present_account(
    tmp_path,
) -> None:
    source_path = tmp_path / "WebsiteResidentialSegs.csv"
    with source_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["QuickRefID", "PropertyNumber", "vTSGRSeg_ImpNum", "fSegType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "QuickRefID": "QR1",
                "PropertyNumber": "100",
                "vTSGRSeg_ImpNum": "2",
                "fSegType": "RP",
            }
        )
        writer.writerow(
            {
                "QuickRefID": "QR1",
                "PropertyNumber": "100",
                "vTSGRSeg_ImpNum": "2",
                "fSegType": "SPA",
            }
        )

    service = FortBendBathroomFeatureService()
    rows = service._build_feature_rows(
        county_id="fort_bend",
        tax_year=2026,
        source_path=source_path,
        source_file_version="WebsiteResidentialSegs.csv:sha256:test",
        account_to_parcel_id={"100": "parcel-1"},
    )

    assert len(rows) == 1
    record = rows[0]
    assert record.parcel_id == "parcel-1"
    assert record.selected_improvement_number == "2"
    assert record.selected_improvement_source_row_count == 2
    assert record.bathroom_count_status == "no_bathroom_source"
    assert record.bathroom_count_confidence == "none"
    assert "source_present_without_characteristic_segment" in record.bathroom_flags
    assert "selected_improvement_without_characteristic_segment" in record.bathroom_flags
