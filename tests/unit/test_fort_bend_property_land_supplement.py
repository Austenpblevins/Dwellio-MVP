from __future__ import annotations

from app.ingestion.fort_bend_property_land_supplement import (
    AggregatedSupplementalLand,
    CanonicalLandRow,
    PropertyLandSegment,
    apply_fill_only_actions,
    aggregate_property_land_segments,
    build_fill_only_apply_actions,
    build_fill_only_plan,
)


def test_aggregate_property_land_segments_uses_deterministic_primary_and_totals() -> None:
    rows = [
        PropertyLandSegment(
            account_number="A-1",
            quick_ref_id="Q1",
            sequence=2,
            square_feet=1000.0,
            acres=0.02295684,
            frontage_sf=40.0,
            depth_sf=100.0,
        ),
        PropertyLandSegment(
            account_number="A-1",
            quick_ref_id="Q1",
            sequence=1,
            square_feet=1500.0,
            acres=0.03443526,
            frontage_sf=50.0,
            depth_sf=110.0,
        ),
    ]
    aggregated = aggregate_property_land_segments(rows)
    assert "A-1" in aggregated
    result = aggregated["A-1"]
    assert result.segment_count == 2
    assert result.valid_segment_count == 2
    assert result.primary_land_sf == 1500.0
    assert result.total_land_sf == 2500.0
    assert result.primary_frontage_sf == 50.0
    assert result.primary_depth_sf == 110.0


def test_build_fill_only_plan_preserves_conflicts_and_fills_missing() -> None:
    canonical = {
        "A-1": CanonicalLandRow(
            parcel_id="p1",
            account_number="A-1",
            land_sf=2000.0,
            land_acres=0.10,
            frontage_sf=60.0,
            depth_sf=120.0,
        ),
        "A-2": CanonicalLandRow(
            parcel_id="p2",
            account_number="A-2",
            land_sf=None,
            land_acres=None,
            frontage_sf=None,
            depth_sf=None,
        ),
    }
    supplemental = {
        "A-1": AggregatedSupplementalLand(
            account_number="A-1",
            segment_count=1,
            valid_segment_count=1,
            primary_land_sf=1600.0,  # conflict with existing positive value
            primary_land_acres=0.0367,
            primary_frontage_sf=45.0,
            primary_depth_sf=105.0,
            total_land_sf=1600.0,
            total_land_acres=0.0367,
        ),
        "A-2": AggregatedSupplementalLand(
            account_number="A-2",
            segment_count=1,
            valid_segment_count=1,
            primary_land_sf=1800.0,  # fill missing
            primary_land_acres=0.0413,
            primary_frontage_sf=48.0,
            primary_depth_sf=112.0,
            total_land_sf=1800.0,
            total_land_acres=0.0413,
        ),
    }
    plan = build_fill_only_plan(canonical, supplemental)
    assert plan["join_match_accounts"] == 2
    assert plan["fill_counts"]["land_sf"] == 1
    assert plan["preserve_counts"]["land_sf"] == 1
    assert plan["conflict_counts"]["land_sf"] == 1
    assert plan["potential_additional_land_sf_positive_fill_only"] == 1
    assert plan["projected_land_sf_positive_after_fill_only"] == 2
    assert len(plan["conflict_samples"]) == 1
    assert plan["conflict_samples"][0]["account_number"] == "A-1"


def test_build_fill_only_apply_actions_only_targets_fillable_rows() -> None:
    canonical = {
        "A-1": CanonicalLandRow(
            parcel_id="p1",
            account_number="A-1",
            land_sf=2000.0,
            land_acres=0.10,
            frontage_sf=60.0,
            depth_sf=120.0,
        ),
        "A-2": CanonicalLandRow(
            parcel_id="p2",
            account_number="A-2",
            land_sf=None,
            land_acres=None,
            frontage_sf=None,
            depth_sf=None,
        ),
    }
    supplemental = {
        "A-1": AggregatedSupplementalLand(
            account_number="A-1",
            segment_count=1,
            valid_segment_count=1,
            primary_land_sf=1600.0,
            primary_land_acres=0.0367,
            primary_frontage_sf=45.0,
            primary_depth_sf=105.0,
            total_land_sf=1600.0,
            total_land_acres=0.0367,
        ),
        "A-2": AggregatedSupplementalLand(
            account_number="A-2",
            segment_count=1,
            valid_segment_count=1,
            primary_land_sf=1800.0,
            primary_land_acres=0.0413,
            primary_frontage_sf=48.0,
            primary_depth_sf=112.0,
            total_land_sf=1800.0,
            total_land_acres=0.0413,
        ),
    }
    actions = build_fill_only_apply_actions(canonical, supplemental, tax_year=2026)
    assert len(actions) == 1
    action = actions[0]
    assert action.account_number == "A-2"
    assert action.parcel_id == "p2"
    assert action.land_sf == 1800.0
    assert action.land_acres == 0.0413
    assert action.frontage_sf == 48.0
    assert action.depth_sf == 112.0


def test_apply_fill_only_actions_dry_run_is_non_mutating() -> None:
    summary = apply_fill_only_actions(
        [
            build_fill_only_apply_actions(
                {
                    "A-2": CanonicalLandRow(
                        parcel_id="p2",
                        account_number="A-2",
                        land_sf=None,
                        land_acres=None,
                        frontage_sf=None,
                        depth_sf=None,
                    )
                },
                {
                    "A-2": AggregatedSupplementalLand(
                        account_number="A-2",
                        segment_count=1,
                        valid_segment_count=1,
                        primary_land_sf=1800.0,
                        primary_land_acres=0.0413,
                        primary_frontage_sf=48.0,
                        primary_depth_sf=112.0,
                        total_land_sf=1800.0,
                        total_land_acres=0.0413,
                    )
                },
                tax_year=2026,
            )[0]
        ],
        dry_run=True,
    )
    assert summary["dry_run"] is True
    assert summary["row_count"] == 1
    assert summary["mutated_row_count"] == 0
    assert summary["sample_accounts"] == ["A-2"]
