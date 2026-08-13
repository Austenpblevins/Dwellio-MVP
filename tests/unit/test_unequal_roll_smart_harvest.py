from __future__ import annotations

from app.services.unequal_roll_smart_harvest import (
    CURRENT_ORDER_CAP_100,
    DYNAMIC_CAP_150,
    SIMILARITY_TOP_100,
    cheap_same_neighborhood_similarity_score,
    select_same_neighborhood_harvest,
)


def _subject() -> dict[str, object]:
    return {
        "county_id": "fort_bend",
        "subdivision_name": "Oak Meadows",
        "property_class_code": "A1",
        "living_area_sf": 2000.0,
        "year_built": 2004,
        "effective_age": 6.0,
        "bedrooms": 4,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "stories": 2.0,
        "land_sf": 7200.0,
        "land_acres": 0.1653,
    }


def _row(
    account_number: str,
    *,
    subdivision_name: str = "Oak Meadows",
    living_area_sf: float = 2000.0,
    year_built: int = 2004,
    effective_age: float = 6.0,
    bedrooms: int = 4,
    full_baths: float = 2.0,
    half_baths: float = 1.0,
    stories: float = 2.0,
    land_sf: float = 7200.0,
    property_class_code: str = "A1",
) -> dict[str, object]:
    return {
        "account_number": account_number,
        "subdivision_name": subdivision_name,
        "living_area_sf": living_area_sf,
        "year_built": year_built,
        "effective_age": effective_age,
        "bedrooms": bedrooms,
        "full_baths": full_baths,
        "half_baths": half_baths,
        "stories": stories,
        "land_sf": land_sf,
        "land_acres": land_sf / 43560.0,
        "property_class_code": property_class_code,
    }


def test_similarity_prefers_closer_candidate() -> None:
    subject = _subject()
    close = _row("A", living_area_sf=2010.0, land_sf=7250.0)
    far = _row("B", subdivision_name="Other", living_area_sf=2600.0, land_sf=12000.0)

    assert cheap_same_neighborhood_similarity_score(
        subject_snapshot=subject,
        row=close,
    ) > cheap_same_neighborhood_similarity_score(subject_snapshot=subject, row=far)


def test_select_same_neighborhood_harvest_similarity_reorders_candidates() -> None:
    subject = _subject()
    rows = [
        _row("300", living_area_sf=2800.0, subdivision_name="Other"),
        _row("100", living_area_sf=2005.0),
        _row("200", living_area_sf=2010.0),
    ]

    selection = select_same_neighborhood_harvest(
        subject_snapshot=subject,
        same_neighborhood_rows=rows,
        strategy=SIMILARITY_TOP_100,
    )

    assert [row["account_number"] for row in selection.selected_rows[:2]] == ["100", "200"]
    assert selection.scored_universe[0]["account_number"] == "100"


def test_select_same_neighborhood_harvest_dynamic_cap_expands_large_universe() -> None:
    subject = _subject()
    rows = [_row(f"{i:03d}") for i in range(320)]

    selection = select_same_neighborhood_harvest(
        subject_snapshot=subject,
        same_neighborhood_rows=rows,
        strategy=DYNAMIC_CAP_150,
    )

    assert selection.cap_used == 150
    assert selection.selected_count == 150
    assert selection.excluded_by_cap == 170


def test_select_same_neighborhood_harvest_current_strategy_preserves_order() -> None:
    subject = _subject()
    rows = [_row(f"{i:03d}") for i in range(105)]

    selection = select_same_neighborhood_harvest(
        subject_snapshot=subject,
        same_neighborhood_rows=rows,
        strategy=CURRENT_ORDER_CAP_100,
    )

    assert selection.cap_used == 100
    assert selection.selected_count == 100
    assert [row["account_number"] for row in selection.selected_rows[:3]] == ["000", "001", "002"]
