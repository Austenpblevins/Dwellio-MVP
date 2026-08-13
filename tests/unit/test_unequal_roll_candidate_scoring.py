from __future__ import annotations

from app.services.unequal_roll_candidate_scoring import compute_similarity_score


def _subject(*, county_id: str = "harris") -> dict[str, object]:
    return {
        "county_id": county_id,
        "neighborhood_code": "NBH-001",
        "subdivision_name": "Heights",
        "living_area_sf": 2000.0,
        "bedrooms": 4,
        "stories": 2.0,
        "property_class_code": "A1",
        "quality_code": "B" if county_id == "harris" else "0",
        "condition_code": "GOOD" if county_id == "harris" else "0",
    }


def _candidate(*, county_id: str = "harris") -> dict[str, object]:
    return {
        "county_id": county_id,
        "neighborhood_code": "NBH-001",
        "subdivision_name": "Heights",
        "living_area_sf": 2050.0,
        "bedrooms": 4,
        "stories": 2.0,
        "property_class_code": "A1",
        "quality_code": "B" if county_id == "harris" else "0",
        "condition_code": "GOOD" if county_id == "harris" else "0",
    }


def test_similarity_score_produces_component_level_detail() -> None:
    result = compute_similarity_score(
        subject_snapshot=_subject(),
        row=_candidate(),
        discovery_tier="same_neighborhood",
        eligibility_status="eligible",
        eligibility_detail_json={"primary_reason_code": None, "secondary_reason_codes": []},
        valuation_bathroom_features_json=None,
    )

    assert result.raw_similarity_score > 0
    assert result.normalized_similarity_score > 0
    assert result.score_detail_json["components"]["locality"]["weighted_points"] > 0
    assert result.score_detail_json["components"]["living_area_similarity"]["weighted_points"] > 0


def test_similarity_score_applies_review_status_penalty() -> None:
    eligible_result = compute_similarity_score(
        subject_snapshot=_subject(),
        row=_candidate(),
        discovery_tier="same_neighborhood",
        eligibility_status="eligible",
        eligibility_detail_json={"primary_reason_code": None, "secondary_reason_codes": []},
        valuation_bathroom_features_json=None,
    )
    review_result = compute_similarity_score(
        subject_snapshot=_subject(),
        row=_candidate(),
        discovery_tier="same_neighborhood",
        eligibility_status="review",
        eligibility_detail_json={
            "primary_reason_code": "wide_living_area_gap",
            "secondary_reason_codes": [],
        },
        valuation_bathroom_features_json=None,
    )

    assert review_result.raw_similarity_score < eligible_result.raw_similarity_score
    assert review_result.score_detail_json["eligibility_status_multiplier"]["value"] < 1.0


def test_similarity_score_applies_harris_quality_and_property_class_influence() -> None:
    result = compute_similarity_score(
        subject_snapshot=_subject(county_id="harris"),
        row={
            **_candidate(county_id="harris"),
            "property_class_code": "A4",
            "quality_code": "C",
            "condition_code": "AVERAGE",
        },
        discovery_tier="same_neighborhood",
        eligibility_status="review",
        eligibility_detail_json={
            "primary_reason_code": "property_class_adjacent_family",
            "secondary_reason_codes": ["quality_adjacent", "condition_adjacent"],
        },
        valuation_bathroom_features_json=None,
    )

    assert (
        result.score_detail_json["components"]["property_class_relation"]["property_class_relation"]
        == "adjacent_family"
    )
    assert result.score_detail_json["components"]["quality_similarity"]["quality_gap_steps"] == 1
    assert result.score_detail_json["components"]["condition_similarity"]["condition_gap_steps"] == 1


def test_similarity_score_applies_fort_bend_numeric_quality_condition_influence() -> None:
    result = compute_similarity_score(
        subject_snapshot=_subject(county_id="fort_bend"),
        row={
            **_candidate(county_id="fort_bend"),
            "quality_code": "1",
            "condition_code": "1",
        },
        discovery_tier="same_neighborhood",
        eligibility_status="review",
        eligibility_detail_json={
            "primary_reason_code": "quality_adjacent",
            "secondary_reason_codes": ["condition_adjacent"],
        },
        valuation_bathroom_features_json=None,
    )

    assert result.score_detail_json["components"]["quality_similarity"]["quality_gap_steps"] == 1
    assert result.score_detail_json["components"]["condition_similarity"]["condition_gap_steps"] == 1


def test_similarity_score_applies_conservative_fort_bend_bathroom_modifier_without_coercing_baths() -> None:
    usable_result = compute_similarity_score(
        subject_snapshot=_subject(county_id="fort_bend"),
        row=_candidate(county_id="fort_bend"),
        discovery_tier="same_neighborhood",
        eligibility_status="eligible",
        eligibility_detail_json={"primary_reason_code": None, "secondary_reason_codes": []},
        valuation_bathroom_features_json={
            "attachment_status": "attached",
            "bathroom_count_status": "exact_supported",
            "bathroom_count_confidence": "high",
        },
    )
    review_result = compute_similarity_score(
        subject_snapshot=_subject(county_id="fort_bend"),
        row=_candidate(county_id="fort_bend"),
        discovery_tier="same_neighborhood",
        eligibility_status="review",
        eligibility_detail_json={
            "primary_reason_code": "fort_bend_bathroom_status_review",
            "secondary_reason_codes": [],
        },
        valuation_bathroom_features_json={
            "attachment_status": "attached",
            "bathroom_count_status": "ambiguous_multi_improvement",
            "bathroom_count_confidence": "low",
        },
    )

    assert review_result.raw_similarity_score < usable_result.raw_similarity_score
    assert (
        review_result.score_detail_json["fort_bend_bathroom_modifier"]["review_required"] is True
    )
