from __future__ import annotations

from app.services.public_owner import build_public_owner_summary


def test_build_public_owner_summary_masks_individual_names() -> None:
    summary = build_public_owner_summary("Alex Jordan Example", confidence_score=0.61)

    assert summary.display_name == "A. J. Example"
    assert summary.owner_type == "individual"
    assert summary.privacy_mode == "masked_individual_name"
    assert summary.confidence_label == "medium"


def test_build_public_owner_summary_preserves_entity_names() -> None:
    summary = build_public_owner_summary("Oak Meadow Holdings LLC", confidence_score=0.92)

    assert summary.display_name == "Oak Meadow Holdings LLC"
    assert summary.owner_type == "entity"
    assert summary.privacy_mode == "public_entity_name"
    assert summary.confidence_label == "high"
