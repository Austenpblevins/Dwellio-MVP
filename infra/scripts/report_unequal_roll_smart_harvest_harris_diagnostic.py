from __future__ import annotations

import argparse
import csv
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

INPUT_CONTRACT = {
    "script_mode": "enriched_artifact_reporter",
    "full_diagnostic_generator": False,
    "supported_primary_input": "enriched_smart_harvest_diagnostic_json_or_clarification_wrapper",
    "supports_original_harris_artifact_directly": False,
    "optional_reference_input": "original_harris_smart_harvest_artifact_for_reconciliation_only",
    "notes": [
        "This script is review-only and no-persist.",
        "It reports from an already-enriched diagnostic artifact rather than generating a fresh Harris replay.",
        "A clarification wrapper artifact is supported when it includes a source_artifact path to the richer case-level diagnostic JSON.",
        "When a source Harris artifact is available, it is used only for neighborhood total reconciliation.",
    ],
}

FIELD_LINEAGE_OVERRIDES = {
    "land_sf": {
        "field_label": "land_sf",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Available end-to-end in current unequal-roll runtime and review evidence.",
    },
    "land_acres": {
        "field_label": "land_acres",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Available end-to-end in current unequal-roll runtime and review evidence.",
    },
    "bedrooms": {
        "field_label": "bedrooms",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated today, but remains non-monetized.",
    },
    "full_baths": {
        "field_label": "full_baths",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated as resolved bath counts.",
    },
    "half_baths": {
        "field_label": "half_baths",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated as resolved bath counts.",
    },
    "effective_bath_count": {
        "field_label": "effective_bath_count",
        "canonical_db_source_available": False,
        "parcel_summary_view_available": False,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": True,
        "final_value_review_evidence_available": False,
        "lineage_note": "Derived in code from full and half baths for review/scoring logic; not persisted as its own field.",
    },
    "effective_age": {
        "field_label": "effective_age",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": False,
        "lineage_note": "Available through runtime but not surfaced in current review-evidence export.",
    },
    "year_built": {
        "field_label": "year_built",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": False,
        "lineage_note": "Available through runtime but not surfaced in current review-evidence export.",
    },
    "garage_spaces": {
        "field_label": "garage_spaces",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Available upstream and in review evidence, but dropped before unequal-roll subject snapshot persistence and candidate discovery payloads.",
    },
    "frontage": {
        "field_label": "frontage_sf",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": False,
        "lineage_note": "Available upstream, but currently dropped before subject snapshot persistence, candidate payloads, and review evidence.",
    },
    "depth": {
        "field_label": "depth_sf",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": False,
        "lineage_note": "Available upstream, but currently dropped before subject snapshot persistence, candidate payloads, and review evidence.",
    },
    "pool": {
        "field_label": "pool_flag",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated, with monetary adjustment support but not main similarity scoring.",
    },
    "stories": {
        "field_label": "stories",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated end-to-end.",
    },
    "quality": {
        "field_label": "quality_code",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated end-to-end.",
    },
    "condition": {
        "field_label": "condition_code",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated end-to-end.",
    },
    "subdivision": {
        "field_label": "subdivision_name",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated end-to-end.",
    },
    "property_class": {
        "field_label": "property_class_code",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": True,
        "unequal_roll_subject_snapshots_column_available": True,
        "snapshot_json_available": True,
        "candidate_discovery_payload_available": True,
        "scoring_input_available": True,
        "final_value_review_evidence_available": True,
        "lineage_note": "Persisted and propagated end-to-end.",
    },
    "geometry_availability": {
        "field_label": "geometry_availability",
        "canonical_db_source_available": True,
        "parcel_summary_view_available": False,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": False,
        "lineage_note": "Geometry flags are available in subject-source query logic but not persisted or propagated into unequal-roll payloads.",
    },
    "market_value_per_sf": {
        "field_label": "market_value_per_sf",
        "canonical_db_source_available": False,
        "parcel_summary_view_available": False,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": False,
        "lineage_note": "Not currently materialized as a runtime field; would need to be derived for review-only use.",
    },
    "appraised_value_per_sf": {
        "field_label": "appraised_value_per_sf",
        "canonical_db_source_available": False,
        "parcel_summary_view_available": False,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Candidate-side appraised value per SF is available in final-value output; subject-side analog persists as subject_appraised_psf.",
    },
    "adjusted_value_per_sf": {
        "field_label": "adjusted_value_per_sf",
        "canonical_db_source_available": False,
        "parcel_summary_view_available": False,
        "unequal_roll_subject_snapshots_column_available": False,
        "snapshot_json_available": False,
        "candidate_discovery_payload_available": False,
        "scoring_input_available": False,
        "final_value_review_evidence_available": True,
        "lineage_note": "Derived candidate-side only in final-value output and downstream diagnostics.",
    },
}


FIELD_AUDIT_SPECS = [
    {
        "feature": "land_sf",
        "subject_keys": ["land_sf"],
        "candidate_keys": ["land_sf"],
        "review_keys": ["land_sf", "subject_land_sf", "land_sf_delta"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:398",
            "app/services/unequal_roll_candidate_discovery.py:253",
            "app/services/unequal_roll_smart_harvest.py:201",
            "app/services/unequal_roll_final_value.py:298",
        ],
    },
    {
        "feature": "land_acres",
        "subject_keys": ["land_acres"],
        "candidate_keys": ["land_acres"],
        "review_keys": ["land_acres", "subject_land_acres", "land_acres_delta"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:399",
            "app/services/unequal_roll_candidate_discovery.py:254",
            "app/services/unequal_roll_smart_harvest.py:205",
            "app/services/unequal_roll_final_value.py:299",
        ],
    },
    {
        "feature": "bedrooms",
        "subject_keys": ["bedrooms"],
        "candidate_keys": ["bedrooms"],
        "review_keys": ["bedrooms"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:389",
            "app/services/unequal_roll_candidate_discovery.py:247",
            "app/services/unequal_roll_candidate_scoring.py:54",
            "app/services/unequal_roll_candidate_adjustment_math.py:685",
        ],
    },
    {
        "feature": "full_baths",
        "subject_keys": ["full_baths"],
        "candidate_keys": ["full_baths"],
        "review_keys": ["full_baths"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": False,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:390",
            "app/services/unequal_roll_candidate_discovery.py:248",
            "app/services/unequal_roll_candidate_adjustment_support.py:641",
            "app/services/unequal_roll_candidate_adjustment_math.py:549",
        ],
    },
    {
        "feature": "half_baths",
        "subject_keys": ["half_baths"],
        "candidate_keys": ["half_baths"],
        "review_keys": ["half_baths"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": False,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:391",
            "app/services/unequal_roll_candidate_discovery.py:249",
            "app/services/unequal_roll_candidate_adjustment_support.py:653",
            "app/services/unequal_roll_candidate_adjustment_math.py:617",
        ],
    },
    {
        "feature": "effective_bath_count",
        "subject_keys": ["effective_bath_count"],
        "candidate_keys": ["effective_bath_count"],
        "review_keys": [],
        "present_in_subject_snapshot": False,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_smart_harvest.py:137",
            "app/services/unequal_roll_smart_harvest.py:204",
        ],
    },
    {
        "feature": "effective_age",
        "subject_keys": ["effective_age"],
        "candidate_keys": ["effective_age"],
        "review_keys": ["effective_age"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": False,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:388",
            "app/services/unequal_roll_candidate_discovery.py:246",
            "app/services/unequal_roll_smart_harvest.py:121",
            "app/services/unequal_roll_candidate_adjustment_math.py:505",
        ],
    },
    {
        "feature": "year_built",
        "subject_keys": ["year_built"],
        "candidate_keys": ["year_built"],
        "review_keys": ["year_built"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:387",
            "app/services/unequal_roll_candidate_discovery.py:245",
            "app/services/unequal_roll_smart_harvest.py:117",
        ],
    },
    {
        "feature": "garage_spaces",
        "subject_keys": ["garage_spaces"],
        "candidate_keys": ["garage_spaces"],
        "review_keys": ["garage_spaces"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": False,
        "unused": True,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:396",
            "app/services/unequal_roll_candidate_discovery.py:236",
            "infra/scripts/build_unequal_roll_model_review_evidence.py:595",
        ],
    },
    {
        "feature": "frontage",
        "subject_keys": ["frontage_sf"],
        "candidate_keys": ["frontage_sf"],
        "review_keys": ["frontage_sf"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": False,
        "unused": True,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:400",
            "app/services/unequal_roll_candidate_discovery.py:236",
        ],
    },
    {
        "feature": "depth",
        "subject_keys": ["depth_sf"],
        "candidate_keys": ["depth_sf"],
        "review_keys": ["depth_sf"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": False,
        "unused": True,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:401",
            "app/services/unequal_roll_candidate_discovery.py:236",
        ],
    },
    {
        "feature": "pool",
        "subject_keys": ["pool_flag"],
        "candidate_keys": ["pool_flag"],
        "review_keys": ["pool_flag"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": False,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:397",
            "app/services/unequal_roll_candidate_discovery.py:252",
            "app/services/unequal_roll_candidate_adjustment_math.py:771",
        ],
    },
    {
        "feature": "stories",
        "subject_keys": ["stories"],
        "candidate_keys": ["stories"],
        "review_keys": ["stories"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:393",
            "app/services/unequal_roll_candidate_discovery.py:251",
            "app/services/unequal_roll_candidate_scoring.py:63",
            "app/services/unequal_roll_candidate_adjustment_math.py:740",
        ],
    },
    {
        "feature": "quality",
        "subject_keys": ["quality_code"],
        "candidate_keys": ["quality_code"],
        "review_keys": ["quality_code"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:394",
            "app/services/unequal_roll_candidate_discovery.py:250",
            "app/services/unequal_roll_candidate_scoring.py:72",
            "app/services/unequal_roll_candidate_adjustment_math.py:803",
        ],
    },
    {
        "feature": "condition",
        "subject_keys": ["condition_code"],
        "candidate_keys": ["condition_code"],
        "review_keys": ["condition_code"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": True,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:395",
            "app/services/unequal_roll_candidate_discovery.py:251",
            "app/services/unequal_roll_candidate_scoring.py:77",
            "app/services/unequal_roll_candidate_adjustment_math.py:841",
        ],
    },
    {
        "feature": "subdivision",
        "subject_keys": ["subdivision_name"],
        "candidate_keys": ["subdivision_name"],
        "review_keys": ["subdivision_name", "subject_subdivision_name"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:383",
            "app/services/unequal_roll_candidate_discovery.py:242",
            "app/services/unequal_roll_candidate_scoring.py:91",
            "infra/scripts/build_unequal_roll_model_review_evidence.py:585",
        ],
    },
    {
        "feature": "property_class",
        "subject_keys": ["property_class_code"],
        "candidate_keys": ["property_class_code"],
        "review_keys": ["property_class_code"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": True,
        "present_in_scoring_inputs": True,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:381",
            "app/services/unequal_roll_candidate_discovery.py:244",
            "app/services/unequal_roll_candidate_scoring.py:67",
            "infra/scripts/build_unequal_roll_model_review_evidence.py:587",
        ],
    },
    {
        "feature": "geometry_availability",
        "subject_keys": ["has_parcel_polygon", "has_parcel_centroid"],
        "candidate_keys": ["has_parcel_polygon", "has_parcel_centroid"],
        "review_keys": ["has_parcel_polygon", "has_parcel_centroid"],
        "present_in_subject_snapshot": True,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": False,
        "unused": True,
        "code_refs": [
            "app/services/unequal_roll_subject_snapshot.py:418",
            "app/services/unequal_roll_candidate_discovery.py:236",
        ],
    },
    {
        "feature": "market_value_per_sf",
        "subject_keys": ["market_value_per_sf"],
        "candidate_keys": ["market_value_per_sf"],
        "review_keys": ["market_value_per_sf"],
        "present_in_subject_snapshot": False,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": False,
        "monetized_adjustment": False,
        "scoring_review_only": False,
        "unused": True,
        "code_refs": [
            "app/services/unequal_roll_candidate_discovery.py:255",
            "app/services/unequal_roll_final_value.py:280",
        ],
    },
    {
        "feature": "appraised_value_per_sf",
        "subject_keys": ["appraised_value_per_sf"],
        "candidate_keys": ["raw_appraised_value_per_sf", "appraised_value_per_sf"],
        "review_keys": ["raw_appraised_value_per_sf", "appraised_value_per_sf"],
        "present_in_subject_snapshot": False,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_final_value.py:297",
            "app/services/unequal_roll_candidate_adjustment_math.py:1254",
        ],
    },
    {
        "feature": "adjusted_value_per_sf",
        "subject_keys": ["adjusted_value_per_sf"],
        "candidate_keys": ["adjusted_appraised_value_per_sf", "adjusted_value_per_sf"],
        "review_keys": ["adjusted_appraised_value_per_sf", "adjusted_value_per_sf"],
        "present_in_subject_snapshot": False,
        "present_in_candidate_discovery_payload": False,
        "present_in_scoring_inputs": False,
        "present_in_final_value_review_evidence_output": True,
        "monetized_adjustment": False,
        "scoring_review_only": True,
        "unused": False,
        "code_refs": [
            "app/services/unequal_roll_final_value.py:295",
            "app/services/unequal_roll_candidate_adjustment_math.py:1256",
        ],
    },
]

def classify_feature_posture(spec: dict[str, Any]) -> str:
    lineage = resolve_lineage(spec)
    if spec.get("monetized_adjustment"):
        if lineage.get("scoring_input_available"):
            return "monetized_adjustment_and_scoring"
        return "monetized_adjustment"
    if spec.get("scoring_review_only"):
        if lineage.get("candidate_discovery_payload_available") and not spec.get("unused"):
            return "scoring_and_non_monetized_guardrail"
        return "scoring_review_only_derived"
    if spec.get("unused") and (
        lineage.get("canonical_db_source_available")
        or lineage.get("parcel_summary_view_available")
        or lineage.get("present_in_subject_snapshot")
    ) and not lineage.get("candidate_discovery_payload_available"):
        return "unavailable_missing_from_candidate_payload"
    if spec.get("unused"):
        return "unused_or_review_only_observational"
    return "review_only_observational"


def resolve_lineage(spec: dict[str, Any]) -> dict[str, Any]:
    override = FIELD_LINEAGE_OVERRIDES.get(str(spec.get("feature") or ""), {})
    lineage = {
        **spec,
        **override,
    }
    lineage["present_in_subject_snapshot"] = bool(
        lineage.get("unequal_roll_subject_snapshots_column_available")
        or lineage.get("snapshot_json_available")
    )
    return lineage


FEATURE_USAGE_POSTURE = [
    {
        "feature": resolve_lineage(spec)["field_label"],
        "posture": classify_feature_posture(spec),
        "code_refs": spec["code_refs"],
    }
    for spec in FIELD_AUDIT_SPECS
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate durable review-only Harris smart-harvest reporting from an enriched "
            "diagnostic artifact or a clarification wrapper that points to one. This script "
            "does not run a fresh smart-harvest replay."
        )
    )
    parser.add_argument(
        "--input-artifact",
        type=Path,
        required=True,
        help=(
            "Path to an enriched diagnostic JSON artifact with per-case comparison, feature, "
            "fairness, harm, and alternative sections, or to a clarification wrapper whose "
            "source_artifact points to that richer case-level JSON."
        ),
    )
    parser.add_argument(
        "--harris-reference-artifact",
        type=Path,
        default=None,
        help=(
            "Optional original Harris smart-harvest artifact used only for neighborhood total "
            "reconciliation."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    return parser


def build_payload(
    *,
    diagnostic_artifact: dict[str, Any],
    harris_reference_artifact: dict[str, Any] | None,
    input_resolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_copy = deepcopy(diagnostic_artifact)
    cases = list(source_copy.get("cases") or [])
    priority_loss_cases = [
        case for case in cases if str(case.get("cohort_role") or "") == "priority_taxpayer_loss"
    ]
    compact_rows = [build_compact_subject_row(case) for case in priority_loss_cases]
    field_audit = build_data_availability_payload_propagation_audit(priority_loss_cases)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "input_contract": {
            **deepcopy(INPUT_CONTRACT),
            **dict(input_resolution or {}),
        },
        "source_artifact": source_copy.get("source_artifact"),
        "input_artifact_generated_at": source_copy.get("generated_at"),
        "guardrails": build_guardrail_summary(),
        "cohort_summary": build_cohort_summary(source_copy),
        "neighborhood_total_reconciliation": reconcile_neighborhood_totals(
            priority_loss_cases=priority_loss_cases,
            harris_reference_artifact=harris_reference_artifact,
        ),
        "per_subject_compact_evidence_table": compact_rows,
        "current_vs_similarity_top_100_included_comp_comparison": [
            build_included_comp_comparison_row(case) for case in priority_loss_cases
        ],
        "feature_mismatch_summary": build_feature_mismatch_summary(priority_loss_cases),
        "value_fairness_value_per_sf_outlier_summary": build_value_fairness_summary(
            priority_loss_cases
        ),
        "lower_value_equally_credible_alternative_table": [
            format_lower_value_alternative_row(case) for case in priority_loss_cases
        ],
        "feature_usage_posture_table": list(FEATURE_USAGE_POSTURE),
        "data_availability_payload_propagation_audit": field_audit,
        "coverage_missingness_summary": build_coverage_missingness_summary(field_audit),
        "finding_buckets": build_finding_buckets(priority_loss_cases),
        "recommendation_summary": build_recommendation_summary(source_copy, priority_loss_cases),
    }
    return payload


def build_guardrail_summary() -> dict[str, Any]:
    return {
        "db_writes_occurred": False,
        "runtime_defaults_changed": False,
        "smart_harvest_became_default": False,
        "tie_break_automation_enabled": False,
        "scoring_or_adjustment_formulas_changed": False,
        "workflow": "no_persist_review_only",
    }


def build_cohort_summary(diagnostic_artifact: dict[str, Any]) -> dict[str, Any]:
    summary = dict(diagnostic_artifact.get("summary") or {})
    return {
        "cases_reviewed": summary.get("cases_reviewed", 0),
        "priority_taxpayer_loss_cases": summary.get("priority_taxpayer_loss_cases", 0),
        "positive_control_cases": summary.get("positive_control_cases", 0),
        "stable_control_cases": summary.get("stable_control_cases", 0),
        "harm_category_counts": dict(summary.get("harm_category_counts") or {}),
        "tiebreak_class_counts": dict(summary.get("tiebreak_class_counts") or {}),
    }


def reconcile_neighborhood_totals(
    *,
    priority_loss_cases: list[dict[str, Any]],
    harris_reference_artifact: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    for case in priority_loss_cases:
        neighborhood = str(case.get("neighborhood_code") or "")
        totals[neighborhood] = round(
            totals.get(neighborhood, 0.0)
            + float(case.get("artifact_reduction_change_amount") or 0.0),
            2,
        )

    rows: list[dict[str, Any]] = []
    for neighborhood in sorted(totals):
        row = {
            "neighborhood_code": neighborhood,
            "diagnostic_loss_only_total": totals[neighborhood],
            "prior_net_total": None,
            "prior_loss_component": None,
            "difference_explained_by_gains_or_non_loss_cases": None,
            "reconciliation_status": "loss_only_total_without_reference_artifact",
        }
        if harris_reference_artifact is not None:
            summary = (
                (harris_reference_artifact.get("summary") or {})
                .get("neighborhood_summary", {})
                .get(neighborhood, {})
            )
            prior_net = _as_float(summary.get("net"))
            prior_loss = _as_float(summary.get("loss"))
            row.update(
                {
                    "prior_net_total": prior_net,
                    "prior_loss_component": prior_loss,
                    "difference_explained_by_gains_or_non_loss_cases": None
                    if prior_net is None or prior_loss is None
                    else round(prior_net - prior_loss, 2),
                    "reconciliation_status": "reconciled_against_harris_reference",
                }
            )
        rows.append(row)
    return rows


def build_compact_subject_row(case: dict[str, Any]) -> dict[str, Any]:
    current_result = dict(case.get("current_result") or {})
    smart_result = dict(case.get("smart_result") or {})
    comparison = dict(case.get("comparison_summary") or {})
    delta = dict(
        (case.get("value_fairness_outlier_report") or {}).get("delta_smart_minus_current") or {}
    )
    harm = dict(case.get("smart_harvest_harm_explanation") or {})
    feature_summary = dict((case.get("feature_mismatch_report") or {}).get("summary") or {})
    ordered_signals: list[str] = []
    for value in feature_summary.get("features_where_smart_is_closer") or []:
        if value not in ordered_signals:
            ordered_signals.append(str(value))
    for value in feature_summary.get("features_where_smart_is_farther") or []:
        if value not in ordered_signals:
            ordered_signals.append(str(value))
    return {
        "subject_account": case.get("account"),
        "neighborhood_code": case.get("neighborhood_code"),
        "current_included_comp_count": current_result.get("included_comp_count"),
        "smart_included_comp_count": smart_result.get("included_comp_count"),
        "overlap_count": comparison.get("comp_overlap_count"),
        "adjusted_median_delta": comparison.get("adjusted_median_change"),
        "appraised_value_per_sf_delta": delta.get("median_appraised_value_per_sf"),
        "adjusted_value_per_sf_delta": delta.get("median_adjusted_value_per_sf"),
        "primary_harm_classification": harm.get("primary_explanation_category"),
        "top_3_feature_mismatch_signals": ordered_signals[:3],
        "attribution_risk_labels": build_attribution_risk_labels(case),
        "should_remain_gated_manual_review_only": harm.get(
            "should_case_remain_gated_manual_review_only"
        ),
    }


def build_included_comp_comparison_row(case: dict[str, Any]) -> dict[str, Any]:
    comparison = dict(case.get("comparison_summary") or {})
    current_result = dict(case.get("current_result") or {})
    smart_result = dict(case.get("smart_result") or {})
    return {
        "subject_account": case.get("account"),
        "neighborhood_code": case.get("neighborhood_code"),
        "comp_overlap_count": comparison.get("comp_overlap_count"),
        "comps_removed_by_smart_harvest": list(
            comparison.get("comps_removed_by_smart_harvest") or []
        ),
        "comps_added_by_smart_harvest": list(
            comparison.get("comps_added_by_smart_harvest") or []
        ),
        "included_comp_count_change": comparison.get("included_comp_count_change"),
        "review_heavy_count_change": comparison.get("review_heavy_count_change"),
        "likely_exclude_count_change": comparison.get("likely_exclude_count_change"),
        "status_change": dict(comparison.get("status_change") or {}),
        "support_status_change": dict(comparison.get("support_status_change") or {}),
        "requested_reduction_change": comparison.get("requested_reduction_change"),
        "adjusted_median_change": comparison.get("adjusted_median_change"),
        "current_final_status": current_result.get("final_value_status"),
        "smart_final_status": smart_result.get("final_value_status"),
    }


def build_feature_mismatch_summary(priority_loss_cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: dict[str, int] = {}
    farther_counter: dict[str, int] = {}
    for case in priority_loss_cases:
        summary = dict((case.get("feature_mismatch_report") or {}).get("summary") or {})
        for feature in summary.get("features_where_smart_is_closer") or []:
            counter[str(feature)] = counter.get(str(feature), 0) + 1
        for feature in summary.get("features_where_smart_is_farther") or []:
            farther_counter[str(feature)] = farther_counter.get(str(feature), 0) + 1
    rows = []
    for feature, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        rows.append(
            {
                "feature": feature,
                "closer_case_count": count,
                "farther_case_count": farther_counter.get(feature, 0),
            }
        )
    return rows


def build_value_fairness_summary(priority_loss_cases: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [
        dict((case.get("value_fairness_outlier_report") or {}).get("delta_smart_minus_current") or {})
        for case in priority_loss_cases
    ]
    return {
        "average_delta_similarity_score": _avg(
            [row.get("avg_similarity_score") for row in deltas]
        ),
        "average_delta_median_adjusted_value": _avg(
            [row.get("median_adjusted_value") for row in deltas]
        ),
        "average_delta_median_appraised_value_per_sf": _avg(
            [row.get("median_appraised_value_per_sf") for row in deltas]
        ),
        "average_delta_median_adjusted_value_per_sf": _avg(
            [row.get("median_adjusted_value_per_sf") for row in deltas]
        ),
    }


def format_lower_value_alternative_row(case: dict[str, Any]) -> dict[str, Any]:
    alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})
    accepted = list(alternative.get("accepted_alternatives") or [])
    rejected = list(alternative.get("rejected_alternatives_sample") or [])
    selected = accepted[0] if accepted else (rejected[0] if rejected else {})
    smart_median = (
        (case.get("value_fairness_outlier_report") or {}).get("smart_included", {}) or {}
    ).get("median_adjusted_value")
    accepted_reason = selected.get("reason_accepted_as_equally_credible")
    rejected_reason = selected.get("reason_rejected")
    return {
        "subject_account": case.get("account"),
        "neighborhood_code": case.get("neighborhood_code"),
        "selected_smart_included_comp_median_adjusted_value": smart_median,
        "count_lower_value_equally_credible_alternatives": alternative.get(
            "count_lower_value_equally_credible_alternatives"
        ),
        "top_candidate_account_ids": list(alternative.get("top_candidate_account_ids") or []),
        "candidate_account_id": selected.get("account_number"),
        "candidate_similarity_score": selected.get("similarity_score"),
        "candidate_adjusted_value": selected.get("adjusted_value"),
        "value_difference_vs_smart_selected_median": selected.get(
            "value_difference_vs_selected_smart_comp_median"
        ),
        "accepted_or_rejected_reason": accepted_reason
        or (", ".join(rejected_reason) if isinstance(rejected_reason, list) else rejected_reason),
        "classification": alternative.get("opportunity_class"),
    }


def build_data_availability_payload_propagation_audit(
    priority_loss_cases: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    subject_contexts = [_subject_context(case) for case in priority_loss_cases]
    candidate_rows = _all_candidate_rows(priority_loss_cases)
    review_rows = _all_review_rows(priority_loss_cases)
    rows: list[dict[str, Any]] = []
    for spec in FIELD_AUDIT_SPECS:
        lineage = resolve_lineage(spec)
        subject_present = _count_records_with_any_key(subject_contexts, spec["subject_keys"])
        candidate_present = _count_records_with_any_key(candidate_rows, spec["candidate_keys"])
        review_present = _count_records_with_any_key(review_rows, spec["review_keys"])
        subject_total = len(subject_contexts)
        candidate_total = len(candidate_rows)
        review_total = len(review_rows)
        subject_cov = _coverage_pct(subject_present, subject_total)
        candidate_cov = _coverage_pct(candidate_present, candidate_total)
        review_cov = _coverage_pct(review_present, review_total)
        max_cov = max(subject_cov or 0.0, candidate_cov or 0.0, review_cov or 0.0)
        no_observed_rows = (subject_total + candidate_total + review_total) == 0
        too_sparse = max_cov < 0.5 if not no_observed_rows else True
        enough_review = max_cov >= 0.5
        enough_scoring = (
            lineage["candidate_discovery_payload_available"]
            and lineage["scoring_input_available"]
            and (candidate_cov or 0.0) >= 0.8
        )
        not_usable_yet = (
            bool(spec.get("unused"))
            or not lineage.get("candidate_discovery_payload_available")
            or no_observed_rows
            or too_sparse
        )
        rows.append(
            {
                "feature": lineage["field_label"],
                "canonical_db_source_available": lineage["canonical_db_source_available"],
                "parcel_summary_view_available": lineage["parcel_summary_view_available"],
                "unequal_roll_subject_snapshots_column_available": lineage[
                    "unequal_roll_subject_snapshots_column_available"
                ],
                "snapshot_json_available": lineage["snapshot_json_available"],
                "present_in_subject_snapshot": lineage["present_in_subject_snapshot"],
                "candidate_discovery_payload_available": lineage[
                    "candidate_discovery_payload_available"
                ],
                "scoring_input_available": lineage["scoring_input_available"],
                "final_value_review_evidence_available": lineage[
                    "final_value_review_evidence_available"
                ],
                "enriched_diagnostic_artifact_available": (
                    subject_present > 0 or candidate_present > 0 or review_present > 0
                ),
                "monetized_adjustment": spec["monetized_adjustment"],
                "scoring_review_only": spec["scoring_review_only"],
                "unused": spec["unused"],
                "unavailable": not lineage["candidate_discovery_payload_available"]
                and not lineage["scoring_input_available"],
                "too_sparse_to_use_safely": too_sparse,
                "enriched_subject_present_count": subject_present,
                "enriched_subject_missing_count": max(0, subject_total - subject_present),
                "enriched_subject_coverage_pct": subject_cov,
                "enriched_candidate_present_count": candidate_present,
                "enriched_candidate_missing_count": max(0, candidate_total - candidate_present),
                "enriched_candidate_coverage_pct": candidate_cov,
                "enriched_review_present_count": review_present,
                "enriched_review_missing_count": max(0, review_total - review_present),
                "enriched_review_coverage_pct": review_cov,
                "enough_for_review_only_use": enough_review,
                "enough_for_scoring_experiments": enough_scoring,
                "not_usable_yet": not_usable_yet,
                "posture": classify_feature_posture(spec),
                "code_refs": list(spec["code_refs"]),
                "lineage_note": lineage["lineage_note"],
            }
        )
    return rows


def build_coverage_missingness_summary(
    field_audit_rows: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    return {
        "review_only_usable_fields": [
            row
            for row in field_audit_rows
            if row["enough_for_review_only_use"] and not row["not_usable_yet"]
        ],
        "scoring_experiment_ready_fields": [
            row for row in field_audit_rows if row["enough_for_scoring_experiments"]
        ],
        "not_usable_yet_fields": [
            row for row in field_audit_rows if row["not_usable_yet"] or row["too_sparse_to_use_safely"]
        ],
    }


def build_attribution_risk_labels(case: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    harm = dict(case.get("smart_harvest_harm_explanation") or {})
    unadjusted = {
        str(value)
        for value in harm.get("which_unadjusted_features_most_explain_loss") or []
    }
    delta = dict(
        (case.get("value_fairness_outlier_report") or {}).get("delta_smart_minus_current") or {}
    )
    appraised_psf_delta = _as_float(delta.get("median_appraised_value_per_sf")) or 0.0
    adjusted_psf_delta = _as_float(delta.get("median_adjusted_value_per_sf")) or 0.0

    if appraised_psf_delta > 0 or adjusted_psf_delta > 0:
        labels.append("possible_price_tier_drift")
    if "land_sf" in unadjusted or "land_acres" in unadjusted:
        labels.append("land_signal_not_causal")
        labels.append("possible_micro_location_proxy")
    if "bedrooms" in unadjusted:
        labels.append("bedroom_signal_not_causal")
    if any(feature in unadjusted for feature in ("garage_spaces", "frontage", "depth")):
        labels.append("payload_gap_limits_explanation")
    if not labels and harm.get("primary_explanation_category") == "lost_lower_value_but_still_credible_comps":
        labels.append("possible_micro_location_proxy")
    if "land_signal_not_causal" in labels or "bedroom_signal_not_causal" in labels:
        labels.append("payload_gap_limits_explanation")
    return sorted(set(labels))


def build_finding_buckets(priority_loss_cases: list[dict[str, Any]]) -> dict[str, list[str]]:
    feature_counter: dict[str, int] = {}
    labeled_cases = 0
    for case in priority_loss_cases:
        for feature in (
            case.get("smart_harvest_harm_explanation", {}) or {}
        ).get("which_unadjusted_features_most_explain_loss", []):
            feature_counter[str(feature)] = feature_counter.get(str(feature), 0) + 1
        if build_attribution_risk_labels(case):
            labeled_cases += 1
    ranked = sorted(feature_counter.items(), key=lambda item: (-item[1], item[0]))
    top_text = ", ".join(f"{feature} ({count})" for feature, count in ranked[:4])
    return {
        "evidence_backed_findings": [
            "This report is derived from a prior no-persist diagnostic artifact and does not execute runtime code paths that write to the database.",
            "Priority Harris loss cases still show higher adjusted medians alongside higher similarity under smart harvest.",
            "Garage, frontage, and depth remain unavailable in the current candidate payload even though subject or offline evidence paths can surface some of them elsewhere.",
        ],
        "heuristic_findings": [
            f"Heuristic ranking of likely missing review dimensions is led by {top_text}."
            if top_text
            else "Heuristic ranking of likely missing review dimensions is unavailable for the supplied artifact.",
            f"Attribution-risk labels were applied to {labeled_cases} priority loss cases as review-only interpretation aids.",
        ],
        "hypotheses_requiring_more_validation": [
            "Subdivision micro-location and value-tier drift may explain part of the Harris harm pattern better than broad physical mismatch alone.",
            "Land and bedroom signals may be proxies for valuation tier or micro-location rather than direct causal adjustment gaps.",
        ],
    }


def build_recommendation_summary(
    diagnostic_artifact: dict[str, Any],
    priority_loss_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    summary = dict(diagnostic_artifact.get("summary") or {})
    return {
        "keep_analysis_only": True,
        "script_mode": INPUT_CONTRACT["script_mode"],
        "keep_similarity_top_100_gated_in_harris": True,
        "add_review_only_feature_mismatch_and_value_fairness_reporting": True,
        "add_review_only_data_availability_payload_gap_audit": True,
        "exclude_specific_harris_neighborhoods_from_broader_validation": [
            neighborhood
            for neighborhood, row in sorted(
                ((summary.get("neighborhood_summary") or {}).items()),
                key=lambda item: item[0],
            )
            if int((row or {}).get("priority_loss_cases") or 0) > 0
        ],
        "priority_loss_case_count": len(priority_loss_cases),
    }


def parse_artifact(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def infer_harris_reference_artifact(
    *, input_artifact: dict[str, Any], explicit_path: Path | None
) -> dict[str, Any] | None:
    if explicit_path is not None:
        return parse_artifact(explicit_path)
    source_path = input_artifact.get("source_harris_artifact") or input_artifact.get(
        "source_artifact"
    )
    if isinstance(source_path, str):
        path = Path(source_path)
        if path.exists():
            return parse_artifact(path)
    return None


def resolve_diagnostic_artifact(input_artifact: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if isinstance(input_artifact.get("cases"), list):
        return input_artifact, {
            "input_artifact_kind": "enriched_case_level_diagnostic",
            "resolved_case_level_artifact_via_source_pointer": False,
        }

    source_path = input_artifact.get("source_artifact")
    if isinstance(source_path, str):
        path = Path(source_path)
        if path.exists():
            resolved = parse_artifact(path)
            if isinstance(resolved.get("cases"), list):
                return resolved, {
                    "input_artifact_kind": "clarification_wrapper",
                    "resolved_case_level_artifact_via_source_pointer": True,
                    "wrapper_generated_at": input_artifact.get("generated_at"),
                    "wrapper_source_artifact": source_path,
                }

    raise ValueError(
        "Input artifact must be a case-level enriched diagnostic JSON, or a clarification "
        "wrapper whose source_artifact points to one."
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "subject_account",
        "neighborhood_code",
        "current_included_comp_count",
        "smart_included_comp_count",
        "overlap_count",
        "adjusted_median_delta",
        "appraised_value_per_sf_delta",
        "adjusted_value_per_sf_delta",
        "primary_harm_classification",
        "top_3_feature_mismatch_signals",
        "attribution_risk_labels",
        "should_remain_gated_manual_review_only",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            payload["top_3_feature_mismatch_signals"] = ";".join(
                payload.get("top_3_feature_mismatch_signals") or []
            )
            payload["attribution_risk_labels"] = ";".join(
                payload.get("attribution_risk_labels") or []
            )
            writer.writerow(payload)


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Harris Smart Harvest Review-Only Diagnostic",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Source artifact: `{payload.get('source_artifact')}`",
        f"- Input contract mode: `{payload['input_contract']['script_mode']}`",
        f"- Supports original Harris artifact directly: `{str(payload['input_contract']['supports_original_harris_artifact_directly']).lower()}`",
        f"- Cases reviewed: `{payload['cohort_summary']['cases_reviewed']}`",
        f"- Priority loss cases: `{payload['cohort_summary']['priority_taxpayer_loss_cases']}`",
        f"- DB writes occurred: `{str(payload['guardrails']['db_writes_occurred']).lower()}`",
        f"- Runtime defaults changed: `{str(payload['guardrails']['runtime_defaults_changed']).lower()}`",
        f"- Smart harvest became default: `{str(payload['guardrails']['smart_harvest_became_default']).lower()}`",
        f"- Tie-break automation enabled: `{str(payload['guardrails']['tie_break_automation_enabled']).lower()}`",
        f"- Scoring/adjustment formulas changed: `{str(payload['guardrails']['scoring_or_adjustment_formulas_changed']).lower()}`",
        "",
        "## Neighborhood Reconciliation",
    ]
    for row in payload["neighborhood_total_reconciliation"]:
        lines.append(
            f"- `{row['neighborhood_code']}`: diagnostic_loss_only `{row['diagnostic_loss_only_total']}`, "
            f"prior_net `{row['prior_net_total']}`, prior_loss `{row['prior_loss_component']}`, "
            f"gain_offset `{row['difference_explained_by_gains_or_non_loss_cases']}`"
        )
    lines += ["", "## Data Availability Audit"]
    for row in payload["data_availability_payload_propagation_audit"]:
        lines.append(
            f"- `{row['feature']}`: subject_snapshot `{str(row['present_in_subject_snapshot']).lower()}`, "
            f"candidate_payload `{str(row['candidate_discovery_payload_available']).lower()}`, "
            f"enriched_subject_cov `{row['enriched_subject_coverage_pct']}`, "
            f"enriched_candidate_cov `{row['enriched_candidate_coverage_pct']}`, "
            f"enriched_review_cov `{row['enriched_review_coverage_pct']}`, "
            f"review_only `{str(row['enough_for_review_only_use']).lower()}`, "
            f"scoring_ready `{str(row['enough_for_scoring_experiments']).lower()}`, "
            f"not_usable_yet `{str(row['not_usable_yet']).lower()}`"
        )
    lines += ["", "## Evidence vs Heuristic vs Hypothesis"]
    for bucket, values in payload["finding_buckets"].items():
        lines.append(f"### {bucket.replace('_', ' ').title()}")
        for value in values:
            lines.append(f"- {value}")
    lines += ["", "## Recommendation Summary"]
    for key, value in payload["recommendation_summary"].items():
        lines.append(f"- `{key}`: {json.dumps(value)}")
    path.write_text("\n".join(lines))


def _subject_context(case: dict[str, Any]) -> dict[str, Any]:
    for key in ("subject_features", "subject_snapshot", "subject_context", "subject_summary"):
        value = case.get(key)
        if isinstance(value, dict):
            return value
    for container_key in ("current_result", "smart_result"):
        container = case.get(container_key)
        if isinstance(container, dict):
            for key in ("subject_features", "subject_snapshot", "subject_context", "subject_summary"):
                value = container.get(key)
                if isinstance(value, dict):
                    return value
    return {}


def _all_candidate_rows(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case in cases:
        for key in (
            "current_candidate_rows",
            "smart_candidate_rows",
            "current_included_comp_rows",
            "smart_included_comp_rows",
        ):
            rows.extend(_normalize_rows(case.get(key)))
        for container_key in ("current_result", "smart_result"):
            container = case.get(container_key)
            if isinstance(container, dict):
                for key in ("candidate_rows", "included_comp_rows", "review_visible_comp_rows"):
                    rows.extend(_normalize_rows(container.get(key)))
    return rows


def _all_review_rows(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case in cases:
        for key in ("current_included_comp_rows", "smart_included_comp_rows"):
            rows.extend(_normalize_rows(case.get(key)))
        for container_key in ("current_result", "smart_result"):
            container = case.get(container_key)
            if isinstance(container, dict):
                for key in ("included_comp_rows", "review_visible_comp_rows", "excluded_comp_rows"):
                    rows.extend(_normalize_rows(container.get(key)))
    return rows


def _normalize_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _count_records_with_any_key(records: list[dict[str, Any]], keys: list[str]) -> int:
    if not keys:
        return 0
    count = 0
    for record in records:
        nested_sources = [
            record,
            dict(record.get("source_features") or {}),
            dict(record.get("subject_features") or {}),
            dict(record.get("feature_diffs") or {}),
            dict(record.get("neighborhood_value_context") or {}),
        ]
        if any(_is_present(source.get(key)) for source in nested_sources for key in keys):
            count += 1
    return count


def _coverage_pct(present_count: int, total_count: int) -> float | None:
    if total_count <= 0:
        return None
    return round(present_count / total_count, 4)


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def _avg(values: list[Any]) -> float | None:
    clean = [_as_float(value) for value in values if _as_float(value) is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    args = build_parser().parse_args()
    input_artifact = parse_artifact(args.input_artifact)
    resolved_artifact, input_resolution = resolve_diagnostic_artifact(input_artifact)
    harris_reference_artifact = infer_harris_reference_artifact(
        input_artifact=input_artifact,
        explicit_path=args.harris_reference_artifact,
    )
    payload = build_payload(
        diagnostic_artifact=resolved_artifact,
        harris_reference_artifact=harris_reference_artifact,
        input_resolution=input_resolution,
    )
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    prefix = args.output_dir / f"unequal_roll_smart_harvest_harris_diagnostic_{timestamp}"
    json_path = Path(f"{prefix}.json")
    csv_path = Path(f"{prefix}.csv")
    md_path = Path(f"{prefix}.md")
    json_path.write_text(json.dumps(payload, indent=2))
    write_csv(csv_path, payload["per_subject_compact_evidence_table"])
    write_md(md_path, payload)
    print(
        json.dumps(
            {
                "json": str(json_path),
                "csv": str(csv_path),
                "md": str(md_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
