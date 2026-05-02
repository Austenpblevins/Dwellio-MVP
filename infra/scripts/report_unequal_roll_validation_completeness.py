from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.unequal_roll_validation_completeness import (
    classify_subject_output,
    summarize_completeness,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Classify unequal-roll replay outputs so silent unavailable rows become "
            "explicit defect categories for completeness-gated validation."
        )
    )
    parser.add_argument(
        "--artifacts",
        nargs="+",
        required=True,
        help="One or more chunk artifact JSON paths to classify.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON path for the classified completeness report.",
    )
    args = parser.parse_args()

    subject_rows: list[dict[str, Any]] = []
    source_artifacts: list[str] = []

    for artifact in args.artifacts:
        path = Path(artifact)
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_artifacts.append(str(path))
        chunk_number = payload.get("chunk_metadata", {}).get("chunk_number")
        for row in payload.get("subjects", []):
            enriched = dict(row)
            classification = classify_subject_output(enriched)
            enriched["completeness_status_code"] = classification.status_code
            enriched["completeness_status_family"] = classification.status_family
            enriched["completeness_gate_pass"] = classification.completeness_gate_pass
            enriched["completeness_defect_category"] = classification.defect_category
            enriched["missing_required_fields"] = list(
                classification.missing_required_fields
            )
            enriched["source_chunk_number"] = chunk_number
            enriched["source_artifact"] = str(path)
            subject_rows.append(enriched)

    report = {
        "source_artifacts": source_artifacts,
        "summary": summarize_completeness(subject_rows),
        "subjects": subject_rows,
    }
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
