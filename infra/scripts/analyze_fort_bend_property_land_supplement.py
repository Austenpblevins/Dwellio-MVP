from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

from app.ingestion.fort_bend_property_land_supplement import (
    aggregate_property_land_segments,
    build_fill_only_plan,
    load_fort_bend_2026_canonical_land,
    parse_property_land_e_file,
)


DEFAULT_SOURCE = (
    "/Users/nblevins/county-data/2026/raw/fort_bend/"
    "Fort Bend_Property Data -3-27-2026 - Redacted/PropertyLand-E/"
    "PropertyDataExport4558082.txt"
)


def _write_conflict_csv(path: Path, plan: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "account_number",
                "existing_land_sf",
                "supplemental_land_sf",
                "existing_land_acres",
                "supplemental_land_acres",
                "existing_frontage_sf",
                "supplemental_frontage_sf",
                "existing_depth_sf",
                "supplemental_depth_sf",
            ]
        )
        for sample in plan.get("conflict_samples", []):
            existing = sample.get("existing", {})
            supplemental = sample.get("supplemental", {})
            writer.writerow(
                [
                    sample.get("account_number"),
                    existing.get("land_sf"),
                    supplemental.get("land_sf"),
                    existing.get("land_acres"),
                    supplemental.get("land_acres"),
                    existing.get("frontage_sf"),
                    supplemental.get("frontage_sf"),
                    existing.get("depth_sf"),
                    supplemental.get("depth_sf"),
                ]
            )


def _write_plan_md(path: Path, source_file: str, plan: dict) -> None:
    lines = [
        "# Fort Bend PropertyLand-E Fill-Only Plan",
        "",
        "## Source-Priority Rule",
        "- Preserve existing positive canonical `parcel_lands` values.",
        "- Fill missing/non-positive canonical values from PropertyLand-E only.",
        "- Do not replace existing positive values in this phase.",
        "- Surface conflicts explicitly; no silent overwrite.",
        "",
        f"Source file: `{source_file}`",
        "",
        "## Join + Coverage",
        f"- stage21_accounts_total: {plan['stage21_accounts_total']}",
        f"- supplemental_accounts_total: {plan['supplemental_accounts_total']}",
        f"- join_match_accounts: {plan['join_match_accounts']}",
        f"- join_unmatched_canonical_accounts: {plan['join_unmatched_canonical_accounts']}",
        f"- join_unmatched_supplemental_accounts: {plan['join_unmatched_supplemental_accounts']}",
        "",
        "## Fill / Preserve / Conflict Counts",
        f"- fill_counts: {plan['fill_counts']}",
        f"- preserve_counts: {plan['preserve_counts']}",
        f"- conflict_counts: {plan['conflict_counts']}",
        "",
        "## Land SF Projection (Fill-Only)",
        f"- existing_land_sf_positive: {plan['existing_land_sf_positive']}",
        f"- potential_additional_land_sf_positive_fill_only: {plan['potential_additional_land_sf_positive_fill_only']}",
        f"- projected_land_sf_positive_after_fill_only: {plan['projected_land_sf_positive_after_fill_only']}",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-file", default=DEFAULT_SOURCE)
    parser.add_argument("--output-prefix", default="/private/tmp/unequal_roll_fort_bend_propertyland_e_implementation_validation")
    parser.add_argument("--tax-year", type=int, default=2026)
    args = parser.parse_args()

    source_segments = parse_property_land_e_file(args.source_file)
    aggregated = aggregate_property_land_segments(source_segments)
    canonical = load_fort_bend_2026_canonical_land(tax_year=args.tax_year)
    plan = build_fill_only_plan(canonical, aggregated)

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_json = Path(f"{args.output_prefix}_{stamp}.json")
    output_csv = Path(f"/private/tmp/unequal_roll_fort_bend_propertyland_e_conflicts_{stamp}.csv")
    output_md = Path(f"/private/tmp/unequal_roll_fort_bend_propertyland_e_fill_plan_{stamp}.md")

    payload = {
        "generated_at": datetime.now().isoformat(),
        "source_file": args.source_file,
        "tax_year": args.tax_year,
        "parsed_segment_rows": len(source_segments),
        "aggregated_accounts": len(aggregated),
        "plan": plan,
        "live_rebuild_command_if_approved": (
            "IngestionLifecycleService().normalize("
            "county_id='fort_bend', tax_year=2026, dataset_type='property_roll', "
            "import_batch_id='f6940138-b38f-4250-a457-85ded1d342db', dry_run=False)"
        ),
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _write_conflict_csv(output_csv, plan)
    _write_plan_md(output_md, args.source_file, plan)

    print(output_json)
    print(output_csv)
    print(output_md)


if __name__ == "__main__":
    main()
