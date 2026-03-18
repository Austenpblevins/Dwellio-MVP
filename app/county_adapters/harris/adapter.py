from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from app.county_adapters.common.base import (
    AcquiredDataset,
    AdapterDataset,
    AdapterMetadata,
    CountyAdapter,
    PublishResult,
    StagingRow,
    ValidationFinding,
)
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.utils.hashing import sha256_text
from app.utils.logging import get_logger
from app.utils.text_normalization import normalize_address_query

logger = get_logger(__name__)

HARRIS_PROPERTY_ROLL_FIXTURE: list[dict[str, Any]] = [
    {
        "account_number": "1001001001001",
        "cad_property_id": "HCAD-1001",
        "situs_address": "101 Main St",
        "situs_city": "Houston",
        "situs_zip": "77002",
        "owner_name": "Alex Example",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "neighborhood_code": "HOU-001",
        "subdivision_name": "Downtown",
        "school_district_name": "Houston ISD",
        "living_area_sf": 2150,
        "year_built": 2004,
        "effective_year_built": 2012,
        "effective_age": 14,
        "bedrooms": 4,
        "full_baths": 2,
        "half_baths": 1,
        "stories": 2,
        "quality_code": "AVG",
        "condition_code": "GOOD",
        "garage_spaces": 2,
        "pool_flag": False,
        "land_sf": 6400,
        "land_acres": 0.1469,
        "land_value": 95000,
        "improvement_value": 255000,
        "market_value": 350000,
        "assessed_value": 330000,
        "notice_value": 360000,
        "appraised_value": 345000,
        "certified_value": None,
        "prior_year_market_value": 340000,
        "prior_year_assessed_value": 320000,
        "exemptions": [
            {"exemption_type_code": "homestead", "exemption_amount": 100000},
        ],
    },
    {
        "account_number": "1001001001002",
        "cad_property_id": "HCAD-1002",
        "situs_address": "202 Oak Ave",
        "situs_city": "Houston",
        "situs_zip": "77008",
        "owner_name": "Jordan Example",
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "neighborhood_code": "HOU-002",
        "subdivision_name": "Heights",
        "school_district_name": "Houston ISD",
        "living_area_sf": 1875,
        "year_built": 1998,
        "effective_year_built": 2008,
        "effective_age": 18,
        "bedrooms": 3,
        "full_baths": 2,
        "half_baths": 0,
        "stories": 1,
        "quality_code": "AVG",
        "condition_code": "AVERAGE",
        "garage_spaces": 1,
        "pool_flag": False,
        "land_sf": 5000,
        "land_acres": 0.1148,
        "land_value": 78000,
        "improvement_value": 210000,
        "market_value": 288000,
        "assessed_value": 270000,
        "notice_value": 292000,
        "appraised_value": 281000,
        "certified_value": None,
        "prior_year_market_value": 281000,
        "prior_year_assessed_value": 266000,
        "exemptions": [
            {"exemption_type_code": "homestead", "exemption_amount": 100000},
            {"exemption_type_code": "over65", "exemption_amount": 10000},
        ],
    },
]


class HarrisCountyAdapter(CountyAdapter):
    county_id = "harris"

    def __init__(self) -> None:
        self.config = load_county_adapter_config(self.county_id)

    def list_available_datasets(self, county_id: str, tax_year: int) -> list[AdapterDataset]:
        if county_id != self.county_id:
            raise ValueError(f"Adapter for {self.county_id} cannot serve county {county_id}.")
        return [
            AdapterDataset(
                dataset_type="property_roll",
                source_system_code="HCAD_BULK",
                tax_year=tax_year,
                description="Fixture-driven Harris property roll dataset for Stage 2 ingestion verification.",
                source_url="https://hcad.org",
            )
        ]

    def acquire_dataset(self, dataset_type: str, tax_year: int) -> AcquiredDataset:
        if dataset_type != "property_roll":
            raise ValueError(f"Harris Stage 2 fixture only supports property_roll, not {dataset_type}.")
        payload = json.dumps(HARRIS_PROPERTY_ROLL_FIXTURE, sort_keys=True).encode("utf-8")
        return AcquiredDataset(
            dataset_type=dataset_type,
            source_system_code="HCAD_BULK",
            tax_year=tax_year,
            original_filename=f"harris-property-roll-{tax_year}.json",
            content=payload,
            media_type="application/json",
            source_url="https://hcad.org",
        )

    def detect_file_format(self, file: AcquiredDataset) -> str:
        if file.original_filename.endswith(".json"):
            return "json"
        raise ValueError(f"Unsupported file format for {file.original_filename}.")

    def parse_raw_to_staging(self, file: AcquiredDataset) -> list[StagingRow]:
        parsed_rows = json.loads(file.content.decode("utf-8"))
        return [
            StagingRow(
                table_name="stg_county_property_raw",
                raw_payload=row,
                row_hash=sha256_text(json.dumps(row, sort_keys=True)),
            )
            for row in parsed_rows
        ]

    def normalize_staging_to_canonical(
        self,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if dataset_type != "property_roll":
            raise ValueError(f"Harris Stage 2 fixture only supports property_roll, not {dataset_type}.")

        property_roll = []
        for row in staging_rows:
            source_record_hash = sha256_text(json.dumps(row, sort_keys=True))
            exemptions = row.get("exemptions", [])
            exemption_total = sum(item["exemption_amount"] for item in exemptions)
            property_roll.append(
                {
                    "parcel": {
                        "account_number": row["account_number"],
                        "cad_property_id": row["cad_property_id"],
                        "situs_address": row["situs_address"],
                        "situs_city": row["situs_city"],
                        "situs_zip": row["situs_zip"],
                        "owner_name": row["owner_name"],
                        "property_type_code": row.get("property_type_code", "sfr"),
                        "property_class_code": row.get("property_class_code"),
                        "neighborhood_code": row.get("neighborhood_code"),
                        "subdivision_name": row.get("subdivision_name"),
                        "school_district_name": row.get("school_district_name"),
                        "source_record_hash": source_record_hash,
                    },
                    "address": {
                        "situs_address": row["situs_address"],
                        "situs_city": row["situs_city"],
                        "situs_zip": row["situs_zip"],
                        "normalized_address": normalize_address_query(
                            f"{row['situs_address']} {row['situs_city']} TX {row['situs_zip']}"
                        ),
                    },
                    "characteristics": {
                        "property_type_code": row.get("property_type_code", "sfr"),
                        "property_class_code": row.get("property_class_code"),
                        "neighborhood_code": row.get("neighborhood_code"),
                        "subdivision_name": row.get("subdivision_name"),
                        "school_district_name": row.get("school_district_name"),
                        "homestead_flag": any(item["exemption_type_code"] == "homestead" for item in exemptions),
                        "owner_occupied_flag": any(item["exemption_type_code"] == "homestead" for item in exemptions),
                        "primary_use_code": "residential",
                        "neighborhood_group": row.get("neighborhood_code"),
                        "effective_age": row.get("effective_age"),
                    },
                    "improvements": [
                        {
                            "building_label": "Main",
                            "living_area_sf": row.get("living_area_sf"),
                            "year_built": row.get("year_built"),
                            "effective_year_built": row.get("effective_year_built"),
                            "effective_age": row.get("effective_age"),
                            "bedrooms": row.get("bedrooms"),
                            "full_baths": row.get("full_baths"),
                            "half_baths": row.get("half_baths"),
                            "stories": row.get("stories"),
                            "quality_code": row.get("quality_code"),
                            "condition_code": row.get("condition_code"),
                            "garage_spaces": row.get("garage_spaces"),
                            "pool_flag": row.get("pool_flag"),
                        }
                    ],
                    "land_segments": [
                        {
                            "segment_num": 1,
                            "land_type_code": "site",
                            "land_sf": row.get("land_sf"),
                            "land_acres": row.get("land_acres"),
                            "market_value": row.get("land_value"),
                        }
                    ],
                    "value_components": [
                        {
                            "component_code": "land",
                            "component_label": "Land Value",
                            "component_category": "market",
                            "market_value": row.get("land_value"),
                            "assessed_value": row.get("land_value"),
                            "taxable_value": row.get("land_value"),
                        },
                        {
                            "component_code": "improvement",
                            "component_label": "Improvement Value",
                            "component_category": "market",
                            "market_value": row.get("improvement_value"),
                            "assessed_value": row.get("improvement_value"),
                            "taxable_value": row.get("improvement_value"),
                        },
                        {
                            "component_code": "market_total",
                            "component_label": "Market Total",
                            "component_category": "market",
                            "market_value": row.get("market_value"),
                            "assessed_value": row.get("assessed_value"),
                            "taxable_value": max((row.get("assessed_value") or 0) - exemption_total, 0),
                        },
                    ],
                    "assessment": {
                        "land_value": row.get("land_value"),
                        "improvement_value": row.get("improvement_value"),
                        "market_value": row.get("market_value"),
                        "assessed_value": row.get("assessed_value"),
                        "capped_value": row.get("assessed_value"),
                        "appraised_value": row.get("appraised_value"),
                        "exemption_value_total": exemption_total,
                        "notice_value": row.get("notice_value"),
                        "certified_value": row.get("certified_value"),
                        "prior_year_market_value": row.get("prior_year_market_value"),
                        "prior_year_assessed_value": row.get("prior_year_assessed_value"),
                    },
                    "exemptions": exemptions,
                }
            )

        return {"property_roll": property_roll}

    def validate_dataset(
        self,
        job_id: str,
        tax_year: int,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> list[ValidationFinding]:
        findings: list[ValidationFinding] = [
            ValidationFinding(
                validation_code="ROW_COUNT_OK",
                message=f"Validated {len(staging_rows)} staging rows for {dataset_type}.",
                validation_scope="staging_row",
                entity_table="stg_county_property_raw",
                details_json={"job_id": job_id, "tax_year": tax_year},
            )
        ]
        for row in staging_rows:
            if not row.get("account_number"):
                findings.append(
                    ValidationFinding(
                        validation_code="MISSING_ACCOUNT_NUMBER",
                        message="Account number is required.",
                        severity="error",
                        validation_scope="staging_row",
                        entity_table="stg_county_property_raw",
                        details_json={"row": row},
                    )
                )
        return findings

    def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
        return PublishResult(
            publish_version=f"harris-{tax_year}-{dataset_type}-{job_id[:8]}",
            details_json={"county_id": self.county_id, "dataset_type": dataset_type},
        )

    def rollback_publish(self, job_id: str) -> None:
        logger.info(
            "harris rollback hook executed",
            extra={"county_id": self.county_id, "job_id": job_id, "external_side_effects": False},
        )
        return None

    def get_adapter_metadata(self) -> AdapterMetadata:
        return AdapterMetadata(
            county_id=self.config.county_id,
            county_name="Harris County",
            appraisal_district_name=self.config.appraisal_district,
            supported_years=[2026],
            supported_dataset_types=["property_roll"],
            known_limitations=[
                "Stage 2 uses fixture data only.",
                "Real Harris parsing and source acquisition are deferred to later stages.",
            ],
            primary_keys_used=["account_number", "cad_property_id"],
            special_parsing_notes="Stage 2 consumes JSON fixture rows and normalizes them into canonical parcel-year tables.",
            manual_fallback_instructions="Use MANUAL_UPLOAD source registration until real county acquisition is implemented.",
        )


def build_harris_fixture_rows() -> list[dict[str, Any]]:
    return [json.loads(json.dumps(row)) for row in HARRIS_PROPERTY_ROLL_FIXTURE]


def harris_metadata_dict() -> dict[str, Any]:
    return asdict(HarrisCountyAdapter().get_adapter_metadata())
