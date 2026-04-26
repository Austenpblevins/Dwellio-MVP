from __future__ import annotations

from app.ingestion.repository import IngestionRepository
from app.services.tax_assignment import MATCH_CONFIDENCE, MATCH_REASON_CODES


def test_capture_property_roll_rollback_manifest_includes_new_accounts_with_none_prior_state(
    monkeypatch,
) -> None:
    repository = IngestionRepository(connection=None)  # type: ignore[arg-type]

    monkeypatch.setattr(repository, "_fetch_rows", lambda query, params: [])

    manifest = repository.capture_property_roll_rollback_manifest(
        county_id="harris",
        tax_year=2024,
        account_numbers=["1001001001001", "1001001001002"],
    )

    assert manifest == {
        "dataset_type": "property_roll",
        "entries": [
            {"account_number": "1001001001001", "prior_state": None},
            {"account_number": "1001001001002", "prior_state": None},
        ],
    }


def test_upsert_property_roll_records_bulk_mode_still_replaces_exemptions(monkeypatch) -> None:
    repository = IngestionRepository(connection=None)  # type: ignore[arg-type]
    core_calls: dict[str, object] = {}
    exemption_calls: dict[str, object] = {}

    monkeypatch.setattr(repository, "fetch_appraisal_district_id", lambda county_id: "district-1")
    monkeypatch.setattr(
        repository,
        "_bulk_upsert_property_roll_core_records",
        lambda **kwargs: core_calls.update(kwargs),
    )
    monkeypatch.setattr(
        repository,
        "_bulk_replace_property_roll_exemptions",
        lambda **kwargs: exemption_calls.update(kwargs),
    )

    normalized_records = [
        {
            "parcel": {
                "account_number": "1001001001001",
                "source_record_hash": "hash-1",
            },
            "exemptions": [{"exemption_type_code": "homestead", "exemption_amount": 100000}],
        }
    ]

    lineage = repository.upsert_property_roll_records(
        county_id="harris",
        tax_year=2026,
        import_batch_id="batch-1",
        job_run_id="job-1",
        source_system_id="source-1",
        normalized_records=normalized_records,
        include_detail_tables=False,
    )

    assert lineage == []
    assert core_calls["county_id"] == "harris"
    assert core_calls["tax_year"] == 2026
    assert core_calls["normalized_records"] == normalized_records
    assert exemption_calls["county_id"] == "harris"
    assert exemption_calls["tax_year"] == 2026
    assert exemption_calls["source_system_id"] == "source-1"
    assert exemption_calls["normalized_records"] == normalized_records


class RecordingCopy:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    def __enter__(self) -> RecordingCopy:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write_row(self, row: tuple[object, ...]) -> None:
        self.rows.append(row)


class RecordingCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params: list[object | None] = []
        self.copy_rows: list[tuple[object, ...]] = []
        self.rowcount = 0
        self._row: dict[str, str] | None = None

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        self.queries.append(query)
        self.params.append(params)
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT appraisal_district_id"):
            self._row = {"appraisal_district_id": "district-1"}
        elif normalized.startswith("INSERT INTO taxing_units"):
            self._row = {"taxing_unit_id": "taxing-unit-1"}
        elif normalized.startswith("INSERT INTO tax_rates"):
            self._row = {"tax_rate_id": "tax-rate-1"}
        else:
            self._row = None

    def copy(self, query: str) -> RecordingCopy:
        self.queries.append(query)
        return RecordingCopy(self.copy_rows)

    def fetchone(self) -> dict[str, str] | None:
        return self._row


class RecordingConnection:
    def __init__(self) -> None:
        self.cursor_instance = RecordingCursor()

    def cursor(self) -> RecordingCursor:
        return self.cursor_instance


def test_bulk_property_roll_upsert_populates_parcel_improvements_summary() -> None:
    connection = RecordingConnection()
    repository = IngestionRepository(connection=connection)  # type: ignore[arg-type]

    repository._bulk_upsert_property_roll_core_records(
        county_id="harris",
        tax_year=2025,
        import_batch_id="batch-1",
        job_run_id="job-1",
        source_system_id="source-1",
        appraisal_district_id="district-1",
        normalized_records=[
            {
                "parcel": {
                    "account_number": "1001001001001",
                    "cad_property_id": "cad-1",
                    "situs_address": "123 Main St",
                    "situs_city": "Houston",
                    "situs_zip": "77001",
                    "owner_name": "Jane Doe",
                    "property_type_code": "sfr",
                    "property_class_code": "A1",
                    "neighborhood_code": "NBHD-1",
                    "subdivision_name": "Oak Creek",
                    "school_district_name": "HISD",
                    "source_record_hash": "hash-1",
                },
                "address": {
                    "normalized_address": "123 MAIN ST HOUSTON TX 77001",
                },
                "characteristics": {
                    "property_type_code": "sfr",
                    "property_class_code": "A1",
                    "neighborhood_code": "NBHD-1",
                    "subdivision_name": "Oak Creek",
                    "school_district_name": "HISD",
                    "homestead_flag": True,
                    "owner_occupied_flag": True,
                    "primary_use_code": "residential",
                    "neighborhood_group": "NBHD-1",
                    "effective_age": 10,
                },
                "improvements": [
                    {
                        "living_area_sf": 2150,
                        "year_built": 2004,
                        "effective_year_built": 2012,
                        "effective_age": 10,
                        "bedrooms": 4,
                        "full_baths": 2,
                        "half_baths": 1,
                        "total_rooms": 8,
                        "stories": 2,
                        "quality_code": "AVG",
                        "condition_code": "GOOD",
                        "garage_spaces": 2,
                        "pool_flag": False,
                    }
                ],
                "assessment": {
                    "land_value": 120000,
                    "improvement_value": 180000,
                    "market_value": 300000,
                    "assessed_value": 300000,
                    "capped_value": 290000,
                    "appraised_value": 300000,
                    "exemption_value_total": 40000,
                    "notice_value": 300000,
                    "certified_value": 295000,
                    "prior_year_market_value": 280000,
                    "prior_year_assessed_value": 270000,
                },
            }
        ],
    )

    joined_queries = "\n".join(connection.cursor_instance.queries)

    assert "INSERT INTO parcel_improvements" in joined_queries
    assert connection.cursor_instance.copy_rows[0][25] == 2150
    assert connection.cursor_instance.copy_rows[0][28] == 2004
    assert connection.cursor_instance.copy_rows[0][34] == 8


def test_bulk_property_roll_upsert_materializes_target_ids_and_skips_unchanged_addresses() -> None:
    connection = RecordingConnection()
    repository = IngestionRepository(connection=connection)  # type: ignore[arg-type]

    repository._bulk_upsert_property_roll_core_records(
        county_id="harris",
        tax_year=2026,
        import_batch_id="batch-1",
        job_run_id="job-1",
        source_system_id="source-1",
        appraisal_district_id="district-1",
        normalized_records=[
            {
                "parcel": {
                    "account_number": "1001001001001",
                    "cad_property_id": "cad-1",
                    "situs_address": "123 Main St",
                    "situs_city": "Houston",
                    "situs_zip": "77001",
                    "owner_name": "Jane Doe",
                    "property_type_code": "sfr",
                    "property_class_code": "A1",
                    "neighborhood_code": "NBHD-1",
                    "subdivision_name": "Oak Creek",
                    "school_district_name": "HISD",
                    "source_record_hash": "hash-1",
                },
                "address": {
                    "normalized_address": "123 MAIN ST HOUSTON TX 77001",
                },
                "characteristics": {
                    "property_type_code": "sfr",
                    "property_class_code": "A1",
                    "neighborhood_code": "NBHD-1",
                    "subdivision_name": "Oak Creek",
                    "school_district_name": "HISD",
                    "homestead_flag": True,
                    "owner_occupied_flag": True,
                    "primary_use_code": "residential",
                    "neighborhood_group": "NBHD-1",
                    "effective_age": 10,
                },
                "improvements": [
                    {
                        "living_area_sf": 2150,
                        "year_built": 2004,
                        "effective_year_built": 2012,
                        "effective_age": 10,
                        "bedrooms": 4,
                        "full_baths": 2,
                        "half_baths": 1,
                        "total_rooms": 8,
                        "stories": 2,
                        "quality_code": "AVG",
                        "condition_code": "GOOD",
                        "garage_spaces": 2,
                        "pool_flag": False,
                    }
                ],
                "assessment": {
                    "land_value": 120000,
                    "improvement_value": 180000,
                    "market_value": 300000,
                    "assessed_value": 300000,
                    "capped_value": 290000,
                    "appraised_value": 300000,
                    "exemption_value_total": 40000,
                    "notice_value": 300000,
                    "certified_value": 295000,
                    "prior_year_market_value": 280000,
                    "prior_year_assessed_value": 270000,
                },
            }
        ],
    )

    joined_queries = "\n".join(connection.cursor_instance.queries)

    assert "CREATE TEMP TABLE IF NOT EXISTS tmp_property_roll_target_parcels" in joined_queries
    assert "CREATE TEMP TABLE IF NOT EXISTS tmp_property_roll_address_changes" in joined_queries
    assert "CREATE TEMP TABLE IF NOT EXISTS tmp_property_roll_target_snapshots" in joined_queries
    assert "LEFT JOIN parcel_addresses pa" in joined_queries
    assert "WHERE pa.parcel_address_id IS NULL" in joined_queries
    assert "FROM tmp_property_roll_address_changes ac" in joined_queries
    assert "JOIN tmp_property_roll_target_parcels tp" in joined_queries
    assert "JOIN tmp_property_roll_target_snapshots ts" in joined_queries
    assert "WHERE parcel_year_snapshots.county_id IS DISTINCT FROM EXCLUDED.county_id" in joined_queries
    assert "WHERE property_characteristics.property_type_code IS DISTINCT FROM EXCLUDED.property_type_code" in joined_queries
    assert "WHERE parcel_improvements.living_area_sf IS DISTINCT FROM EXCLUDED.living_area_sf" in joined_queries
    assert "WHERE parcel_assessments.land_value IS DISTINCT FROM EXCLUDED.land_value" in joined_queries


def test_set_based_tax_assignment_refresh_builds_temp_context_and_bulk_effective_rates() -> None:
    connection = RecordingConnection()
    repository = IngestionRepository(connection=connection)  # type: ignore[arg-type]

    repository.refresh_parcel_tax_assignments_set_based(
        county_id="harris",
        tax_year=2025,
        import_batch_id="batch-1",
        job_run_id="11111111-1111-1111-1111-111111111111",
        source_system_id="source-1",
    )
    repository.refresh_effective_tax_rates(county_id="harris", tax_year=2025)

    joined_queries = "\n".join(connection.cursor_instance.queries)

    assert "CREATE TEMP TABLE tmp_parcel_tax_context" in joined_queries
    assert "CREATE TEMP TABLE tmp_tax_unit_match_hints" in joined_queries
    assert "CREATE TEMP TABLE tmp_ranked_tax_assignments" in joined_queries
    assert "upper(btrim(value.value)) AS candidate_value" in joined_queries
    assert "upper(btrim(hint_value.value))" not in joined_queries
    assert "upper(btrim(fallback_value.value))" not in joined_queries
    assert "INSERT INTO parcel_taxing_units" in joined_queries
    assert "INSERT INTO effective_tax_rates" in joined_queries
    assert "GROUP BY ptu.parcel_id, ptu.tax_year" in joined_queries
    ranked_assignment_params = next(
        params
        for query, params in zip(
            connection.cursor_instance.queries,
            connection.cursor_instance.params,
            strict=False,
        )
        if params is not None and "CREATE TEMP TABLE tmp_ranked_tax_assignments" in query
    )
    assert ranked_assignment_params[5:19] == (
        MATCH_CONFIDENCE["account_numbers"],
        MATCH_REASON_CODES["account_numbers"],
        MATCH_CONFIDENCE["school_district_names"],
        MATCH_REASON_CODES["school_district_names"],
        MATCH_CONFIDENCE["cities"],
        MATCH_REASON_CODES["cities"],
        MATCH_CONFIDENCE["subdivisions"],
        MATCH_REASON_CODES["subdivisions"],
        MATCH_CONFIDENCE["neighborhood_codes"],
        MATCH_REASON_CODES["neighborhood_codes"],
        MATCH_CONFIDENCE["zip_codes"],
        MATCH_REASON_CODES["zip_codes"],
        MATCH_CONFIDENCE["county_ids"],
        MATCH_REASON_CODES["county_ids"],
    )


def test_upsert_tax_rate_records_deletes_stale_rates_for_unit_only_rows() -> None:
    connection = RecordingConnection()
    repository = IngestionRepository(connection=connection)  # type: ignore[arg-type]

    lineage_records = repository.upsert_tax_rate_records(
        county_id="harris",
        tax_year=2026,
        import_batch_id="batch-1",
        job_run_id="job-1",
        source_system_id="source-1",
        normalized_records=[
            {
                "taxing_unit": {
                    "unit_type_code": "school",
                    "unit_code": "A76",
                    "unit_name": "Deferred Unit",
                    "metadata_json": {
                        "rate_bearing_status": "caveated_rate_row_deferred",
                    },
                },
                "tax_rate": None,
                "source_record_hash": "hash-1",
            }
        ],
    )

    delete_params = next(
        params
        for query, params in zip(
            connection.cursor_instance.queries,
            connection.cursor_instance.params,
            strict=False,
        )
        if "DELETE FROM tax_rates" in query
    )

    assert delete_params == ("taxing-unit-1", 2026)
    assert lineage_records == [
        {
            "target_table": "taxing_units",
            "target_id": "taxing-unit-1",
            "taxing_unit_id": "taxing-unit-1",
        }
    ]
