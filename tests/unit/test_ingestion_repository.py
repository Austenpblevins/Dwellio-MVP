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

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        self.queries.append(query)
        self.params.append(params)

    def copy(self, query: str) -> RecordingCopy:
        self.queries.append(query)
        return RecordingCopy(self.copy_rows)


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
    assert connection.cursor_instance.copy_rows[0][26] == 2004


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
