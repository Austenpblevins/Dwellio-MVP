from __future__ import annotations

from app.ingestion.repository import IngestionRepository


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
        self.copy_rows: list[tuple[object, ...]] = []

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        self.queries.append(query)

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
