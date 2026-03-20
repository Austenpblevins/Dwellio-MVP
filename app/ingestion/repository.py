from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg import Connection
from psycopg import sql
from psycopg.types.json import Jsonb

from app.services.tax_assignment import ParcelTaxAssignment, ParcelTaxContext, TaxingUnitContext


@dataclass(frozen=True)
class ImportBatchRecord:
    import_batch_id: str
    raw_file_id: str
    source_system_id: str
    storage_path: str
    original_filename: str
    file_kind: str
    mime_type: str | None
    file_format: str | None


@dataclass(frozen=True)
class JobRunRecord:
    job_run_id: str
    import_batch_id: str
    raw_file_id: str | None


STAGING_TABLES: dict[str, tuple[str, str]] = {
    "property_roll": ("stg_county_property_raw", "stg_county_property_raw_id"),
    "tax_rates": ("stg_county_tax_rates_raw", "stg_county_tax_rates_raw_id"),
    "sales": ("stg_sales_raw", "stg_sales_raw_id"),
    "gis": ("stg_gis_raw", "stg_gis_raw_id"),
}


class IngestionRepository:
    def __init__(self, connection: Connection[Any]) -> None:
        self.connection = connection

    def fetch_source_system_id(self, source_system_code: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_system_id
                FROM source_systems
                WHERE source_system_code = %s
                """,
                (source_system_code,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Missing source_systems row for {source_system_code}.")
        return str(row["source_system_id"])

    def fetch_appraisal_district_id(self, county_id: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT appraisal_district_id
                FROM appraisal_districts
                WHERE county_id = %s
                ORDER BY district_name
                LIMIT 1
                """,
                (county_id,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Missing appraisal district for county {county_id}.")
        return str(row["appraisal_district_id"])

    def create_import_batch(
        self,
        *,
        source_system_id: str,
        county_id: str,
        tax_year: int,
        source_filename: str,
        source_checksum: str,
        source_url: str | None,
        file_format: str,
        dry_run_flag: bool,
    ) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO import_batches (
                  source_system_id,
                  county_id,
                  tax_year,
                  source_filename,
                  source_checksum,
                  source_url,
                  file_format,
                  status,
                  dry_run_flag
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'created', %s)
                RETURNING import_batch_id
                """,
                (
                    source_system_id,
                    county_id,
                    tax_year,
                    source_filename,
                    source_checksum,
                    source_url,
                    file_format,
                    dry_run_flag,
                ),
            )
            row = cursor.fetchone()
        return str(row["import_batch_id"])

    def register_raw_file(
        self,
        *,
        import_batch_id: str,
        source_system_id: str,
        county_id: str,
        tax_year: int,
        storage_path: str,
        original_filename: str,
        checksum: str,
        mime_type: str,
        size_bytes: int,
        file_kind: str,
        source_url: str | None,
        file_format: str,
    ) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_files (
                  import_batch_id,
                  source_system_id,
                  county_id,
                  tax_year,
                  storage_path,
                  original_filename,
                  checksum,
                  mime_type,
                  size_bytes,
                  file_kind,
                  source_url,
                  file_format
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING raw_file_id
                """,
                (
                    import_batch_id,
                    source_system_id,
                    county_id,
                    tax_year,
                    storage_path,
                    original_filename,
                    checksum,
                    mime_type,
                    size_bytes,
                    file_kind,
                    source_url,
                    file_format,
                ),
            )
            row = cursor.fetchone()
        return str(row["raw_file_id"])

    def create_job_run(
        self,
        *,
        county_id: str,
        tax_year: int,
        job_name: str,
        job_stage: str,
        import_batch_id: str,
        raw_file_id: str | None,
        dry_run_flag: bool,
        metadata_json: dict[str, Any] | None = None,
    ) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO job_runs (
                  county_id,
                  tax_year,
                  job_name,
                  status,
                  import_batch_id,
                  raw_file_id,
                  job_stage,
                  dry_run_flag,
                  metadata_json
                )
                VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s)
                RETURNING job_run_id
                """,
                (
                    county_id,
                    tax_year,
                    job_name,
                    import_batch_id,
                    raw_file_id,
                    job_stage,
                    dry_run_flag,
                    Jsonb(metadata_json or {}),
                ),
            )
            row = cursor.fetchone()
        return str(row["job_run_id"])

    def complete_job_run(
        self,
        job_run_id: str,
        *,
        status: str,
        row_count: int | None = None,
        error_message: str | None = None,
        publish_version: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            if metadata_json is None:
                cursor.execute(
                    """
                    UPDATE job_runs
                    SET
                      status = %s,
                      row_count = COALESCE(%s, row_count),
                      error_message = %s,
                      publish_version = COALESCE(%s, publish_version),
                      finished_at = now(),
                      updated_at = now()
                    WHERE job_run_id = %s
                    """,
                    (
                        status,
                        row_count,
                        error_message,
                        publish_version,
                        job_run_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE job_runs
                    SET
                      status = %s,
                      row_count = COALESCE(%s, row_count),
                      error_message = %s,
                      publish_version = COALESCE(%s, publish_version),
                      metadata_json = %s,
                      finished_at = now(),
                      updated_at = now()
                    WHERE job_run_id = %s
                    """,
                    (
                        status,
                        row_count,
                        error_message,
                        publish_version,
                        Jsonb(metadata_json),
                        job_run_id,
                    ),
                )

    def update_import_batch(
        self,
        import_batch_id: str,
        *,
        status: str,
        row_count: int | None = None,
        error_count: int | None = None,
        publish_state: str | None = None,
        publish_version: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE import_batches
                SET
                  status = %s,
                  row_count = COALESCE(%s, row_count),
                  error_count = COALESCE(%s, error_count),
                  publish_state = COALESCE(%s, publish_state),
                  publish_version = COALESCE(%s, publish_version),
                  completed_at = CASE
                    WHEN %s IN ('fetched', 'staged', 'normalized', 'published', 'failed', 'rolled_back')
                      THEN now()
                    ELSE completed_at
                  END,
                  updated_at = now()
                WHERE import_batch_id = %s
                """,
                (
                    status,
                    row_count,
                    error_count,
                    publish_state,
                    publish_version,
                    status,
                    import_batch_id,
                ),
            )

    def find_import_batch(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        import_batch_id: str | None,
    ) -> ImportBatchRecord:
        with self.connection.cursor() as cursor:
            if import_batch_id is not None:
                cursor.execute(
                    """
                    SELECT
                      ib.import_batch_id,
                      rf.raw_file_id,
                      rf.source_system_id,
                      rf.storage_path,
                      rf.original_filename,
                      rf.file_kind,
                      rf.mime_type,
                      rf.file_format
                    FROM import_batches ib
                    JOIN raw_files rf ON rf.import_batch_id = ib.import_batch_id
                    WHERE ib.import_batch_id = %s
                    ORDER BY rf.created_at DESC
                    LIMIT 1
                    """,
                    (import_batch_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                      ib.import_batch_id,
                      rf.raw_file_id,
                      rf.source_system_id,
                      rf.storage_path,
                      rf.original_filename,
                      rf.file_kind,
                      rf.mime_type,
                      rf.file_format
                    FROM import_batches ib
                    JOIN raw_files rf ON rf.import_batch_id = ib.import_batch_id
                    WHERE ib.county_id = %s
                      AND ib.tax_year = %s
                      AND rf.file_kind = %s
                    ORDER BY ib.created_at DESC, rf.created_at DESC
                    LIMIT 1
                    """,
                    (county_id, tax_year, dataset_type),
                )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(
                f"No import batch found for county={county_id}, tax_year={tax_year}, dataset_type={dataset_type}."
            )
        return ImportBatchRecord(
            import_batch_id=str(row["import_batch_id"]),
            raw_file_id=str(row["raw_file_id"]),
            source_system_id=str(row["source_system_id"]),
            storage_path=row["storage_path"],
            original_filename=row["original_filename"],
            file_kind=row["file_kind"],
            mime_type=row["mime_type"],
            file_format=row["file_format"],
        )

    def find_latest_import_batch_id(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> str | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ib.import_batch_id
                FROM import_batches ib
                JOIN raw_files rf ON rf.import_batch_id = ib.import_batch_id
                WHERE ib.county_id = %s
                  AND ib.tax_year = %s
                  AND rf.file_kind = %s
                ORDER BY ib.created_at DESC, rf.created_at DESC
                LIMIT 1
                """,
                (county_id, tax_year, dataset_type),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return str(row["import_batch_id"])

    def insert_staging_rows(
        self,
        *,
        import_batch_id: str,
        county_id: str,
        dataset_type: str,
        staging_rows: Iterable[dict[str, Any]],
    ) -> list[dict[str, str]]:
        table_name, id_column = self.resolve_staging_table(dataset_type)
        inserted_rows: list[dict[str, str]] = []
        with self.connection.cursor() as cursor:
            for staging_row in staging_rows:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} (
                      import_batch_id,
                      county_id,
                      raw_payload,
                      row_hash
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING {id_column}
                    """,
                    (
                        import_batch_id,
                        county_id,
                        Jsonb(staging_row["raw_payload"]),
                        staging_row["row_hash"],
                    ),
                )
                row = cursor.fetchone()
                inserted_rows.append(
                    {
                        "staging_table": table_name,
                        "staging_row_id": str(row[id_column]),
                        "row_hash": staging_row["row_hash"],
                    }
                )
        return inserted_rows

    def fetch_staging_rows(self, *, import_batch_id: str, dataset_type: str) -> list[dict[str, Any]]:
        table_name, id_column = self.resolve_staging_table(dataset_type)
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {id_column}, raw_payload, row_hash
                FROM {table_name}
                WHERE import_batch_id = %s
                ORDER BY created_at ASC
                """,
                (import_batch_id,),
            )
            rows = cursor.fetchall()
        return [
            {
                "staging_table": table_name,
                "staging_row_id": str(row[id_column]),
                "raw_payload": row["raw_payload"],
                "row_hash": row["row_hash"],
            }
            for row in rows
        ]

    def insert_validation_results(
        self,
        *,
        job_run_id: str,
        import_batch_id: str,
        raw_file_id: str | None,
        county_id: str,
        tax_year: int,
        findings: Iterable[dict[str, Any]],
    ) -> None:
        with self.connection.cursor() as cursor:
            for finding in findings:
                cursor.execute(
                    """
                    INSERT INTO validation_results (
                      job_run_id,
                      import_batch_id,
                      raw_file_id,
                      county_id,
                      tax_year,
                      validation_scope,
                      severity,
                      entity_table,
                      validation_code,
                      message,
                      details_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_run_id,
                        import_batch_id,
                        raw_file_id,
                        county_id,
                        tax_year,
                        finding["validation_scope"],
                        finding["severity"],
                        finding.get("entity_table"),
                        finding["validation_code"],
                        finding["message"],
                        Jsonb(finding.get("details_json", {})),
                    ),
                )

    def insert_lineage_records(self, records: Iterable[dict[str, Any]]) -> None:
        with self.connection.cursor() as cursor:
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO lineage_records (
                      job_run_id,
                      import_batch_id,
                      raw_file_id,
                      relation_type,
                      source_table,
                      source_id,
                      target_table,
                      target_id,
                      source_record_hash,
                      details_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record.get("job_run_id"),
                        record.get("import_batch_id"),
                        record.get("raw_file_id"),
                        record["relation_type"],
                        record["source_table"],
                        record.get("source_id"),
                        record["target_table"],
                        record.get("target_id"),
                        record.get("source_record_hash"),
                        Jsonb(record.get("details_json", {})),
                    ),
                )

    def upsert_property_roll_records(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        normalized_records: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        appraisal_district_id = self.fetch_appraisal_district_id(county_id)
        lineage_records: list[dict[str, str]] = []

        for record in normalized_records:
            with self.connection.cursor() as cursor:
                parcel = record["parcel"]
                cursor.execute(
                    """
                    INSERT INTO parcels (
                      county_id,
                      appraisal_district_id,
                      tax_year,
                      account_number,
                      cad_property_id,
                      situs_address,
                      situs_city,
                      situs_state,
                      situs_zip,
                      owner_name,
                      property_type_code,
                      property_class_code,
                      neighborhood_code,
                      subdivision_name,
                      school_district_name,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, 'TX', %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (county_id, account_number)
                    DO UPDATE SET
                      appraisal_district_id = EXCLUDED.appraisal_district_id,
                      tax_year = EXCLUDED.tax_year,
                      cad_property_id = EXCLUDED.cad_property_id,
                      situs_address = EXCLUDED.situs_address,
                      situs_city = EXCLUDED.situs_city,
                      situs_state = EXCLUDED.situs_state,
                      situs_zip = EXCLUDED.situs_zip,
                      owner_name = EXCLUDED.owner_name,
                      property_type_code = EXCLUDED.property_type_code,
                      property_class_code = EXCLUDED.property_class_code,
                      neighborhood_code = EXCLUDED.neighborhood_code,
                      subdivision_name = EXCLUDED.subdivision_name,
                      school_district_name = EXCLUDED.school_district_name,
                      source_system_id = EXCLUDED.source_system_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    RETURNING parcel_id
                    """,
                    (
                        county_id,
                        appraisal_district_id,
                        tax_year,
                        parcel["account_number"],
                        parcel.get("cad_property_id"),
                        parcel["situs_address"],
                        parcel["situs_city"],
                        parcel["situs_zip"],
                        parcel["owner_name"],
                        parcel.get("property_type_code", "sfr"),
                        parcel.get("property_class_code"),
                        parcel.get("neighborhood_code"),
                        parcel.get("subdivision_name"),
                        parcel.get("school_district_name"),
                        source_system_id,
                        parcel["source_record_hash"],
                    ),
                )
                parcel_id = str(cursor.fetchone()["parcel_id"])

                cursor.execute("UPDATE parcel_addresses SET is_current = false WHERE parcel_id = %s", (parcel_id,))
                address = record["address"]
                cursor.execute(
                    """
                    INSERT INTO parcel_addresses (
                      parcel_id,
                      situs_address,
                      situs_city,
                      situs_state,
                      situs_zip,
                      normalized_address,
                      is_current,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, 'TX', %s, %s, true, %s, %s)
                    """,
                    (
                        parcel_id,
                        address["situs_address"],
                        address["situs_city"],
                        address["situs_zip"],
                        address["normalized_address"],
                        source_system_id,
                        parcel["source_record_hash"],
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO parcel_year_snapshots (
                      parcel_id,
                      county_id,
                      appraisal_district_id,
                      tax_year,
                      account_number,
                      source_system_id,
                      import_batch_id,
                      job_run_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      county_id = EXCLUDED.county_id,
                      appraisal_district_id = EXCLUDED.appraisal_district_id,
                      account_number = EXCLUDED.account_number,
                      source_system_id = EXCLUDED.source_system_id,
                      import_batch_id = EXCLUDED.import_batch_id,
                      job_run_id = EXCLUDED.job_run_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    RETURNING parcel_year_snapshot_id
                    """,
                    (
                        parcel_id,
                        county_id,
                        appraisal_district_id,
                        tax_year,
                        parcel["account_number"],
                        source_system_id,
                        import_batch_id,
                        job_run_id,
                        parcel["source_record_hash"],
                    ),
                )
                snapshot_id = str(cursor.fetchone()["parcel_year_snapshot_id"])

                characteristics = record["characteristics"]
                cursor.execute(
                    """
                    INSERT INTO property_characteristics (
                      parcel_year_snapshot_id,
                      property_type_code,
                      property_class_code,
                      neighborhood_code,
                      subdivision_name,
                      school_district_name,
                      homestead_flag,
                      owner_occupied_flag,
                      primary_use_code,
                      neighborhood_group,
                      effective_age
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_year_snapshot_id)
                    DO UPDATE SET
                      property_type_code = EXCLUDED.property_type_code,
                      property_class_code = EXCLUDED.property_class_code,
                      neighborhood_code = EXCLUDED.neighborhood_code,
                      subdivision_name = EXCLUDED.subdivision_name,
                      school_district_name = EXCLUDED.school_district_name,
                      homestead_flag = EXCLUDED.homestead_flag,
                      owner_occupied_flag = EXCLUDED.owner_occupied_flag,
                      primary_use_code = EXCLUDED.primary_use_code,
                      neighborhood_group = EXCLUDED.neighborhood_group,
                      effective_age = EXCLUDED.effective_age,
                      updated_at = now()
                    """,
                    (
                        snapshot_id,
                        characteristics.get("property_type_code", "sfr"),
                        characteristics.get("property_class_code"),
                        characteristics.get("neighborhood_code"),
                        characteristics.get("subdivision_name"),
                        characteristics.get("school_district_name"),
                        characteristics.get("homestead_flag", False),
                        characteristics.get("owner_occupied_flag", False),
                        characteristics.get("primary_use_code"),
                        characteristics.get("neighborhood_group"),
                        characteristics.get("effective_age"),
                    ),
                )

                cursor.execute("DELETE FROM improvements WHERE parcel_year_snapshot_id = %s", (snapshot_id,))
                for improvement in record["improvements"]:
                    cursor.execute(
                        """
                        INSERT INTO improvements (
                          parcel_year_snapshot_id,
                          improvement_type,
                          building_label,
                          living_area_sf,
                          year_built,
                          effective_year_built,
                          effective_age,
                          bedrooms,
                          full_baths,
                          half_baths,
                          stories,
                          quality_code,
                          condition_code,
                          garage_spaces,
                          pool_flag,
                          source_system_id,
                          source_record_hash
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            snapshot_id,
                            improvement.get("improvement_type", "primary_structure"),
                            improvement.get("building_label", "Main"),
                            improvement.get("living_area_sf"),
                            improvement.get("year_built"),
                            improvement.get("effective_year_built"),
                            improvement.get("effective_age"),
                            improvement.get("bedrooms"),
                            improvement.get("full_baths"),
                            improvement.get("half_baths"),
                            improvement.get("stories"),
                            improvement.get("quality_code"),
                            improvement.get("condition_code"),
                            improvement.get("garage_spaces"),
                            improvement.get("pool_flag"),
                            source_system_id,
                            parcel["source_record_hash"],
                        ),
                    )

                primary_improvement = record["improvements"][0]
                cursor.execute(
                    """
                    INSERT INTO parcel_improvements (
                      parcel_id,
                      tax_year,
                      living_area_sf,
                      year_built,
                      effective_year_built,
                      effective_age,
                      bedrooms,
                      full_baths,
                      half_baths,
                      stories,
                      quality_code,
                      condition_code,
                      garage_spaces,
                      pool_flag,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      living_area_sf = EXCLUDED.living_area_sf,
                      year_built = EXCLUDED.year_built,
                      effective_year_built = EXCLUDED.effective_year_built,
                      effective_age = EXCLUDED.effective_age,
                      bedrooms = EXCLUDED.bedrooms,
                      full_baths = EXCLUDED.full_baths,
                      half_baths = EXCLUDED.half_baths,
                      stories = EXCLUDED.stories,
                      quality_code = EXCLUDED.quality_code,
                      condition_code = EXCLUDED.condition_code,
                      garage_spaces = EXCLUDED.garage_spaces,
                      pool_flag = EXCLUDED.pool_flag,
                      source_system_id = EXCLUDED.source_system_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    """,
                    (
                        parcel_id,
                        tax_year,
                        primary_improvement.get("living_area_sf"),
                        primary_improvement.get("year_built"),
                        primary_improvement.get("effective_year_built"),
                        primary_improvement.get("effective_age"),
                        primary_improvement.get("bedrooms"),
                        primary_improvement.get("full_baths"),
                        primary_improvement.get("half_baths"),
                        primary_improvement.get("stories"),
                        primary_improvement.get("quality_code"),
                        primary_improvement.get("condition_code"),
                        primary_improvement.get("garage_spaces"),
                        primary_improvement.get("pool_flag"),
                        source_system_id,
                        parcel["source_record_hash"],
                    ),
                )

                cursor.execute("DELETE FROM land_segments WHERE parcel_year_snapshot_id = %s", (snapshot_id,))
                for segment in record["land_segments"]:
                    cursor.execute(
                        """
                        INSERT INTO land_segments (
                          parcel_year_snapshot_id,
                          segment_num,
                          land_type_code,
                          land_sf,
                          land_acres,
                          frontage_sf,
                          depth_sf,
                          market_value,
                          ag_use_value,
                          source_system_id,
                          source_record_hash
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            snapshot_id,
                            segment.get("segment_num", 1),
                            segment.get("land_type_code"),
                            segment.get("land_sf"),
                            segment.get("land_acres"),
                            segment.get("frontage_sf"),
                            segment.get("depth_sf"),
                            segment.get("market_value"),
                            segment.get("ag_use_value"),
                            source_system_id,
                            parcel["source_record_hash"],
                        ),
                    )

                primary_land = record["land_segments"][0]
                cursor.execute(
                    """
                    INSERT INTO parcel_lands (
                      parcel_id,
                      tax_year,
                      land_sf,
                      land_acres,
                      frontage_sf,
                      depth_sf,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      land_sf = EXCLUDED.land_sf,
                      land_acres = EXCLUDED.land_acres,
                      frontage_sf = EXCLUDED.frontage_sf,
                      depth_sf = EXCLUDED.depth_sf,
                      source_system_id = EXCLUDED.source_system_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    """,
                    (
                        parcel_id,
                        tax_year,
                        primary_land.get("land_sf"),
                        primary_land.get("land_acres"),
                        primary_land.get("frontage_sf"),
                        primary_land.get("depth_sf"),
                        source_system_id,
                        parcel["source_record_hash"],
                    ),
                )

                cursor.execute("DELETE FROM value_components WHERE parcel_year_snapshot_id = %s", (snapshot_id,))
                for component in record["value_components"]:
                    cursor.execute(
                        """
                        INSERT INTO value_components (
                          parcel_year_snapshot_id,
                          component_code,
                          component_label,
                          component_category,
                          market_value,
                          assessed_value,
                          taxable_value,
                          source_system_id,
                          source_record_hash
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            snapshot_id,
                            component["component_code"],
                            component.get("component_label"),
                            component.get("component_category"),
                            component.get("market_value"),
                            component.get("assessed_value"),
                            component.get("taxable_value"),
                            source_system_id,
                            parcel["source_record_hash"],
                        ),
                    )

                assessment = record["assessment"]
                cursor.execute(
                    """
                    INSERT INTO parcel_assessments (
                      parcel_id,
                      tax_year,
                      land_value,
                      improvement_value,
                      market_value,
                      assessed_value,
                      capped_value,
                      appraised_value,
                      exemption_value_total,
                      notice_value,
                      certified_value,
                      prior_year_market_value,
                      prior_year_assessed_value,
                      homestead_flag,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      land_value = EXCLUDED.land_value,
                      improvement_value = EXCLUDED.improvement_value,
                      market_value = EXCLUDED.market_value,
                      assessed_value = EXCLUDED.assessed_value,
                      capped_value = EXCLUDED.capped_value,
                      appraised_value = EXCLUDED.appraised_value,
                      exemption_value_total = EXCLUDED.exemption_value_total,
                      notice_value = EXCLUDED.notice_value,
                      certified_value = EXCLUDED.certified_value,
                      prior_year_market_value = EXCLUDED.prior_year_market_value,
                      prior_year_assessed_value = EXCLUDED.prior_year_assessed_value,
                      homestead_flag = EXCLUDED.homestead_flag,
                      source_system_id = EXCLUDED.source_system_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    """,
                    (
                        parcel_id,
                        tax_year,
                        assessment.get("land_value"),
                        assessment.get("improvement_value"),
                        assessment.get("market_value"),
                        assessment.get("assessed_value"),
                        assessment.get("capped_value"),
                        assessment.get("appraised_value"),
                        assessment.get("exemption_value_total"),
                        assessment.get("notice_value"),
                        assessment.get("certified_value"),
                        assessment.get("prior_year_market_value"),
                        assessment.get("prior_year_assessed_value"),
                        characteristics.get("homestead_flag", False),
                        source_system_id,
                        parcel["source_record_hash"],
                    ),
                )

                cursor.execute(
                    "DELETE FROM parcel_exemptions WHERE parcel_id = %s AND tax_year = %s",
                    (parcel_id, tax_year),
                )
                for exemption in record["exemptions"]:
                    cursor.execute(
                        """
                        INSERT INTO parcel_exemptions (
                          parcel_id,
                          tax_year,
                          exemption_type_code,
                          exemption_amount,
                          source_system_id,
                          source_record_hash
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (parcel_id, tax_year, exemption_type_code)
                        DO UPDATE SET
                          exemption_amount = EXCLUDED.exemption_amount,
                          source_system_id = EXCLUDED.source_system_id,
                          source_record_hash = EXCLUDED.source_record_hash,
                          updated_at = now()
                        """,
                        (
                            parcel_id,
                            tax_year,
                            exemption["exemption_type_code"],
                            exemption["exemption_amount"],
                            source_system_id,
                            parcel["source_record_hash"],
                        ),
                    )

            lineage_records.append(
                {
                    "target_table": "parcel_year_snapshots",
                    "target_id": snapshot_id,
                    "parcel_id": parcel_id,
                }
            )

        return lineage_records

    def upsert_tax_rate_records(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        normalized_records: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        appraisal_district_id = self.fetch_appraisal_district_id(county_id)
        lineage_records: list[dict[str, str]] = []

        for record in normalized_records:
            taxing_unit = record["taxing_unit"]
            tax_rate = record["tax_rate"]
            source_record_hash = record["source_record_hash"]
            with self.connection.cursor() as cursor:
                parent_taxing_unit_id = None
                if taxing_unit.get("parent_unit_code"):
                    cursor.execute(
                        """
                        SELECT taxing_unit_id
                        FROM taxing_units
                        WHERE county_id = %s
                          AND tax_year = %s
                          AND unit_code = %s
                        LIMIT 1
                        """,
                        (county_id, tax_year, taxing_unit["parent_unit_code"]),
                    )
                    parent_row = cursor.fetchone()
                    if parent_row is not None:
                        parent_taxing_unit_id = str(parent_row["taxing_unit_id"])

                cursor.execute(
                    """
                    INSERT INTO taxing_units (
                      county_id,
                      tax_year,
                      appraisal_district_id,
                      unit_type_code,
                      unit_code,
                      unit_name,
                      parent_taxing_unit_id,
                      state_geoid,
                      active_flag,
                      source_system_id,
                      import_batch_id,
                      source_record_hash,
                      metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (county_id, tax_year, unit_code)
                    DO UPDATE SET
                      appraisal_district_id = EXCLUDED.appraisal_district_id,
                      unit_type_code = EXCLUDED.unit_type_code,
                      unit_name = EXCLUDED.unit_name,
                      parent_taxing_unit_id = EXCLUDED.parent_taxing_unit_id,
                      state_geoid = EXCLUDED.state_geoid,
                      active_flag = EXCLUDED.active_flag,
                      source_system_id = EXCLUDED.source_system_id,
                      import_batch_id = EXCLUDED.import_batch_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      metadata_json = EXCLUDED.metadata_json,
                      updated_at = now()
                    RETURNING taxing_unit_id
                    """,
                    (
                        county_id,
                        tax_year,
                        appraisal_district_id,
                        taxing_unit["unit_type_code"],
                        taxing_unit["unit_code"],
                        taxing_unit["unit_name"],
                        parent_taxing_unit_id,
                        taxing_unit.get("state_geoid"),
                        taxing_unit.get("active_flag", True),
                        source_system_id,
                        import_batch_id,
                        source_record_hash,
                        Jsonb(taxing_unit.get("metadata_json", {})),
                    ),
                )
                taxing_unit_id = str(cursor.fetchone()["taxing_unit_id"])

                cursor.execute(
                    """
                    INSERT INTO tax_rates (
                      taxing_unit_id,
                      county_id,
                      tax_year,
                      rate_component,
                      rate_value,
                      rate_per_100,
                      effective_from,
                      effective_to,
                      is_current,
                      source_system_id,
                      import_batch_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (taxing_unit_id, tax_year, rate_component)
                    DO UPDATE SET
                      county_id = EXCLUDED.county_id,
                      rate_value = EXCLUDED.rate_value,
                      rate_per_100 = EXCLUDED.rate_per_100,
                      effective_from = EXCLUDED.effective_from,
                      effective_to = EXCLUDED.effective_to,
                      is_current = EXCLUDED.is_current,
                      source_system_id = EXCLUDED.source_system_id,
                      import_batch_id = EXCLUDED.import_batch_id,
                      source_record_hash = EXCLUDED.source_record_hash,
                      updated_at = now()
                    RETURNING tax_rate_id
                    """,
                    (
                        taxing_unit_id,
                        county_id,
                        tax_year,
                        tax_rate.get("rate_component", "ad_valorem"),
                        tax_rate["rate_value"],
                        tax_rate.get("rate_per_100"),
                        tax_rate.get("effective_from"),
                        tax_rate.get("effective_to"),
                        tax_rate.get("is_current", True),
                        source_system_id,
                        import_batch_id,
                        source_record_hash,
                    ),
                )
                tax_rate_id = str(cursor.fetchone()["tax_rate_id"])
            lineage_records.append(
                {
                    "target_table": "tax_rates",
                    "target_id": tax_rate_id,
                    "taxing_unit_id": taxing_unit_id,
                }
            )

        return lineage_records

    def capture_tax_rate_rollback_manifest(
        self,
        *,
        county_id: str,
        tax_year: int,
        unit_codes: Iterable[str],
    ) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        for unit_code in unit_codes:
            unit_row = self._fetch_optional_row(
                """
                SELECT *
                FROM taxing_units
                WHERE county_id = %s
                  AND tax_year = %s
                  AND unit_code = %s
                """,
                (county_id, tax_year, unit_code),
            )
            if unit_row is None:
                entries.append({"unit_code": unit_code, "prior_state": None})
                continue

            prior_state = {
                "taxing_unit": unit_row,
                "tax_rates": self._fetch_rows(
                    """
                    SELECT *
                    FROM tax_rates
                    WHERE taxing_unit_id = %s
                      AND tax_year = %s
                    ORDER BY rate_component ASC, tax_rate_id ASC
                    """,
                    (unit_row["taxing_unit_id"], tax_year),
                ),
            }
            entries.append({"unit_code": unit_code, "prior_state": prior_state})
        return {"dataset_type": "tax_rates", "entries": entries}

    def fetch_parcel_tax_contexts(self, *, county_id: str, tax_year: int) -> list[ParcelTaxContext]:
        rows = self._fetch_rows(
            """
            SELECT
              p.parcel_id,
              p.county_id,
              pys.tax_year,
              p.account_number,
              COALESCE(pa.situs_city, p.situs_city) AS situs_city,
              COALESCE(pa.situs_zip, p.situs_zip) AS situs_zip,
              COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name,
              COALESCE(pc.subdivision_name, p.subdivision_name) AS subdivision_name,
              COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code
            FROM parcel_year_snapshots pys
            JOIN parcels p
              ON p.parcel_id = pys.parcel_id
            LEFT JOIN parcel_addresses pa
              ON pa.parcel_id = p.parcel_id
             AND pa.is_current = true
            LEFT JOIN property_characteristics pc
              ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
            WHERE p.county_id = %s
              AND pys.tax_year = %s
              AND pys.is_current = true
            ORDER BY p.account_number ASC
            """,
            (county_id, tax_year),
        )
        return [
            ParcelTaxContext(
                parcel_id=str(row["parcel_id"]),
                county_id=row["county_id"],
                tax_year=row["tax_year"],
                account_number=row["account_number"],
                situs_city=row["situs_city"],
                situs_zip=row["situs_zip"],
                school_district_name=row["school_district_name"],
                subdivision_name=row["subdivision_name"],
                neighborhood_code=row["neighborhood_code"],
            )
            for row in rows
        ]

    def fetch_taxing_unit_contexts(self, *, county_id: str, tax_year: int) -> list[TaxingUnitContext]:
        rows = self._fetch_rows(
            """
            SELECT DISTINCT
              tu.taxing_unit_id,
              tu.county_id,
              tu.tax_year,
              tu.unit_type_code,
              tu.unit_code,
              tu.unit_name,
              tu.metadata_json
            FROM taxing_units tu
            JOIN tax_rates tr
              ON tr.taxing_unit_id = tu.taxing_unit_id
             AND tr.tax_year = tu.tax_year
             AND tr.is_current = true
            WHERE tu.county_id = %s
              AND tu.tax_year = %s
              AND tu.active_flag = true
            ORDER BY tu.unit_type_code ASC, tu.unit_code ASC
            """,
            (county_id, tax_year),
        )
        return [
            TaxingUnitContext(
                taxing_unit_id=str(row["taxing_unit_id"]),
                county_id=row["county_id"],
                tax_year=row["tax_year"],
                unit_type_code=row["unit_type_code"],
                unit_code=row["unit_code"],
                unit_name=row["unit_name"],
                metadata_json=dict(row["metadata_json"] or {}),
            )
            for row in rows
        ]

    def has_current_tax_rate_records(self, *, county_id: str, tax_year: int) -> bool:
        row = self._fetch_optional_row(
            """
            SELECT 1 AS present
            FROM tax_rates
            WHERE county_id = %s
              AND tax_year = %s
              AND is_current = true
            LIMIT 1
            """,
            (county_id, tax_year),
        )
        return row is not None

    def replace_parcel_tax_assignments(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        assignments: list[ParcelTaxAssignment],
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM parcel_taxing_units ptu
                USING parcels p
                WHERE ptu.parcel_id = p.parcel_id
                  AND p.county_id = %s
                  AND ptu.tax_year = %s
                  AND ptu.assignment_method <> 'manual'
                """,
                (county_id, tax_year),
            )

            for assignment in assignments:
                cursor.execute(
                    """
                    INSERT INTO parcel_taxing_units (
                      parcel_id,
                      tax_year,
                      taxing_unit_id,
                      assignment_method,
                      assignment_confidence,
                      is_primary,
                      source_system_id,
                      import_batch_id,
                      job_run_id,
                      assignment_reason_code,
                      match_basis_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year, taxing_unit_id)
                    DO UPDATE SET
                      assignment_method = EXCLUDED.assignment_method,
                      assignment_confidence = EXCLUDED.assignment_confidence,
                      is_primary = EXCLUDED.is_primary,
                      source_system_id = EXCLUDED.source_system_id,
                      import_batch_id = EXCLUDED.import_batch_id,
                      job_run_id = EXCLUDED.job_run_id,
                      assignment_reason_code = EXCLUDED.assignment_reason_code,
                      match_basis_json = EXCLUDED.match_basis_json,
                      updated_at = now()
                    """,
                    (
                        assignment.parcel_id,
                        assignment.tax_year,
                        assignment.taxing_unit_id,
                        assignment.assignment_method,
                        assignment.assignment_confidence,
                        assignment.is_primary,
                        source_system_id,
                        import_batch_id,
                        job_run_id,
                        assignment.assignment_reason_code,
                        Jsonb(assignment.match_basis_json),
                    ),
                )
        return len(assignments)

    def refresh_effective_tax_rates(self, *, county_id: str, tax_year: int) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM effective_tax_rates etr
                USING parcels p
                WHERE etr.parcel_id = p.parcel_id
                  AND p.county_id = %s
                  AND etr.tax_year = %s
                """,
                (county_id, tax_year),
            )
            cursor.execute(
                """
                SELECT
                  ptu.parcel_id,
                  ptu.tax_year,
                  SUM(COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)) AS effective_tax_rate,
                  jsonb_agg(
                    jsonb_build_object(
                      'taxing_unit_id', tu.taxing_unit_id,
                      'unit_type_code', tu.unit_type_code,
                      'unit_code', tu.unit_code,
                      'unit_name', tu.unit_name,
                      'rate_component', tr.rate_component,
                      'rate_value', COALESCE(tr.rate_value, tr.rate_per_100 / 100.0),
                      'rate_per_100', tr.rate_per_100,
                      'assignment_method', ptu.assignment_method,
                      'assignment_confidence', ptu.assignment_confidence,
                      'assignment_reason_code', ptu.assignment_reason_code
                    )
                    ORDER BY tu.unit_type_code, tu.unit_name, tr.rate_component
                  ) AS component_breakdown_json
                FROM parcel_taxing_units ptu
                JOIN parcels p
                  ON p.parcel_id = ptu.parcel_id
                JOIN taxing_units tu
                  ON tu.taxing_unit_id = ptu.taxing_unit_id
                JOIN tax_rates tr
                  ON tr.taxing_unit_id = ptu.taxing_unit_id
                 AND tr.tax_year = ptu.tax_year
                 AND tr.is_current = true
                WHERE p.county_id = %s
                  AND ptu.tax_year = %s
                GROUP BY ptu.parcel_id, ptu.tax_year
                ORDER BY ptu.parcel_id ASC
                """,
                (county_id, tax_year),
            )
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO effective_tax_rates (
                      parcel_id,
                      tax_year,
                      effective_tax_rate,
                      source_method,
                      calculation_basis_json
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      effective_tax_rate = EXCLUDED.effective_tax_rate,
                      source_method = EXCLUDED.source_method,
                      calculation_basis_json = EXCLUDED.calculation_basis_json,
                      updated_at = now()
                    """,
                    (
                        row["parcel_id"],
                        row["tax_year"],
                        row["effective_tax_rate"],
                        "parcel_taxing_units_rollup",
                        Jsonb(
                            {
                                "refreshed_from": "parcel_taxing_units_rollup",
                                "components": row["component_breakdown_json"] or [],
                            }
                        ),
                    ),
                )
        return len(rows)

    def inspect_tax_rates_import_batch(self, *, import_batch_id: str, tax_year: int) -> dict[str, Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ib.county_id,
                  ib.tax_year,
                  ib.status,
                  ib.row_count,
                  ib.error_count,
                  ib.publish_state,
                  ib.publish_version,
                  count(DISTINCT rf.raw_file_id) AS raw_file_count,
                  count(DISTINCT jr.job_run_id) AS job_run_count
                FROM import_batches ib
                LEFT JOIN raw_files rf ON rf.import_batch_id = ib.import_batch_id
                LEFT JOIN job_runs jr ON jr.import_batch_id = ib.import_batch_id
                WHERE ib.import_batch_id = %s
                GROUP BY
                  ib.county_id,
                  ib.tax_year,
                  ib.status,
                  ib.row_count,
                  ib.error_count,
                  ib.publish_state,
                  ib.publish_version
                """,
                (import_batch_id,),
            )
            batch_row = cursor.fetchone()
            if batch_row is None:
                raise ValueError(f"Missing import batch {import_batch_id}.")

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM stg_county_tax_rates_raw
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            staging_row_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM lineage_records
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            lineage_record_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT
                  count(*) AS total_count,
                  count(*) FILTER (WHERE severity = 'error') AS error_count
                FROM validation_results
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            validation_counts = cursor.fetchone()

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM taxing_units
                WHERE import_batch_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            taxing_unit_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM tax_rates
                WHERE import_batch_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            tax_rate_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_taxing_units
                WHERE import_batch_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            parcel_tax_assignment_count = cursor.fetchone()["count"]

        return {
            "import_batch_id": import_batch_id,
            "county_id": batch_row["county_id"],
            "tax_year": batch_row["tax_year"],
            "status": batch_row["status"],
            "row_count": batch_row["row_count"],
            "error_count": batch_row["error_count"],
            "publish_state": batch_row["publish_state"],
            "publish_version": batch_row["publish_version"],
            "raw_file_count": batch_row["raw_file_count"],
            "job_run_count": batch_row["job_run_count"],
            "staging_row_count": staging_row_count,
            "lineage_record_count": lineage_record_count,
            "validation_result_count": validation_counts["total_count"],
            "validation_error_count": validation_counts["error_count"],
            "taxing_unit_count": taxing_unit_count,
            "tax_rate_count": tax_rate_count,
            "parcel_tax_assignment_count": parcel_tax_assignment_count,
        }

    def capture_property_roll_rollback_manifest(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_numbers: Iterable[str],
    ) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        for account_number in account_numbers:
            parcel_row = self._fetch_optional_row(
                """
                SELECT *
                FROM parcels
                WHERE county_id = %s
                  AND account_number = %s
                """,
                (county_id, account_number),
            )
            if parcel_row is None:
                entries.append({"account_number": account_number, "prior_state": None})
                continue

            parcel_id = parcel_row["parcel_id"]
            snapshot_row = self._fetch_optional_row(
                """
                SELECT *
                FROM parcel_year_snapshots
                WHERE parcel_id = %s
                  AND tax_year = %s
                """,
                (parcel_id, tax_year),
            )
            prior_state: dict[str, Any] = {
                "parcel": parcel_row,
                "parcel_addresses": self._fetch_rows(
                    """
                    SELECT *
                    FROM parcel_addresses
                    WHERE parcel_id = %s
                    ORDER BY created_at ASC, parcel_address_id ASC
                    """,
                    (parcel_id,),
                ),
                "parcel_improvement": self._fetch_optional_row(
                    """
                    SELECT *
                    FROM parcel_improvements
                    WHERE parcel_id = %s
                      AND tax_year = %s
                    """,
                    (parcel_id, tax_year),
                ),
                "parcel_land": self._fetch_optional_row(
                    """
                    SELECT *
                    FROM parcel_lands
                    WHERE parcel_id = %s
                      AND tax_year = %s
                    """,
                    (parcel_id, tax_year),
                ),
                "parcel_assessment": self._fetch_optional_row(
                    """
                    SELECT *
                    FROM parcel_assessments
                    WHERE parcel_id = %s
                      AND tax_year = %s
                    """,
                    (parcel_id, tax_year),
                ),
                "parcel_exemptions": self._fetch_rows(
                    """
                    SELECT *
                    FROM parcel_exemptions
                    WHERE parcel_id = %s
                      AND tax_year = %s
                    ORDER BY exemption_type_code ASC
                    """,
                    (parcel_id, tax_year),
                ),
                "parcel_year_snapshot": snapshot_row,
                "property_characteristics": None,
                "improvements": [],
                "land_segments": [],
                "value_components": [],
            }
            if snapshot_row is not None:
                snapshot_id = snapshot_row["parcel_year_snapshot_id"]
                prior_state["property_characteristics"] = self._fetch_optional_row(
                    """
                    SELECT *
                    FROM property_characteristics
                    WHERE parcel_year_snapshot_id = %s
                    """,
                    (snapshot_id,),
                )
                prior_state["improvements"] = self._fetch_rows(
                    """
                    SELECT *
                    FROM improvements
                    WHERE parcel_year_snapshot_id = %s
                    ORDER BY created_at ASC, improvement_id ASC
                    """,
                    (snapshot_id,),
                )
                prior_state["land_segments"] = self._fetch_rows(
                    """
                    SELECT *
                    FROM land_segments
                    WHERE parcel_year_snapshot_id = %s
                    ORDER BY segment_num ASC, land_segment_id ASC
                    """,
                    (snapshot_id,),
                )
                prior_state["value_components"] = self._fetch_rows(
                    """
                    SELECT *
                    FROM value_components
                    WHERE parcel_year_snapshot_id = %s
                    ORDER BY component_code ASC
                    """,
                    (snapshot_id,),
                )
            entries.append({"account_number": account_number, "prior_state": prior_state})

        return {"dataset_type": "property_roll", "entries": entries}

    def fetch_job_run_metadata(self, *, import_batch_id: str, job_stage: str) -> dict[str, Any] | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT metadata_json
                FROM job_runs
                WHERE import_batch_id = %s
                  AND job_stage = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (import_batch_id, job_stage),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return dict(row["metadata_json"] or {})

    def inspect_property_roll_import_batch(self, *, import_batch_id: str, tax_year: int) -> dict[str, Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ib.county_id,
                  ib.tax_year,
                  ib.status,
                  ib.row_count,
                  ib.error_count,
                  ib.publish_state,
                  ib.publish_version,
                  count(DISTINCT rf.raw_file_id) AS raw_file_count,
                  count(DISTINCT jr.job_run_id) AS job_run_count
                FROM import_batches ib
                LEFT JOIN raw_files rf ON rf.import_batch_id = ib.import_batch_id
                LEFT JOIN job_runs jr ON jr.import_batch_id = ib.import_batch_id
                WHERE ib.import_batch_id = %s
                GROUP BY
                  ib.county_id,
                  ib.tax_year,
                  ib.status,
                  ib.row_count,
                  ib.error_count,
                  ib.publish_state,
                  ib.publish_version
                """,
                (import_batch_id,),
            )
            batch_row = cursor.fetchone()
            if batch_row is None:
                raise ValueError(f"Missing import batch {import_batch_id}.")

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM stg_county_property_raw
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            staging_row_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM lineage_records
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            lineage_record_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT
                  count(*) AS total_count,
                  count(*) FILTER (WHERE severity = 'error') AS error_count
                FROM validation_results
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            validation_counts = cursor.fetchone()

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_year_snapshots
                WHERE import_batch_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            parcel_year_snapshot_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_assessments pa
                JOIN parcel_year_snapshots pys
                  ON pys.parcel_id = pa.parcel_id
                 AND pys.tax_year = pa.tax_year
                WHERE pys.import_batch_id = %s
                  AND pa.tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            parcel_assessment_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_exemptions pe
                JOIN parcel_year_snapshots pys
                  ON pys.parcel_id = pe.parcel_id
                 AND pys.tax_year = pe.tax_year
                WHERE pys.import_batch_id = %s
                  AND pe.tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            parcel_exemption_count = cursor.fetchone()["count"]

        return {
            "import_batch_id": import_batch_id,
            "county_id": batch_row["county_id"],
            "tax_year": batch_row["tax_year"],
            "status": batch_row["status"],
            "row_count": batch_row["row_count"],
            "error_count": batch_row["error_count"],
            "publish_state": batch_row["publish_state"],
            "publish_version": batch_row["publish_version"],
            "raw_file_count": batch_row["raw_file_count"],
            "job_run_count": batch_row["job_run_count"],
            "staging_row_count": staging_row_count,
            "lineage_record_count": lineage_record_count,
            "validation_result_count": validation_counts["total_count"],
            "validation_error_count": validation_counts["error_count"],
            "parcel_year_snapshot_count": parcel_year_snapshot_count,
            "parcel_assessment_count": parcel_assessment_count,
            "parcel_exemption_count": parcel_exemption_count,
        }

    def fetch_validation_failures(self, *, import_batch_id: str, limit: int = 25) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT
              validation_result_id,
              validation_code,
              message,
              severity,
              validation_scope,
              entity_table,
              details_json,
              created_at
            FROM validation_results
            WHERE import_batch_id = %s
              AND severity = 'error'
            ORDER BY created_at ASC, validation_result_id ASC
            LIMIT %s
            """,
            (import_batch_id, limit),
        )

    def rollback_property_roll_records(
        self,
        *,
        import_batch_id: str,
        tax_year: int,
        rollback_manifest: dict[str, Any] | None,
    ) -> int:
        manifest_entries = {
            entry["account_number"]: entry.get("prior_state")
            for entry in (rollback_manifest or {}).get("entries", [])
        }
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT parcel_year_snapshot_id, parcel_id, account_number
                FROM parcel_year_snapshots
                WHERE import_batch_id = %s
                  AND tax_year = %s
                ORDER BY created_at ASC
                """,
                (import_batch_id, tax_year),
            )
            affected_rows = cursor.fetchall()

        for affected_row in affected_rows:
            parcel_id = str(affected_row["parcel_id"])
            account_number = affected_row["account_number"]
            prior_state = manifest_entries.get(account_number)
            self._delete_property_roll_state(parcel_id=parcel_id, tax_year=tax_year)
            if prior_state is None:
                self._delete_orphan_parcel(parcel_id)
                continue
            self._restore_property_roll_state(prior_state)

        return len(affected_rows)

    def rollback_tax_rate_records(
        self,
        *,
        county_id: str,
        import_batch_id: str,
        tax_year: int,
        rollback_manifest: dict[str, Any] | None,
    ) -> int:
        manifest_entries = {
            entry["unit_code"]: entry.get("prior_state")
            for entry in (rollback_manifest or {}).get("entries", [])
        }
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT unit_code
                FROM (
                  SELECT tu.unit_code
                  FROM taxing_units tu
                  WHERE tu.import_batch_id = %s
                    AND tu.county_id = %s
                    AND tu.tax_year = %s
                  UNION
                  SELECT tu.unit_code
                  FROM tax_rates tr
                  JOIN taxing_units tu ON tu.taxing_unit_id = tr.taxing_unit_id
                  WHERE tr.import_batch_id = %s
                    AND tu.county_id = %s
                    AND tr.tax_year = %s
                ) affected_units
                ORDER BY unit_code ASC
                """,
                (import_batch_id, county_id, tax_year, import_batch_id, county_id, tax_year),
            )
            affected_rows = cursor.fetchall()

        for affected_row in affected_rows:
            unit_code = affected_row["unit_code"]
            current_unit = self._fetch_optional_row(
                """
                SELECT *
                FROM taxing_units
                WHERE county_id = %s
                  AND tax_year = %s
                  AND unit_code = %s
                """,
                (county_id, tax_year, unit_code),
            )
            if current_unit is not None:
                taxing_unit_id = current_unit["taxing_unit_id"]
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM tax_rates WHERE taxing_unit_id = %s AND tax_year = %s",
                        (taxing_unit_id, tax_year),
                    )
                    cursor.execute(
                        "DELETE FROM parcel_taxing_units WHERE taxing_unit_id = %s AND tax_year = %s AND assignment_method <> 'manual'",
                        (taxing_unit_id, tax_year),
                    )
                    cursor.execute(
                        "DELETE FROM taxing_unit_boundaries WHERE taxing_unit_id = %s AND tax_year = %s",
                        (taxing_unit_id, tax_year),
                    )
                    cursor.execute("DELETE FROM taxing_units WHERE taxing_unit_id = %s", (taxing_unit_id,))

            prior_state = manifest_entries.get(unit_code)
            if prior_state is None:
                continue
            if prior_state.get("taxing_unit") is not None:
                self._insert_rows("taxing_units", [prior_state["taxing_unit"]])
            self._insert_rows("tax_rates", prior_state.get("tax_rates", []))

        return len(affected_rows)

    def _delete_property_roll_state(self, *, parcel_id: str, tax_year: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM parcel_exemptions WHERE parcel_id = %s AND tax_year = %s", (parcel_id, tax_year))
            cursor.execute("DELETE FROM parcel_assessments WHERE parcel_id = %s AND tax_year = %s", (parcel_id, tax_year))
            cursor.execute("DELETE FROM parcel_lands WHERE parcel_id = %s AND tax_year = %s", (parcel_id, tax_year))
            cursor.execute("DELETE FROM parcel_improvements WHERE parcel_id = %s AND tax_year = %s", (parcel_id, tax_year))
            cursor.execute("DELETE FROM parcel_addresses WHERE parcel_id = %s", (parcel_id,))
            cursor.execute(
                "DELETE FROM parcel_year_snapshots WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )

    def _restore_property_roll_state(self, prior_state: dict[str, Any] | None) -> None:
        if prior_state is None:
            return

        parcel_row = prior_state.get("parcel")
        if parcel_row is None:
            return

        self._restore_parcel_row(parcel_row)
        self._insert_rows("parcel_addresses", prior_state.get("parcel_addresses", []))

        snapshot_row = prior_state.get("parcel_year_snapshot")
        if snapshot_row is not None:
            self._insert_rows("parcel_year_snapshots", [snapshot_row])
            if prior_state.get("property_characteristics") is not None:
                self._insert_rows("property_characteristics", [prior_state["property_characteristics"]])
            self._insert_rows("improvements", prior_state.get("improvements", []))
            self._insert_rows("land_segments", prior_state.get("land_segments", []))
            self._insert_rows("value_components", prior_state.get("value_components", []))

        if prior_state.get("parcel_improvement") is not None:
            self._insert_rows("parcel_improvements", [prior_state["parcel_improvement"]])
        if prior_state.get("parcel_land") is not None:
            self._insert_rows("parcel_lands", [prior_state["parcel_land"]])
        if prior_state.get("parcel_assessment") is not None:
            self._insert_rows("parcel_assessments", [prior_state["parcel_assessment"]])
        self._insert_rows("parcel_exemptions", prior_state.get("parcel_exemptions", []))

    def _delete_orphan_parcel(self, parcel_id: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM parcel_year_snapshots
                  WHERE parcel_id = %s
                ) AS has_snapshots
                """,
                (parcel_id,),
            )
            row = cursor.fetchone()
            if row is not None and not row["has_snapshots"]:
                cursor.execute("DELETE FROM parcels WHERE parcel_id = %s", (parcel_id,))

    def _restore_parcel_row(self, parcel_row: dict[str, Any]) -> None:
        columns = [
            "parcel_id",
            "county_id",
            "appraisal_district_id",
            "tax_year",
            "account_number",
            "cad_property_id",
            "geo_account_number",
            "quick_ref_id",
            "situs_address",
            "situs_city",
            "situs_state",
            "situs_zip",
            "owner_name",
            "property_type_code",
            "property_class_code",
            "neighborhood_code",
            "subdivision_name",
            "school_district_name",
            "latitude",
            "longitude",
            "geom",
            "active_flag",
            "source_system_id",
            "source_record_hash",
            "created_at",
            "updated_at",
        ]
        values = [parcel_row.get(column) for column in columns]
        assignments = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in columns[1:]
        ]
        statement = sql.SQL(
            """
            INSERT INTO parcels ({columns})
            VALUES ({values})
            ON CONFLICT (parcel_id)
            DO UPDATE SET {assignments}
            """
        ).format(
            columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            assignments=sql.SQL(", ").join(assignments),
        )
        with self.connection.cursor() as cursor:
            cursor.execute(statement, values)

    def _insert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        with self.connection.cursor() as cursor:
            for row in rows:
                cursor.execute(statement, [row.get(column) for column in columns])

    def _fetch_optional_row(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        if row is None:
            return None
        return self._serialize_row(row)

    def _fetch_rows(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [self._serialize_row(row) for row in rows]

    def _serialize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {key: self._jsonable(value) for key, value in row.items()}

    def _jsonable(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._jsonable(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._jsonable(item) for item in value]
        if isinstance(value, tuple):
            return [self._jsonable(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if value.__class__.__name__ == "UUID":
            return str(value)
        return value

    def resolve_staging_table(self, dataset_type: str) -> tuple[str, str]:
        try:
            return STAGING_TABLES[dataset_type]
        except KeyError as exc:
            raise ValueError(f"Unsupported staging dataset_type: {dataset_type}") from exc
