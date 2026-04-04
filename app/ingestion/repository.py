from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from app.services.exemption_normalization import normalize_parcel_exemptions
from app.services.ownership_reconciliation import (
    build_current_owner_rollup,
    build_owner_periods,
    normalize_owner_name,
)
from app.services.tax_assignment import (
    MATCH_CONFIDENCE,
    MATCH_PRIORITY,
    MATCH_REASON_CODES,
    ParcelTaxAssignment,
    ParcelTaxContext,
    TaxingUnitContext,
)


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


@dataclass(frozen=True)
class DuplicateRawFileRecord:
    import_batch_id: str
    raw_file_id: str
    status: str
    publish_state: str | None
    source_filename: str | None
    row_count: int | None


STAGING_TABLES: dict[str, tuple[str, str]] = {
    "property_roll": ("stg_county_property_raw", "stg_county_property_raw_id"),
    "tax_rates": ("stg_county_tax_rates_raw", "stg_county_tax_rates_raw_id"),
    "sales": ("stg_sales_raw", "stg_sales_raw_id"),
    "deeds": ("stg_sales_raw", "stg_sales_raw_id"),
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
        dataset_type: str,
        source_filename: str | None,
        source_checksum: str | None,
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
                  dataset_type,
                  source_filename,
                  source_checksum,
                  source_url,
                  file_format,
                  status,
                  dry_run_flag
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'created', %s)
                RETURNING import_batch_id
                """,
                (
                    source_system_id,
                    county_id,
                    tax_year,
                    dataset_type,
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
        status_reason: str | None = None,
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
                  status_reason = COALESCE(%s, status_reason),
                  completed_at = CASE
                    WHEN %s IN (
                      'fetched',
                      'staged',
                      'normalized',
                      'published',
                      'failed',
                      'rolled_back',
                      'publish_blocked',
                      'validation_failed'
                    )
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
                    status_reason,
                    status,
                    import_batch_id,
                ),
            )

    def update_import_batch_source_details(
        self,
        import_batch_id: str,
        *,
        source_filename: str,
        source_checksum: str,
        source_url: str | None,
        file_format: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE import_batches
                SET
                  source_filename = %s,
                  source_checksum = %s,
                  source_url = %s,
                  file_format = %s,
                  updated_at = now()
                WHERE import_batch_id = %s
                """,
                (
                    source_filename,
                    source_checksum,
                    source_url,
                    file_format,
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
                      AND COALESCE(ib.dataset_type, rf.file_kind) = %s
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
                  AND COALESCE(ib.dataset_type, rf.file_kind) = %s
                ORDER BY ib.created_at DESC, rf.created_at DESC
                LIMIT 1
                """,
                (county_id, tax_year, dataset_type),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return str(row["import_batch_id"])

    def find_duplicate_raw_file(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        checksum: str,
    ) -> DuplicateRawFileRecord | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ib.import_batch_id,
                  rf.raw_file_id,
                  ib.status,
                  ib.publish_state,
                  ib.source_filename,
                  ib.row_count
                FROM raw_files rf
                JOIN import_batches ib
                  ON ib.import_batch_id = rf.import_batch_id
                WHERE rf.county_id = %s
                  AND rf.tax_year = %s
                  AND rf.file_kind = %s
                  AND rf.checksum = %s
                ORDER BY rf.created_at DESC, rf.raw_file_id DESC
                LIMIT 1
                """,
                (county_id, tax_year, dataset_type, checksum),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return DuplicateRawFileRecord(
            import_batch_id=str(row["import_batch_id"]),
            raw_file_id=str(row["raw_file_id"]),
            status=row["status"],
            publish_state=row.get("publish_state"),
            source_filename=row.get("source_filename"),
            row_count=row.get("row_count"),
        )

    def count_validation_errors(self, *, import_batch_id: str) -> int:
        row = self._fetch_optional_row(
            """
            SELECT count(*) AS count
            FROM validation_results
            WHERE import_batch_id = %s
              AND severity = 'error'
            """,
            (import_batch_id,),
        )
        if row is None:
            return 0
        return int(row["count"] or 0)

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

    def fetch_staging_rows(
        self, *, import_batch_id: str, dataset_type: str
    ) -> list[dict[str, Any]]:
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

    def iterate_staging_rows(
        self,
        *,
        import_batch_id: str,
        dataset_type: str,
        chunk_size: int,
    ) -> Iterable[list[dict[str, Any]]]:
        table_name, id_column = self.resolve_staging_table(dataset_type)
        last_seen_id: str | None = None

        while True:
            with self.connection.cursor() as cursor:
                if last_seen_id is None:
                    cursor.execute(
                        f"""
                        SELECT {id_column}, raw_payload, row_hash
                        FROM {table_name}
                        WHERE import_batch_id = %s
                        ORDER BY {id_column} ASC
                        LIMIT %s
                        """,
                        (import_batch_id, chunk_size),
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT {id_column}, raw_payload, row_hash
                        FROM {table_name}
                        WHERE import_batch_id = %s
                          AND {id_column} > %s
                        ORDER BY {id_column} ASC
                        LIMIT %s
                        """,
                        (import_batch_id, last_seen_id, chunk_size),
                    )
                rows = cursor.fetchall()

            if not rows:
                return

            chunk = [
                {
                    "staging_table": table_name,
                    "staging_row_id": str(row[id_column]),
                    "raw_payload": row["raw_payload"],
                    "row_hash": row["row_hash"],
                }
                for row in rows
            ]
            last_seen_id = chunk[-1]["staging_row_id"]
            yield chunk

    def count_staging_rows(self, *, import_batch_id: str, dataset_type: str) -> int:
        table_name, _ = self.resolve_staging_table(dataset_type)
        row = self._fetch_optional_row(
            f"""
            SELECT count(*) AS count
            FROM {table_name}
            WHERE import_batch_id = %s
            """,
            (import_batch_id,),
        )
        if row is None:
            return 0
        return int(row["count"] or 0)

    def count_property_roll_rows_for_import_batch(self, *, import_batch_id: str) -> int:
        row = self._fetch_optional_row(
            """
            SELECT count(*) AS count
            FROM parcel_year_snapshots
            WHERE import_batch_id = %s
            """,
            (import_batch_id,),
        )
        if row is None:
            return 0
        return int(row["count"] or 0)

    def count_property_roll_improvement_rows_for_import_batch(
        self,
        *,
        import_batch_id: str,
        tax_year: int,
    ) -> int:
        row = self._fetch_optional_row(
            """
            SELECT count(*) AS count
            FROM parcel_improvements pi
            JOIN parcel_year_snapshots pys
              ON pys.parcel_id = pi.parcel_id
             AND pys.tax_year = pi.tax_year
            WHERE pys.import_batch_id = %s
              AND pys.tax_year = %s
            """,
            (import_batch_id, tax_year),
        )
        if row is None:
            return 0
        return int(row["count"] or 0)

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
        rows = list(findings)
        if not rows:
            return
        with self.connection.cursor() as cursor:
            cursor.executemany(
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
                [
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
                    )
                    for finding in rows
                ],
            )

    def insert_lineage_records(self, records: Iterable[dict[str, Any]]) -> None:
        rows = list(records)
        if not rows:
            return
        with self.connection.cursor() as cursor:
            cursor.executemany(
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
                [
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
                    )
                    for record in rows
                ],
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
        include_detail_tables: bool = True,
    ) -> list[dict[str, str]]:
        appraisal_district_id = self.fetch_appraisal_district_id(county_id)
        lineage_records: list[dict[str, str]] = []
        if not normalized_records:
            return lineage_records

        if not include_detail_tables:
            self._bulk_upsert_property_roll_core_records(
                county_id=county_id,
                tax_year=tax_year,
                import_batch_id=import_batch_id,
                job_run_id=job_run_id,
                source_system_id=source_system_id,
                appraisal_district_id=appraisal_district_id,
                normalized_records=normalized_records,
            )
            return lineage_records

        account_numbers = [record["parcel"]["account_number"] for record in normalized_records]

        with self.connection.cursor() as cursor:
            cursor.executemany(
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
                """,
                [
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
                    )
                    for record in normalized_records
                    for parcel in [record["parcel"]]
                ],
            )
            cursor.execute(
                """
                SELECT parcel_id, account_number
                FROM parcels
                WHERE county_id = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, account_numbers),
            )
            parcel_ids_by_account = {
                row["account_number"]: str(row["parcel_id"]) for row in cursor.fetchall()
            }
            parcel_ids = list(parcel_ids_by_account.values())

            if parcel_ids:
                cursor.execute(
                    "UPDATE parcel_addresses SET is_current = false WHERE parcel_id = ANY(%s)",
                    (parcel_ids,),
                )
            cursor.executemany(
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
                [
                    (
                        parcel_ids_by_account[parcel["account_number"]],
                        address["situs_address"],
                        address["situs_city"],
                        address["situs_zip"],
                        address["normalized_address"],
                        source_system_id,
                        parcel["source_record_hash"],
                    )
                    for record in normalized_records
                    for parcel, address in [(record["parcel"], record["address"])]
                ],
            )

            cursor.executemany(
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
                  cad_owner_name,
                  cad_owner_name_normalized,
                  source_record_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (parcel_id, tax_year)
                DO UPDATE SET
                  county_id = EXCLUDED.county_id,
                  appraisal_district_id = EXCLUDED.appraisal_district_id,
                  account_number = EXCLUDED.account_number,
                  source_system_id = EXCLUDED.source_system_id,
                  import_batch_id = EXCLUDED.import_batch_id,
                  job_run_id = EXCLUDED.job_run_id,
                  cad_owner_name = EXCLUDED.cad_owner_name,
                  cad_owner_name_normalized = EXCLUDED.cad_owner_name_normalized,
                  source_record_hash = EXCLUDED.source_record_hash,
                  updated_at = now()
                """,
                [
                    (
                        parcel_ids_by_account[parcel["account_number"]],
                        county_id,
                        appraisal_district_id,
                        tax_year,
                        parcel["account_number"],
                        source_system_id,
                        import_batch_id,
                        job_run_id,
                        parcel.get("owner_name"),
                        normalize_owner_name(parcel.get("owner_name")),
                        parcel["source_record_hash"],
                    )
                    for record in normalized_records
                    for parcel in [record["parcel"]]
                ],
            )
            cursor.execute(
                """
                SELECT pys.parcel_year_snapshot_id, pys.parcel_id, p.account_number
                FROM parcel_year_snapshots pys
                JOIN parcels p ON p.parcel_id = pys.parcel_id
                WHERE p.county_id = %s
                  AND pys.tax_year = %s
                  AND p.account_number = ANY(%s)
                """,
                (county_id, tax_year, account_numbers),
            )
            snapshot_ids_by_account = {
                row["account_number"]: str(row["parcel_year_snapshot_id"])
                for row in cursor.fetchall()
            }
            snapshot_ids = list(snapshot_ids_by_account.values())

            cursor.executemany(
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
                [
                    (
                        snapshot_ids_by_account[parcel["account_number"]],
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
                    )
                    for record in normalized_records
                    for parcel, characteristics in [(record["parcel"], record["characteristics"])]
                ],
            )

            if snapshot_ids and include_detail_tables:
                cursor.execute(
                    "DELETE FROM improvements WHERE parcel_year_snapshot_id = ANY(%s)",
                    (snapshot_ids,),
                )
                cursor.execute(
                    "DELETE FROM land_segments WHERE parcel_year_snapshot_id = ANY(%s)",
                    (snapshot_ids,),
                )
                cursor.execute(
                    "DELETE FROM value_components WHERE parcel_year_snapshot_id = ANY(%s)",
                    (snapshot_ids,),
                )

            improvement_rows: list[tuple[Any, ...]] = []
            parcel_improvement_rows: list[tuple[Any, ...]] = []
            land_segment_rows: list[tuple[Any, ...]] = []
            parcel_land_rows: list[tuple[Any, ...]] = []
            value_component_rows: list[tuple[Any, ...]] = []
            assessment_rows: list[tuple[Any, ...]] = []
            exemption_rows: list[tuple[Any, ...]] = []

            for record in normalized_records:
                parcel = record["parcel"]
                account_number = parcel["account_number"]
                parcel_id = parcel_ids_by_account[account_number]
                snapshot_id = snapshot_ids_by_account[account_number]
                source_record_hash = parcel["source_record_hash"]
                characteristics = record["characteristics"]

                improvements = list(record.get("improvements") or [])

                if include_detail_tables:
                    for improvement in improvements:
                        improvement_rows.append(
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
                                source_record_hash,
                            )
                        )

                # parcel_improvements is part of the canonical parcel-year summary path, so we
                # keep this summary row populated even when bulk property-roll mode skips the
                # heavier per-snapshot detail tables.
                if improvements:
                    primary_improvement = improvements[0]
                    parcel_improvement_rows.append(
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
                            source_record_hash,
                        )
                    )

                if include_detail_tables:
                    for segment in record["land_segments"]:
                        land_segment_rows.append(
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
                                source_record_hash,
                            )
                        )

                    primary_land = record["land_segments"][0]
                    parcel_land_rows.append(
                        (
                            parcel_id,
                            tax_year,
                            primary_land.get("land_sf"),
                            primary_land.get("land_acres"),
                            primary_land.get("frontage_sf"),
                            primary_land.get("depth_sf"),
                            source_system_id,
                            source_record_hash,
                        )
                    )

                    for component in record["value_components"]:
                        value_component_rows.append(
                            (
                                snapshot_id,
                                component["component_code"],
                                component.get("component_label"),
                                component.get("component_category"),
                                component.get("market_value"),
                                component.get("assessed_value"),
                                component.get("taxable_value"),
                                source_system_id,
                                source_record_hash,
                            )
                        )

                assessment = record["assessment"]
                assessment_rows.append(
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
                        source_record_hash,
                    )
                )

                if include_detail_tables:
                    for exemption in normalize_parcel_exemptions(record.get("exemptions", [])):
                        exemption_rows.append(
                            (
                                parcel_id,
                                tax_year,
                                exemption["exemption_type_code"],
                                exemption["exemption_amount"],
                                exemption.get("raw_exemption_codes", []),
                                exemption.get("source_entry_count", 1),
                                exemption.get("amount_missing_flag", False),
                                exemption.get("granted_flag", True),
                                source_system_id,
                                source_record_hash,
                            )
                        )

            if improvement_rows and include_detail_tables:
                cursor.executemany(
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
                    improvement_rows,
                )
            if parcel_improvement_rows:
                cursor.executemany(
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
                    parcel_improvement_rows,
                )

            if land_segment_rows and include_detail_tables:
                cursor.executemany(
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
                    land_segment_rows,
                )
            if include_detail_tables:
                cursor.executemany(
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
                    parcel_land_rows,
                )

            if value_component_rows and include_detail_tables:
                cursor.executemany(
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
                    value_component_rows,
                )
            cursor.executemany(
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
                assessment_rows,
            )

            if parcel_ids and include_detail_tables:
                cursor.execute(
                    "DELETE FROM parcel_exemptions WHERE parcel_id = ANY(%s) AND tax_year = %s",
                    (parcel_ids, tax_year),
                )
            if exemption_rows and include_detail_tables:
                cursor.executemany(
                    """
                    INSERT INTO parcel_exemptions (
                      parcel_id,
                      tax_year,
                      exemption_type_code,
                      exemption_amount,
                      raw_exemption_codes,
                      source_entry_count,
                      amount_missing_flag,
                      granted_flag,
                      source_system_id,
                      source_record_hash
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    exemption_rows,
                )

        for account_number in account_numbers:
            lineage_records.append(
                {
                    "target_table": "parcel_year_snapshots",
                    "target_id": snapshot_ids_by_account[account_number],
                    "parcel_id": parcel_ids_by_account[account_number],
                }
            )

        return lineage_records

    def _bulk_upsert_property_roll_core_records(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        appraisal_district_id: str,
        normalized_records: list[dict[str, Any]],
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE IF NOT EXISTS tmp_property_roll_core_upsert (
                  account_number text PRIMARY KEY,
                  cad_property_id text,
                  situs_address text,
                  situs_city text,
                  situs_zip text,
                  owner_name text,
                  parcel_property_type_code text,
                  property_class_code text,
                  neighborhood_code text,
                  subdivision_name text,
                  school_district_name text,
                  source_record_hash text,
                  normalized_address text,
                  cad_owner_name text,
                  cad_owner_name_normalized text,
                  characteristics_property_type_code text,
                  characteristics_property_class_code text,
                  characteristics_neighborhood_code text,
                  characteristics_subdivision_name text,
                  characteristics_school_district_name text,
                  homestead_flag boolean,
                  owner_occupied_flag boolean,
                  primary_use_code text,
                  neighborhood_group text,
                  effective_age integer,
                  living_area_sf numeric,
                  year_built integer,
                  effective_year_built integer,
                  improvement_effective_age integer,
                  bedrooms integer,
                  full_baths numeric,
                  half_baths numeric,
                  stories numeric,
                  quality_code text,
                  condition_code text,
                  garage_spaces numeric,
                  pool_flag boolean,
                  land_value numeric,
                  improvement_value numeric,
                  market_value numeric,
                  assessed_value numeric,
                  capped_value numeric,
                  appraised_value numeric,
                  exemption_value_total numeric,
                  notice_value numeric,
                  certified_value numeric,
                  prior_year_market_value numeric,
                  prior_year_assessed_value numeric
                ) ON COMMIT DROP
                """
            )
            cursor.execute("TRUNCATE tmp_property_roll_core_upsert")
            with cursor.copy(
                """
                COPY tmp_property_roll_core_upsert (
                  account_number,
                  cad_property_id,
                  situs_address,
                  situs_city,
                  situs_zip,
                  owner_name,
                  parcel_property_type_code,
                  property_class_code,
                  neighborhood_code,
                  subdivision_name,
                  school_district_name,
                  source_record_hash,
                  normalized_address,
                  cad_owner_name,
                  cad_owner_name_normalized,
                  characteristics_property_type_code,
                  characteristics_property_class_code,
                  characteristics_neighborhood_code,
                  characteristics_subdivision_name,
                  characteristics_school_district_name,
                  homestead_flag,
                  owner_occupied_flag,
                  primary_use_code,
                  neighborhood_group,
                  effective_age,
                  living_area_sf,
                  year_built,
                  effective_year_built,
                  improvement_effective_age,
                  bedrooms,
                  full_baths,
                  half_baths,
                  stories,
                  quality_code,
                  condition_code,
                  garage_spaces,
                  pool_flag,
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
                  prior_year_assessed_value
                ) FROM STDIN
                """
            ) as copy:
                for record in normalized_records:
                    parcel = record["parcel"]
                    address = record["address"]
                    characteristics = record["characteristics"]
                    improvements = list(record.get("improvements") or [])
                    primary_improvement = improvements[0] if improvements else {}
                    assessment = record["assessment"]
                    copy.write_row(
                        (
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
                            parcel["source_record_hash"],
                            address["normalized_address"],
                            parcel.get("owner_name"),
                            normalize_owner_name(parcel.get("owner_name")),
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
                        )
                    )

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
                SELECT
                  %s,
                  %s,
                  %s,
                  account_number,
                  cad_property_id,
                  situs_address,
                  situs_city,
                  'TX',
                  situs_zip,
                  owner_name,
                  parcel_property_type_code,
                  property_class_code,
                  neighborhood_code,
                  subdivision_name,
                  school_district_name,
                  %s,
                  source_record_hash
                FROM tmp_property_roll_core_upsert
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
                """,
                (county_id, appraisal_district_id, tax_year, source_system_id),
            )
            cursor.execute(
                """
                UPDATE parcel_addresses pa
                SET is_current = false
                FROM parcels p
                JOIN tmp_property_roll_core_upsert t
                  ON t.account_number = p.account_number
                WHERE pa.parcel_id = p.parcel_id
                  AND p.county_id = %s
                """,
                (county_id,),
            )
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
                SELECT
                  p.parcel_id,
                  t.situs_address,
                  t.situs_city,
                  'TX',
                  t.situs_zip,
                  t.normalized_address,
                  true,
                  %s,
                  t.source_record_hash
                FROM tmp_property_roll_core_upsert t
                JOIN parcels p
                  ON p.county_id = %s
                 AND p.account_number = t.account_number
                """,
                (source_system_id, county_id),
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
                  cad_owner_name,
                  cad_owner_name_normalized,
                  source_record_hash
                )
                SELECT
                  p.parcel_id,
                  %s,
                  %s,
                  %s,
                  t.account_number,
                  %s,
                  %s,
                  %s,
                  t.cad_owner_name,
                  t.cad_owner_name_normalized,
                  t.source_record_hash
                FROM tmp_property_roll_core_upsert t
                JOIN parcels p
                  ON p.county_id = %s
                 AND p.account_number = t.account_number
                ON CONFLICT (parcel_id, tax_year)
                DO UPDATE SET
                  county_id = EXCLUDED.county_id,
                  appraisal_district_id = EXCLUDED.appraisal_district_id,
                  account_number = EXCLUDED.account_number,
                  source_system_id = EXCLUDED.source_system_id,
                  import_batch_id = EXCLUDED.import_batch_id,
                  job_run_id = EXCLUDED.job_run_id,
                  cad_owner_name = EXCLUDED.cad_owner_name,
                  cad_owner_name_normalized = EXCLUDED.cad_owner_name_normalized,
                  source_record_hash = EXCLUDED.source_record_hash,
                  updated_at = now()
                """,
                (
                    county_id,
                    appraisal_district_id,
                    tax_year,
                    source_system_id,
                    import_batch_id,
                    job_run_id,
                    county_id,
                ),
            )
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
                SELECT
                  pys.parcel_year_snapshot_id,
                  t.characteristics_property_type_code,
                  t.characteristics_property_class_code,
                  t.characteristics_neighborhood_code,
                  t.characteristics_subdivision_name,
                  t.characteristics_school_district_name,
                  t.homestead_flag,
                  t.owner_occupied_flag,
                  t.primary_use_code,
                  t.neighborhood_group,
                  t.effective_age
                FROM tmp_property_roll_core_upsert t
                JOIN parcels p
                  ON p.county_id = %s
                 AND p.account_number = t.account_number
                JOIN parcel_year_snapshots pys
                  ON pys.parcel_id = p.parcel_id
                 AND pys.tax_year = %s
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
                (county_id, tax_year),
            )
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
                SELECT
                  p.parcel_id,
                  %s,
                  t.living_area_sf,
                  t.year_built,
                  t.effective_year_built,
                  t.improvement_effective_age,
                  t.bedrooms,
                  t.full_baths,
                  t.half_baths,
                  t.stories,
                  t.quality_code,
                  t.condition_code,
                  t.garage_spaces,
                  t.pool_flag,
                  %s,
                  t.source_record_hash
                FROM tmp_property_roll_core_upsert t
                JOIN parcels p
                  ON p.county_id = %s
                 AND p.account_number = t.account_number
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
                (tax_year, source_system_id, county_id),
            )
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
                SELECT
                  p.parcel_id,
                  %s,
                  t.land_value,
                  t.improvement_value,
                  t.market_value,
                  t.assessed_value,
                  t.capped_value,
                  t.appraised_value,
                  t.exemption_value_total,
                  t.notice_value,
                  t.certified_value,
                  t.prior_year_market_value,
                  t.prior_year_assessed_value,
                  t.homestead_flag,
                  %s,
                  t.source_record_hash
                FROM tmp_property_roll_core_upsert t
                JOIN parcels p
                  ON p.county_id = %s
                 AND p.account_number = t.account_number
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
                (tax_year, source_system_id, county_id),
            )

    def upsert_deed_records(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        normalized_records: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        lineage_records: list[dict[str, str]] = []

        for record in normalized_records:
            linked_account_number = record.get("linked_account_number")
            linked_cad_property_id = record.get("linked_cad_property_id")
            linked_alias_values = list(record.get("linked_alias_values") or [])
            parcel_link = self._find_parcel_for_deed(
                county_id=county_id,
                linked_account_number=linked_account_number,
                linked_cad_property_id=linked_cad_property_id,
                linked_alias_values=linked_alias_values,
            )
            parcel_id = None if parcel_link is None else str(parcel_link["parcel_id"])

            deed_record = dict(record["deed_record"])
            metadata_json = dict(deed_record.get("metadata_json") or {})
            metadata_json.update(
                {
                    "linked_account_number": linked_account_number,
                    "linked_cad_property_id": linked_cad_property_id,
                    "linked_alias_values": linked_alias_values,
                    "parcel_link_status": "matched" if parcel_id is not None else "unmatched",
                    "parcel_link_basis": (
                        None if parcel_link is None else parcel_link["match_basis"]
                    ),
                }
            )

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO deed_records (
                      county_id,
                      parcel_id,
                      tax_year,
                      source_system_id,
                      import_batch_id,
                      job_run_id,
                      instrument_number,
                      volume_page,
                      recording_date,
                      execution_date,
                      consideration_amount,
                      document_type,
                      transfer_type,
                      grantor_summary,
                      grantee_summary,
                      source_record_hash,
                      metadata_json
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (county_id, instrument_number)
                    DO UPDATE SET
                      parcel_id = EXCLUDED.parcel_id,
                      tax_year = EXCLUDED.tax_year,
                      source_system_id = EXCLUDED.source_system_id,
                      import_batch_id = EXCLUDED.import_batch_id,
                      job_run_id = EXCLUDED.job_run_id,
                      volume_page = EXCLUDED.volume_page,
                      recording_date = EXCLUDED.recording_date,
                      execution_date = EXCLUDED.execution_date,
                      consideration_amount = EXCLUDED.consideration_amount,
                      document_type = EXCLUDED.document_type,
                      transfer_type = EXCLUDED.transfer_type,
                      grantor_summary = EXCLUDED.grantor_summary,
                      grantee_summary = EXCLUDED.grantee_summary,
                      source_record_hash = EXCLUDED.source_record_hash,
                      metadata_json = EXCLUDED.metadata_json,
                      updated_at = now()
                    RETURNING deed_record_id
                    """,
                    (
                        county_id,
                        parcel_id,
                        tax_year,
                        source_system_id,
                        import_batch_id,
                        job_run_id,
                        deed_record.get("instrument_number"),
                        deed_record.get("volume_page"),
                        deed_record.get("recording_date"),
                        deed_record.get("execution_date"),
                        deed_record.get("consideration_amount"),
                        deed_record.get("document_type"),
                        deed_record.get("transfer_type"),
                        deed_record.get("grantor_summary"),
                        deed_record.get("grantee_summary"),
                        record["source_record_hash"],
                        Jsonb(metadata_json),
                    ),
                )
                deed_record_id = str(cursor.fetchone()["deed_record_id"])

                cursor.execute(
                    "DELETE FROM deed_parties WHERE deed_record_id = %s",
                    (deed_record_id,),
                )
                for party in record.get("deed_parties", []):
                    cursor.execute(
                        """
                        INSERT INTO deed_parties (
                          deed_record_id,
                          party_role,
                          party_name,
                          normalized_name,
                          party_order,
                          mailing_address
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            deed_record_id,
                            party.get("party_role"),
                            party.get("party_name"),
                            party.get("normalized_name"),
                            party.get("party_order"),
                            party.get("mailing_address"),
                        ),
                    )

            lineage_records.append(
                {
                    "target_table": "deed_records",
                    "target_id": deed_record_id,
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

    def fetch_taxing_unit_contexts(
        self, *, county_id: str, tax_year: int
    ) -> list[TaxingUnitContext]:
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

    def refresh_parcel_tax_assignments_set_based(
        self,
        *,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            cursor.execute(
                """
                CREATE TEMP TABLE tmp_parcel_tax_context ON COMMIT DROP AS
                SELECT
                  p.parcel_id,
                  upper(btrim(p.county_id)) AS county_id,
                  pys.tax_year,
                  upper(btrim(p.account_number)) AS account_number,
                  upper(btrim(COALESCE(pa.situs_city, p.situs_city))) AS situs_city,
                  upper(btrim(COALESCE(pa.situs_zip, p.situs_zip))) AS situs_zip,
                  upper(btrim(COALESCE(pc.school_district_name, p.school_district_name))) AS school_district_name,
                  upper(btrim(COALESCE(pc.subdivision_name, p.subdivision_name))) AS subdivision_name,
                  upper(btrim(COALESCE(pc.neighborhood_code, p.neighborhood_code))) AS neighborhood_code
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
                """,
                (county_id, tax_year),
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_lookup
                  ON tmp_parcel_tax_context(county_id, tax_year)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_account
                  ON tmp_parcel_tax_context(account_number)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_school
                  ON tmp_parcel_tax_context(school_district_name)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_city
                  ON tmp_parcel_tax_context(situs_city)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_subdivision
                  ON tmp_parcel_tax_context(subdivision_name)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_neighborhood
                  ON tmp_parcel_tax_context(neighborhood_code)
                """
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_parcel_tax_context_zip
                  ON tmp_parcel_tax_context(situs_zip)
                """
            )

            cursor.execute(
                """
                CREATE TEMP TABLE tmp_tax_unit_match_hints ON COMMIT DROP AS
                WITH current_units AS (
                  SELECT DISTINCT
                    tu.taxing_unit_id,
                    tu.county_id,
                    tu.tax_year,
                    tu.unit_type_code,
                    tu.unit_code,
                    tu.unit_name,
                    COALESCE(tu.metadata_json, '{}'::jsonb) AS metadata_json
                  FROM taxing_units tu
                  JOIN tax_rates tr
                    ON tr.taxing_unit_id = tu.taxing_unit_id
                   AND tr.tax_year = tu.tax_year
                   AND tr.is_current = true
                  WHERE tu.county_id = %s
                    AND tu.tax_year = %s
                    AND tu.active_flag = true
                ),
                explicit_hints AS (
                  SELECT
                    cu.taxing_unit_id,
                    cu.tax_year,
                    cu.unit_type_code,
                    cu.unit_code,
                    upper(btrim(value.value)) AS candidate_value,
                    hint_value.match_key,
                    COALESCE(
                      NULLIF(cu.metadata_json -> 'assignment_hints' ->> 'priority', '')::integer,
                      hint_value.default_priority
                    ) AS priority
                  FROM current_units cu
                  CROSS JOIN LATERAL (
                    VALUES
                      (
                        'account_numbers',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'account_numbers', '[]'::jsonb),
                        %s
                      ),
                      (
                        'school_district_names',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'school_district_names', '[]'::jsonb),
                        %s
                      ),
                      (
                        'cities',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'cities', '[]'::jsonb),
                        %s
                      ),
                      (
                        'subdivisions',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'subdivisions', '[]'::jsonb),
                        %s
                      ),
                      (
                        'neighborhood_codes',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'neighborhood_codes', '[]'::jsonb),
                        %s
                      ),
                      (
                        'zip_codes',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'zip_codes', '[]'::jsonb),
                        %s
                      ),
                      (
                        'county_ids',
                        COALESCE(cu.metadata_json -> 'assignment_hints' -> 'county_ids', '[]'::jsonb),
                        %s
                      )
                  ) AS hint_value(match_key, values_json, default_priority)
                  CROSS JOIN LATERAL jsonb_array_elements_text(hint_value.values_json) AS value(value)
                  WHERE btrim(value.value) <> ''
                ),
                fallback_hints AS (
                  SELECT
                    cu.taxing_unit_id,
                    cu.tax_year,
                    cu.unit_type_code,
                    cu.unit_code,
                    upper(btrim(value.value)) AS candidate_value,
                    fallback_value.match_key,
                    fallback_value.default_priority AS priority
                  FROM current_units cu
                  CROSS JOIN LATERAL (
                    VALUES
                      (
                        'cities',
                        CASE
                          WHEN cu.unit_type_code = 'city'
                           AND jsonb_array_length(COALESCE(cu.metadata_json -> 'assignment_hints' -> 'cities', '[]'::jsonb)) = 0
                            THEN ARRAY[cu.unit_name]
                          ELSE ARRAY[]::text[]
                        END
                        ||
                        CASE
                          WHEN cu.unit_type_code = 'city'
                           AND jsonb_array_length(COALESCE(cu.metadata_json -> 'assignment_hints' -> 'cities', '[]'::jsonb)) = 0
                            THEN ARRAY(
                              SELECT jsonb_array_elements_text(COALESCE(cu.metadata_json -> 'aliases', '[]'::jsonb))
                            )
                          ELSE ARRAY[]::text[]
                        END,
                        %s
                      ),
                      (
                        'school_district_names',
                        CASE
                          WHEN cu.unit_type_code = 'school'
                           AND jsonb_array_length(COALESCE(cu.metadata_json -> 'assignment_hints' -> 'school_district_names', '[]'::jsonb)) = 0
                            THEN ARRAY[cu.unit_name]
                          ELSE ARRAY[]::text[]
                        END
                        ||
                        CASE
                          WHEN cu.unit_type_code = 'school'
                           AND jsonb_array_length(COALESCE(cu.metadata_json -> 'assignment_hints' -> 'school_district_names', '[]'::jsonb)) = 0
                            THEN ARRAY(
                              SELECT jsonb_array_elements_text(COALESCE(cu.metadata_json -> 'aliases', '[]'::jsonb))
                            )
                          ELSE ARRAY[]::text[]
                        END,
                        %s
                      ),
                      (
                        'subdivisions',
                        CASE
                          WHEN cu.unit_type_code = 'mud'
                           AND jsonb_array_length(COALESCE(cu.metadata_json -> 'assignment_hints' -> 'subdivisions', '[]'::jsonb)) = 0
                            THEN ARRAY(
                              SELECT jsonb_array_elements_text(COALESCE(cu.metadata_json -> 'aliases', '[]'::jsonb))
                            )
                          ELSE ARRAY[]::text[]
                        END,
                        %s
                      )
                  ) AS fallback_value(match_key, values_array, default_priority)
                  CROSS JOIN LATERAL unnest(fallback_value.values_array) AS value(value)
                  WHERE btrim(value.value) <> ''
                )
                SELECT DISTINCT
                  taxing_unit_id,
                  tax_year,
                  unit_type_code,
                  unit_code,
                  candidate_value,
                  match_key,
                  priority
                FROM (
                  SELECT * FROM explicit_hints
                  UNION ALL
                  SELECT * FROM fallback_hints
                ) hints
                WHERE candidate_value IS NOT NULL
                """,
                (
                    county_id,
                    tax_year,
                    MATCH_PRIORITY["account_numbers"],
                    MATCH_PRIORITY["school_district_names"],
                    MATCH_PRIORITY["cities"],
                    MATCH_PRIORITY["subdivisions"],
                    MATCH_PRIORITY["neighborhood_codes"],
                    MATCH_PRIORITY["zip_codes"],
                    MATCH_PRIORITY["county_ids"],
                    MATCH_PRIORITY["cities"],
                    MATCH_PRIORITY["school_district_names"],
                    MATCH_PRIORITY["subdivisions"],
                ),
            )
            cursor.execute(
                """
                CREATE INDEX idx_tmp_tax_unit_match_hints_lookup
                  ON tmp_tax_unit_match_hints(match_key, candidate_value)
                """
            )

            cursor.execute(
                """
                CREATE TEMP TABLE tmp_ranked_tax_assignments ON COMMIT DROP AS
                WITH county_candidates AS (
                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    tu.taxing_unit_id,
                    tu.unit_type_code,
                    tu.unit_code,
                    'source_direct'::assignment_method_enum AS assignment_method,
                    %s::numeric(5,4) AS assignment_confidence,
                    %s::integer AS priority,
                    %s::text AS assignment_reason_code
                  FROM tmp_parcel_tax_context ptc
                  JOIN (
                    SELECT DISTINCT
                      taxing_unit_id,
                      upper(btrim(county_id)) AS county_id,
                      tax_year,
                      unit_type_code,
                      unit_code
                    FROM taxing_units
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND active_flag = true
                      AND unit_type_code = 'county'
                  ) tu
                    ON tu.county_id = ptc.county_id
                   AND tu.tax_year = ptc.tax_year
                ),
                hint_candidates AS (
                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_direct'::assignment_method_enum AS assignment_method,
                    %s::numeric(5,4) AS assignment_confidence,
                    hints.priority,
                    %s::text AS assignment_reason_code
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'account_numbers'
                   AND hints.candidate_value = ptc.account_number

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_inferred'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'school_district_names'
                   AND hints.candidate_value = ptc.school_district_name

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_inferred'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'cities'
                   AND hints.candidate_value = ptc.situs_city

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_inferred'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'subdivisions'
                   AND hints.candidate_value = ptc.subdivision_name

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_inferred'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'neighborhood_codes'
                   AND hints.candidate_value = ptc.neighborhood_code

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_inferred'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'zip_codes'
                   AND hints.candidate_value = ptc.situs_zip

                  UNION ALL

                  SELECT
                    ptc.parcel_id,
                    ptc.tax_year,
                    hints.taxing_unit_id,
                    hints.unit_type_code,
                    hints.unit_code,
                    'source_direct'::assignment_method_enum,
                    %s::numeric(5,4),
                    hints.priority,
                    %s::text
                  FROM tmp_tax_unit_match_hints hints
                  JOIN tmp_parcel_tax_context ptc
                    ON hints.match_key = 'county_ids'
                   AND hints.candidate_value = ptc.county_id
                ),
                all_candidates AS (
                  SELECT * FROM county_candidates
                  UNION ALL
                  SELECT * FROM hint_candidates
                )
                SELECT
                  candidate.*,
                  ROW_NUMBER() OVER (
                    PARTITION BY candidate.parcel_id, candidate.tax_year, candidate.unit_type_code
                    ORDER BY candidate.priority DESC, candidate.assignment_confidence DESC, candidate.unit_code DESC
                  ) AS unit_rank
                FROM all_candidates candidate
                """
                ,
                (
                    MATCH_CONFIDENCE["county_ids"],
                    MATCH_PRIORITY["county_ids"],
                    MATCH_REASON_CODES["county_ids"],
                    county_id,
                    tax_year,
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
                ),
            )

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
                SELECT
                  ranked.parcel_id,
                  ranked.tax_year,
                  ranked.taxing_unit_id,
                  ranked.assignment_method,
                  ranked.assignment_confidence,
                  ranked.unit_rank = 1 AS is_primary,
                  %s,
                  %s,
                  %s,
                  ranked.assignment_reason_code,
                  jsonb_build_object(
                    'matched_field',
                    CASE ranked.assignment_reason_code
                      WHEN 'match_account_number' THEN 'account_number'
                      WHEN 'match_school_district_name' THEN 'school_district_name'
                      WHEN 'match_city' THEN 'situs_city'
                      WHEN 'match_subdivision' THEN 'subdivision_name'
                      WHEN 'match_neighborhood_code' THEN 'neighborhood_code'
                      WHEN 'match_zip_code' THEN 'situs_zip'
                      WHEN 'match_county_id' THEN 'county_id'
                      ELSE NULL
                    END,
                    'assignment_reason_code',
                    ranked.assignment_reason_code
                  )
                FROM tmp_ranked_tax_assignments ranked
                WHERE ranked.unit_rank = 1
                   OR ranked.unit_type_code NOT IN ('county', 'city', 'school', 'mud')
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
                (source_system_id, import_batch_id, job_run_id),
            )
            assignment_count = cursor.rowcount

        return int(assignment_count)

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
                INSERT INTO effective_tax_rates (
                  parcel_id,
                  tax_year,
                  effective_tax_rate,
                  source_method,
                  calculation_basis_json
                )
                SELECT
                  ptu.parcel_id,
                  ptu.tax_year,
                  SUM(COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)) AS effective_tax_rate,
                  'parcel_taxing_units_rollup',
                  jsonb_build_object(
                    'refreshed_from', 'parcel_taxing_units_rollup',
                    'components',
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
                    )
                  ) AS calculation_basis_json
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
                ON CONFLICT (parcel_id, tax_year)
                DO UPDATE SET
                  effective_tax_rate = EXCLUDED.effective_tax_rate,
                  source_method = EXCLUDED.source_method,
                  calculation_basis_json = EXCLUDED.calculation_basis_json,
                  updated_at = now()
                """,
                (county_id, tax_year),
            )
        return int(cursor.rowcount)

    def inspect_tax_rates_import_batch(
        self, *, import_batch_id: str, tax_year: int
    ) -> dict[str, Any]:
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
        requested_accounts = sorted({account_number for account_number in account_numbers if account_number})
        if not requested_accounts:
            return {"dataset_type": "property_roll", "entries": []}

        parcel_rows = self._fetch_rows(
            """
            SELECT *
            FROM parcels
            WHERE county_id = %s
              AND account_number = ANY(%s)
            ORDER BY account_number ASC
            """,
            (county_id, requested_accounts),
        )
        parcel_by_account = {row["account_number"]: row for row in parcel_rows}
        if not parcel_rows:
            return {
                "dataset_type": "property_roll",
                "entries": [
                    {"account_number": account_number, "prior_state": None}
                    for account_number in requested_accounts
                ],
            }

        parcel_ids = [row["parcel_id"] for row in parcel_rows]

        addresses_by_parcel = self._group_rows_by_key(
            self._fetch_rows(
                """
                SELECT *
                FROM parcel_addresses
                WHERE parcel_id = ANY(%s)
                ORDER BY created_at ASC, parcel_address_id ASC
                """,
                (parcel_ids,),
            ),
            "parcel_id",
        )
        parcel_improvements = self._rows_by_key(
            self._fetch_rows(
                """
                SELECT *
                FROM parcel_improvements
                WHERE parcel_id = ANY(%s)
                  AND tax_year = %s
                """,
                (parcel_ids, tax_year),
            ),
            "parcel_id",
        )
        parcel_lands = self._rows_by_key(
            self._fetch_rows(
                """
                SELECT *
                FROM parcel_lands
                WHERE parcel_id = ANY(%s)
                  AND tax_year = %s
                """,
                (parcel_ids, tax_year),
            ),
            "parcel_id",
        )
        parcel_assessments = self._rows_by_key(
            self._fetch_rows(
                """
                SELECT *
                FROM parcel_assessments
                WHERE parcel_id = ANY(%s)
                  AND tax_year = %s
                """,
                (parcel_ids, tax_year),
            ),
            "parcel_id",
        )
        parcel_exemptions = self._group_rows_by_key(
            self._fetch_rows(
                """
                SELECT *
                FROM parcel_exemptions
                WHERE parcel_id = ANY(%s)
                  AND tax_year = %s
                ORDER BY exemption_type_code ASC
                """,
                (parcel_ids, tax_year),
            ),
            "parcel_id",
        )
        snapshot_rows = self._fetch_rows(
            """
            SELECT *
            FROM parcel_year_snapshots
            WHERE parcel_id = ANY(%s)
              AND tax_year = %s
            """,
            (parcel_ids, tax_year),
        )
        snapshots_by_parcel = {row["parcel_id"]: row for row in snapshot_rows}
        snapshot_ids = [row["parcel_year_snapshot_id"] for row in snapshot_rows]

        if snapshot_ids:
            characteristics_by_snapshot = self._rows_by_key(
                self._fetch_rows(
                    """
                    SELECT *
                    FROM property_characteristics
                    WHERE parcel_year_snapshot_id = ANY(%s)
                    """,
                    (snapshot_ids,),
                ),
                "parcel_year_snapshot_id",
            )
            improvements_by_snapshot = self._group_rows_by_key(
                self._fetch_rows(
                    """
                    SELECT *
                    FROM improvements
                    WHERE parcel_year_snapshot_id = ANY(%s)
                    ORDER BY created_at ASC, improvement_id ASC
                    """,
                    (snapshot_ids,),
                ),
                "parcel_year_snapshot_id",
            )
            land_segments_by_snapshot = self._group_rows_by_key(
                self._fetch_rows(
                    """
                    SELECT *
                    FROM land_segments
                    WHERE parcel_year_snapshot_id = ANY(%s)
                    ORDER BY segment_num ASC, land_segment_id ASC
                    """,
                    (snapshot_ids,),
                ),
                "parcel_year_snapshot_id",
            )
            value_components_by_snapshot = self._group_rows_by_key(
                self._fetch_rows(
                    """
                    SELECT *
                    FROM value_components
                    WHERE parcel_year_snapshot_id = ANY(%s)
                    ORDER BY component_code ASC
                    """,
                    (snapshot_ids,),
                ),
                "parcel_year_snapshot_id",
            )
        else:
            characteristics_by_snapshot = {}
            improvements_by_snapshot = {}
            land_segments_by_snapshot = {}
            value_components_by_snapshot = {}

        entries: list[dict[str, Any]] = []
        for account_number in requested_accounts:
            parcel_row = parcel_by_account.get(account_number)
            if parcel_row is None:
                entries.append({"account_number": account_number, "prior_state": None})
                continue

            parcel_id = parcel_row["parcel_id"]
            snapshot_row = snapshots_by_parcel.get(parcel_id)
            snapshot_id = None if snapshot_row is None else snapshot_row["parcel_year_snapshot_id"]
            prior_state: dict[str, Any] = {
                "parcel": parcel_row,
                "parcel_addresses": addresses_by_parcel.get(parcel_id, []),
                "parcel_improvement": parcel_improvements.get(parcel_id),
                "parcel_land": parcel_lands.get(parcel_id),
                "parcel_assessment": parcel_assessments.get(parcel_id),
                "parcel_exemptions": parcel_exemptions.get(parcel_id, []),
                "parcel_year_snapshot": snapshot_row,
                "property_characteristics": (
                    None if snapshot_id is None else characteristics_by_snapshot.get(snapshot_id)
                ),
                "improvements": [] if snapshot_id is None else improvements_by_snapshot.get(snapshot_id, []),
                "land_segments": [] if snapshot_id is None else land_segments_by_snapshot.get(snapshot_id, []),
                "value_components": [] if snapshot_id is None else value_components_by_snapshot.get(snapshot_id, []),
            }
            entries.append({"account_number": account_number, "prior_state": prior_state})

        return {"dataset_type": "property_roll", "entries": entries}

    def capture_deed_rollback_manifest(
        self,
        *,
        county_id: str,
        tax_year: int,
        normalized_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        deed_entries: list[dict[str, Any]] = []
        parcel_entries: dict[str, dict[str, Any]] = {}

        for record in normalized_records:
            instrument_number = record["deed_record"].get("instrument_number")
            if instrument_number is None:
                continue

            prior_deed_row = self._fetch_optional_row(
                """
                SELECT *
                FROM deed_records
                WHERE county_id = %s
                  AND instrument_number = %s
                """,
                (county_id, instrument_number),
            )
            prior_deed_parties = []
            if prior_deed_row is not None:
                prior_deed_parties = self._fetch_rows(
                    """
                    SELECT *
                    FROM deed_parties
                    WHERE deed_record_id = %s
                    ORDER BY party_role ASC, party_order ASC, deed_party_id ASC
                    """,
                    (prior_deed_row["deed_record_id"],),
                )

            deed_entries.append(
                {
                    "instrument_number": instrument_number,
                    "prior_state": (
                        None
                        if prior_deed_row is None
                        else {
                            "deed_record": prior_deed_row,
                            "deed_parties": prior_deed_parties,
                        }
                    ),
                }
            )

            parcel_link = self._find_parcel_for_deed(
                county_id=county_id,
                linked_account_number=record.get("linked_account_number"),
                linked_cad_property_id=record.get("linked_cad_property_id"),
                linked_alias_values=list(record.get("linked_alias_values") or []),
            )
            if parcel_link is None:
                continue

            parcel_id = str(parcel_link["parcel_id"])
            if parcel_id in parcel_entries:
                continue

            parcel_entries[parcel_id] = {
                "parcel_id": parcel_id,
                "prior_rollup": self._fetch_optional_row(
                    """
                    SELECT *
                    FROM current_owner_rollups
                    WHERE parcel_id = %s
                      AND tax_year = %s
                    """,
                    (parcel_id, tax_year),
                ),
                "prior_periods": self._fetch_rows(
                    """
                    SELECT *
                    FROM parcel_owner_periods
                    WHERE parcel_id = %s
                    ORDER BY start_date ASC NULLS FIRST, created_at ASC, parcel_owner_period_id ASC
                    """,
                    (parcel_id,),
                ),
            }

        return {
            "dataset_type": "deeds",
            "deed_entries": deed_entries,
            "parcel_entries": list(parcel_entries.values()),
        }

    def fetch_job_run_metadata(
        self, *, import_batch_id: str, job_stage: str
    ) -> dict[str, Any] | None:
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

    def inspect_property_roll_import_batch(
        self, *, import_batch_id: str, tax_year: int
    ) -> dict[str, Any]:
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

    def inspect_deeds_import_batch(self, *, import_batch_id: str, tax_year: int) -> dict[str, Any]:
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
                FROM stg_sales_raw
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
                FROM deed_records
                WHERE import_batch_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            deed_record_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM deed_parties dp
                JOIN deed_records dr ON dr.deed_record_id = dp.deed_record_id
                WHERE dr.import_batch_id = %s
                  AND dr.tax_year = %s
                """,
                (import_batch_id, tax_year),
            )
            deed_party_count = cursor.fetchone()["count"]

            cursor.execute(
                """
                WITH affected_parcels AS (
                  SELECT DISTINCT parcel_id
                  FROM deed_records
                  WHERE import_batch_id = %s
                    AND tax_year = %s
                    AND parcel_id IS NOT NULL
                )
                SELECT
                  (SELECT count(*)
                   FROM parcel_owner_periods pop
                   JOIN affected_parcels ap ON ap.parcel_id = pop.parcel_id) AS parcel_owner_period_count,
                  (SELECT count(*)
                   FROM current_owner_rollups cor
                   JOIN affected_parcels ap ON ap.parcel_id = cor.parcel_id
                   WHERE cor.tax_year = %s) AS current_owner_rollup_count
                """,
                (import_batch_id, tax_year, tax_year),
            )
            ownership_counts = cursor.fetchone()

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
            "deed_record_count": deed_record_count,
            "deed_party_count": deed_party_count,
            "parcel_owner_period_count": ownership_counts["parcel_owner_period_count"],
            "current_owner_rollup_count": ownership_counts["current_owner_rollup_count"],
        }

    def fetch_validation_failures(
        self, *, import_batch_id: str, limit: int = 25
    ) -> list[dict[str, Any]]:
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
                    cursor.execute(
                        "DELETE FROM taxing_units WHERE taxing_unit_id = %s", (taxing_unit_id,)
                    )

            prior_state = manifest_entries.get(unit_code)
            if prior_state is None:
                continue
            if prior_state.get("taxing_unit") is not None:
                self._insert_rows("taxing_units", [prior_state["taxing_unit"]])
            self._insert_rows("tax_rates", prior_state.get("tax_rates", []))

        return len(affected_rows)

    def rollback_deed_records(
        self,
        *,
        county_id: str,
        import_batch_id: str,
        tax_year: int,
        rollback_manifest: dict[str, Any] | None,
    ) -> int:
        deed_entries = {
            entry["instrument_number"]: entry.get("prior_state")
            for entry in (rollback_manifest or {}).get("deed_entries", [])
        }
        parcel_entries = {
            entry["parcel_id"]: entry
            for entry in (rollback_manifest or {}).get("parcel_entries", [])
        }

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT deed_record_id, instrument_number, parcel_id
                FROM deed_records
                WHERE import_batch_id = %s
                  AND county_id = %s
                  AND tax_year = %s
                ORDER BY instrument_number ASC
                """,
                (import_batch_id, county_id, tax_year),
            )
            affected_rows = cursor.fetchall()

        affected_parcel_ids = {
            str(row["parcel_id"]) for row in affected_rows if row["parcel_id"] is not None
        }
        for parcel_id in affected_parcel_ids:
            self._delete_ownership_reconciliation_state(parcel_id=parcel_id, tax_year=tax_year)

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM deed_parties
                WHERE deed_record_id IN (
                  SELECT deed_record_id
                  FROM deed_records
                  WHERE import_batch_id = %s
                    AND county_id = %s
                    AND tax_year = %s
                )
                """,
                (import_batch_id, county_id, tax_year),
            )
            cursor.execute(
                """
                DELETE FROM deed_records
                WHERE import_batch_id = %s
                  AND county_id = %s
                  AND tax_year = %s
                """,
                (import_batch_id, county_id, tax_year),
            )

        for affected_row in affected_rows:
            instrument_number = affected_row["instrument_number"]
            prior_state = deed_entries.get(instrument_number)
            if prior_state is None:
                continue
            self._insert_rows("deed_records", [prior_state["deed_record"]])
            self._insert_rows("deed_parties", prior_state.get("deed_parties", []))

        for _parcel_id, entry in parcel_entries.items():
            self._restore_ownership_reconciliation_state(
                prior_rollup=entry.get("prior_rollup"),
                prior_periods=entry.get("prior_periods", []),
            )

        return len(affected_rows)

    def refresh_owner_reconciliation(
        self,
        *,
        county_id: str,
        tax_year: int,
        parcel_ids: Iterable[str] | None = None,
    ) -> int:
        with self.connection.cursor() as cursor:
            if parcel_ids is None:
                cursor.execute(
                    """
                    SELECT
                      parcel_id,
                      county_id,
                      tax_year,
                      cad_owner_name,
                      cad_owner_name_normalized,
                      source_system_id
                    FROM parcel_year_snapshots
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND is_current = true
                    ORDER BY parcel_id ASC
                    """,
                    (county_id, tax_year),
                )
            else:
                parcel_id_list = list(parcel_ids)
                if not parcel_id_list:
                    return 0
                cursor.execute(
                    """
                    SELECT
                      parcel_id,
                      county_id,
                      tax_year,
                      cad_owner_name,
                      cad_owner_name_normalized,
                      source_system_id
                    FROM parcel_year_snapshots
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND is_current = true
                      AND parcel_id = ANY(%s::uuid[])
                    ORDER BY parcel_id ASC
                    """,
                    (county_id, tax_year, parcel_id_list),
                )
            snapshot_rows = cursor.fetchall()

        refreshed_count = 0
        for snapshot_row in snapshot_rows:
            parcel_id = str(snapshot_row["parcel_id"])
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM manual_overrides
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND override_scope = 'ownership'
                      AND target_table = 'parcels'
                      AND target_record_id = %s
                      AND status IN ('approved', 'applied')
                      AND COALESCE(effective_to, now() + interval '100 years') >= now()
                    ORDER BY effective_from DESC, created_at DESC
                    LIMIT 1
                    """,
                    (county_id, tax_year, parcel_id),
                )
                manual_override = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT
                      dr.deed_record_id,
                      dr.parcel_id,
                      dr.source_system_id,
                      dr.instrument_number,
                      dr.recording_date,
                      dr.execution_date,
                      COALESCE(dr.recording_date, dr.execution_date) AS effective_date,
                      dr.document_type,
                      dr.transfer_type,
                      dr.grantee_summary,
                      (
                        SELECT COALESCE(
                          jsonb_agg(
                            jsonb_build_object(
                              'party_name', dp.party_name,
                              'normalized_name', dp.normalized_name,
                              'party_order', dp.party_order,
                              'mailing_address', dp.mailing_address
                            )
                            ORDER BY dp.party_order ASC, dp.deed_party_id ASC
                          ),
                          '[]'::jsonb
                        )
                        FROM deed_parties dp
                        WHERE dp.deed_record_id = dr.deed_record_id
                          AND dp.party_role = 'grantee'
                      ) AS grantee_parties
                    FROM deed_records dr
                    WHERE dr.parcel_id = %s
                    ORDER BY COALESCE(dr.recording_date, dr.execution_date) ASC NULLS LAST, dr.instrument_number ASC
                    """,
                    (parcel_id,),
                )
                deed_rows = cursor.fetchall()

                self._delete_ownership_reconciliation_state(parcel_id=parcel_id, tax_year=tax_year)

                owner_periods = build_owner_periods(
                    parcel_id=parcel_id,
                    county_id=county_id,
                    cad_owner_name=snapshot_row["cad_owner_name"],
                    source_system_id=(
                        None
                        if snapshot_row["source_system_id"] is None
                        else str(snapshot_row["source_system_id"])
                    ),
                    deed_records=[
                        {
                            "deed_record_id": str(row["deed_record_id"]),
                            "parcel_id": (
                                None if row["parcel_id"] is None else str(row["parcel_id"])
                            ),
                            "source_system_id": (
                                None
                                if row["source_system_id"] is None
                                else str(row["source_system_id"])
                            ),
                            "instrument_number": row["instrument_number"],
                            "effective_date": row["effective_date"],
                            "document_type": row["document_type"],
                            "transfer_type": row["transfer_type"],
                            "grantee_summary": row["grantee_summary"],
                            "grantee_parties": list(row["grantee_parties"] or []),
                            "mailing_address": next(
                                (
                                    party.get("mailing_address")
                                    for party in list(row["grantee_parties"] or [])
                                    if party.get("mailing_address")
                                ),
                                None,
                            ),
                        }
                        for row in deed_rows
                    ],
                )

                inserted_periods: list[dict[str, Any]] = []
                for period in owner_periods:
                    cursor.execute(
                        """
                        INSERT INTO parcel_owner_periods (
                          parcel_id,
                          county_id,
                          owner_name,
                          owner_name_normalized,
                          start_date,
                          end_date,
                          source_basis,
                          deed_record_id,
                          source_system_id,
                          confidence_score,
                          is_current,
                          metadata_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING parcel_owner_period_id
                        """,
                        (
                            period["parcel_id"],
                            period["county_id"],
                            period["owner_name"],
                            period.get("owner_name_normalized"),
                            period.get("start_date"),
                            period.get("end_date"),
                            period["source_basis"],
                            period.get("deed_record_id"),
                            period.get("source_system_id"),
                            period.get("confidence_score"),
                            period.get("is_current", False),
                            Jsonb(period.get("metadata_json", {})),
                        ),
                    )
                    inserted_period = dict(period)
                    inserted_period["parcel_owner_period_id"] = str(
                        cursor.fetchone()["parcel_owner_period_id"]
                    )
                    inserted_periods.append(inserted_period)

                current_rollup = build_current_owner_rollup(
                    tax_year=tax_year,
                    cad_owner_name=snapshot_row["cad_owner_name"],
                    cad_owner_name_normalized=snapshot_row["cad_owner_name_normalized"],
                    cad_source_system_id=(
                        None
                        if snapshot_row["source_system_id"] is None
                        else str(snapshot_row["source_system_id"])
                    ),
                    owner_periods=inserted_periods,
                    manual_override=None if manual_override is None else dict(manual_override),
                )
                if current_rollup is None:
                    continue

                cursor.execute(
                    """
                    INSERT INTO current_owner_rollups (
                      parcel_id,
                      county_id,
                      tax_year,
                      owner_name,
                      owner_name_normalized,
                      owner_names_json,
                      mailing_address,
                      mailing_city,
                      mailing_state,
                      mailing_zip,
                      source_basis,
                      source_record_hash,
                      source_system_id,
                      owner_period_id,
                      confidence_score,
                      override_flag,
                      metadata_json
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (parcel_id, tax_year)
                    DO UPDATE SET
                      county_id = EXCLUDED.county_id,
                      owner_name = EXCLUDED.owner_name,
                      owner_name_normalized = EXCLUDED.owner_name_normalized,
                      owner_names_json = EXCLUDED.owner_names_json,
                      mailing_address = EXCLUDED.mailing_address,
                      mailing_city = EXCLUDED.mailing_city,
                      mailing_state = EXCLUDED.mailing_state,
                      mailing_zip = EXCLUDED.mailing_zip,
                      source_basis = EXCLUDED.source_basis,
                      source_record_hash = EXCLUDED.source_record_hash,
                      source_system_id = EXCLUDED.source_system_id,
                      owner_period_id = EXCLUDED.owner_period_id,
                      confidence_score = EXCLUDED.confidence_score,
                      override_flag = EXCLUDED.override_flag,
                      metadata_json = EXCLUDED.metadata_json,
                      updated_at = now()
                    """,
                    (
                        parcel_id,
                        county_id,
                        tax_year,
                        current_rollup.owner_name,
                        current_rollup.owner_name_normalized,
                        Jsonb(current_rollup.owner_names_json),
                        current_rollup.mailing_address,
                        current_rollup.mailing_city,
                        current_rollup.mailing_state,
                        current_rollup.mailing_zip,
                        current_rollup.source_basis,
                        current_rollup.source_record_hash,
                        current_rollup.source_system_id,
                        current_rollup.owner_period_id,
                        current_rollup.confidence_score,
                        current_rollup.override_flag,
                        Jsonb(current_rollup.metadata_json),
                    ),
                )
            refreshed_count += 1

        return refreshed_count

    def refresh_search_documents(self, *, county_id: str, tax_year: int) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT dwellio_refresh_search_documents(%s, %s) AS refreshed_count",
                (county_id, tax_year),
            )
            row = cursor.fetchone()

        if row is None:
            return 0
        return int(row["refreshed_count"] or 0)

    def _delete_property_roll_state(self, *, parcel_id: str, tax_year: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM parcel_exemptions WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )
            cursor.execute(
                "DELETE FROM parcel_assessments WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )
            cursor.execute(
                "DELETE FROM parcel_lands WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )
            cursor.execute(
                "DELETE FROM parcel_improvements WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )
            cursor.execute("DELETE FROM parcel_addresses WHERE parcel_id = %s", (parcel_id,))
            cursor.execute(
                "DELETE FROM parcel_year_snapshots WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )

    def _delete_ownership_reconciliation_state(self, *, parcel_id: str, tax_year: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM current_owner_rollups WHERE parcel_id = %s AND tax_year = %s",
                (parcel_id, tax_year),
            )
            cursor.execute(
                "DELETE FROM parcel_owner_periods WHERE parcel_id = %s",
                (parcel_id,),
            )

    def _restore_ownership_reconciliation_state(
        self,
        *,
        prior_rollup: dict[str, Any] | None,
        prior_periods: list[dict[str, Any]],
    ) -> None:
        self._insert_rows("parcel_owner_periods", prior_periods)
        if prior_rollup is not None:
            self._insert_rows("current_owner_rollups", [prior_rollup])

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
                self._insert_rows(
                    "property_characteristics", [prior_state["property_characteristics"]]
                )
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

    def _find_parcel_for_deed(
        self,
        *,
        county_id: str,
        linked_account_number: str | None,
        linked_cad_property_id: str | None,
        linked_alias_values: list[str],
    ) -> dict[str, Any] | None:
        if linked_account_number:
            row = self._fetch_optional_row(
                """
                SELECT parcel_id, 'account_number' AS match_basis
                FROM parcels
                WHERE county_id = %s
                  AND account_number = %s
                LIMIT 1
                """,
                (county_id, linked_account_number),
            )
            if row is not None:
                return row

        if linked_cad_property_id:
            row = self._fetch_optional_row(
                """
                SELECT parcel_id, 'cad_property_id' AS match_basis
                FROM parcels
                WHERE county_id = %s
                  AND cad_property_id = %s
                LIMIT 1
                """,
                (county_id, linked_cad_property_id),
            )
            if row is not None:
                return row

        for alias_value in linked_alias_values:
            row = self._fetch_optional_row(
                """
                SELECT
                  parcel_id,
                  CASE
                    WHEN account_number = %s THEN 'alias_account_number'
                    WHEN cad_property_id = %s THEN 'alias_cad_property_id'
                    WHEN geo_account_number = %s THEN 'alias_geo_account_number'
                    WHEN quick_ref_id = %s THEN 'alias_quick_ref_id'
                    ELSE 'alias'
                  END AS match_basis
                FROM parcels
                WHERE county_id = %s
                  AND (
                    account_number = %s
                    OR cad_property_id = %s
                    OR geo_account_number = %s
                    OR quick_ref_id = %s
                  )
                LIMIT 1
                """,
                (
                    alias_value,
                    alias_value,
                    alias_value,
                    alias_value,
                    county_id,
                    alias_value,
                    alias_value,
                    alias_value,
                    alias_value,
                ),
            )
            if row is not None:
                return row

        return None

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
                cursor.execute(
                    statement,
                    [self._prepare_insert_value(column, row.get(column)) for column in columns],
                )

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

    def _group_rows_by_key(
        self,
        rows: list[dict[str, Any]],
        key_name: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        grouped: dict[Any, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(row.get(key_name), []).append(row)
        return grouped

    def _rows_by_key(
        self,
        rows: list[dict[str, Any]],
        key_name: str,
    ) -> dict[Any, dict[str, Any]]:
        return {row.get(key_name): row for row in rows}

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

    def _prepare_insert_value(self, column: str, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            return Jsonb(value)
        if isinstance(value, list) and column.endswith("_json"):
            return Jsonb(value)
        return value

    def resolve_staging_table(self, dataset_type: str) -> tuple[str, str]:
        try:
            return STAGING_TABLES[dataset_type]
        except KeyError as exc:
            raise ValueError(f"Unsupported staging dataset_type: {dataset_type}") from exc
