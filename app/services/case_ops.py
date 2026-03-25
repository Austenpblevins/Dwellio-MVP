from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.db.connection import get_connection
from app.models.case import (
    AdminCaseAssignment,
    AdminCaseDetail,
    AdminCaseListResponse,
    AdminCaseNote,
    AdminCaseStatusHistoryEntry,
    AdminCaseSummary,
    AdminEvidenceCompSet,
    AdminEvidenceCompSetItem,
    AdminEvidencePacketDetail,
    AdminEvidencePacketItem,
    AdminEvidencePacketListResponse,
    AdminEvidencePacketSummary,
    AdminHearingSummary,
    CaseMutationResult,
    EvidencePacketCreate,
    ProtestCaseCreate,
    ProtestCaseNoteCreate,
    ProtestCaseStatusUpdate,
)


class CaseOpsService:
    def list_cases(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        case_status: str | None = None,
        limit: int = 50,
    ) -> AdminCaseListResponse:
        with get_connection() as connection:
            rows = self._fetch_case_summary_rows(
                connection,
                county_id=county_id,
                tax_year=tax_year,
                case_status=case_status,
                limit=limit,
            )
        return AdminCaseListResponse(
            county_id=county_id,
            tax_year=tax_year,
            case_status=case_status,
            cases=[self._build_case_summary(row) for row in rows],
        )

    def get_case_detail(self, *, protest_case_id: str) -> AdminCaseDetail:
        with get_connection() as connection:
            case_row = self._fetch_case_detail_row(connection, protest_case_id=protest_case_id)
            if case_row is None:
                raise LookupError(f"Missing protest case {protest_case_id}.")
            notes = self._fetch_case_notes(connection, protest_case_id=protest_case_id)
            assignments = self._fetch_case_assignments(connection, protest_case_id=protest_case_id)
            hearings = self._fetch_hearings(connection, protest_case_id=protest_case_id)
            status_history = self._fetch_case_status_history(connection, protest_case_id=protest_case_id)
            packets = self._fetch_packet_summary_rows(connection, protest_case_id=protest_case_id, limit=100)

        return AdminCaseDetail(
            case=self._build_case_summary(case_row),
            notes=[self._build_case_note(row) for row in notes],
            assignments=[self._build_case_assignment(row) for row in assignments],
            hearings=[self._build_hearing_summary(row) for row in hearings],
            status_history=[self._build_case_status_history(row) for row in status_history],
            packets=[self._build_packet_summary(row) for row in packets],
        )

    def create_case(self, *, request: ProtestCaseCreate) -> CaseMutationResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT county_id
                    FROM parcels
                    WHERE parcel_id = %s
                    """,
                    (request.parcel_id,),
                )
                parcel_row = cursor.fetchone()
                if parcel_row is None:
                    raise LookupError(f"Missing parcel {request.parcel_id}.")

                cursor.execute(
                    """
                    INSERT INTO protest_cases (
                      client_id,
                      parcel_id,
                      tax_year,
                      valuation_run_id,
                      case_status,
                      appraisal_district_id,
                      representation_agreement_id,
                      workflow_status_code
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING protest_case_id
                    """,
                    (
                        request.client_id,
                        request.parcel_id,
                        request.tax_year,
                        request.valuation_run_id,
                        request.case_status,
                        request.appraisal_district_id,
                        request.representation_agreement_id,
                        request.workflow_status_code,
                    ),
                )
                case_row = cursor.fetchone()
                protest_case_id = str(case_row["protest_case_id"])

                cursor.execute(
                    """
                    INSERT INTO client_parcels (
                      client_id,
                      parcel_id
                    )
                    VALUES (%s, %s)
                    ON CONFLICT (client_id, parcel_id) DO NOTHING
                    """,
                    (request.client_id, request.parcel_id),
                )

                cursor.execute(
                    """
                    INSERT INTO case_status_history (
                      protest_case_id,
                      workflow_status_code,
                      case_status,
                      reason_text,
                      changed_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        protest_case_id,
                        request.workflow_status_code,
                        request.case_status,
                        "case_created",
                        "system",
                    ),
                )
            connection.commit()

        return CaseMutationResult(
            action="create_protest_case",
            protest_case_id=protest_case_id,
            message="Created protest case and linked the client to the parcel.",
        )

    def add_case_note(
        self,
        *,
        protest_case_id: str,
        request: ProtestCaseNoteCreate,
    ) -> CaseMutationResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO case_notes (
                      protest_case_id,
                      note_text,
                      note_code,
                      author_label
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING case_note_id
                    """,
                    (
                        protest_case_id,
                        request.note_text,
                        request.note_code,
                        request.author_label,
                    ),
                )
                row = cursor.fetchone()
                if row is None:
                    raise LookupError(f"Missing protest case {protest_case_id}.")
            connection.commit()

        return CaseMutationResult(
            action="add_case_note",
            protest_case_id=protest_case_id,
            message=f"Added note {row['case_note_id']} to the protest case.",
        )

    def update_case_status(
        self,
        *,
        protest_case_id: str,
        request: ProtestCaseStatusUpdate,
    ) -> CaseMutationResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE protest_cases
                    SET
                      case_status = %s,
                      workflow_status_code = COALESCE(%s, workflow_status_code),
                      updated_at = now()
                    WHERE protest_case_id = %s
                    RETURNING protest_case_id, workflow_status_code
                    """,
                    (request.case_status, request.workflow_status_code, protest_case_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise LookupError(f"Missing protest case {protest_case_id}.")

                cursor.execute(
                    """
                    INSERT INTO case_status_history (
                      protest_case_id,
                      workflow_status_code,
                      case_status,
                      reason_text,
                      changed_by
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        protest_case_id,
                        request.workflow_status_code or row["workflow_status_code"],
                        request.case_status,
                        request.reason_text,
                        request.changed_by,
                    ),
                )
            connection.commit()

        return CaseMutationResult(
            action="update_case_status",
            protest_case_id=protest_case_id,
            message="Updated protest case status and appended history.",
        )

    def list_packets(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        packet_status: str | None = None,
        limit: int = 50,
    ) -> AdminEvidencePacketListResponse:
        with get_connection() as connection:
            rows = self._fetch_packet_summary_rows(
                connection,
                county_id=county_id,
                tax_year=tax_year,
                packet_status=packet_status,
                limit=limit,
            )
        return AdminEvidencePacketListResponse(
            county_id=county_id,
            tax_year=tax_year,
            packet_status=packet_status,
            packets=[self._build_packet_summary(row) for row in rows],
        )

    def get_packet_detail(self, *, evidence_packet_id: str) -> AdminEvidencePacketDetail:
        with get_connection() as connection:
            packet_row = self._fetch_packet_detail_row(connection, evidence_packet_id=evidence_packet_id)
            if packet_row is None:
                raise LookupError(f"Missing evidence packet {evidence_packet_id}.")
            item_rows = self._fetch_packet_items(connection, evidence_packet_id=evidence_packet_id)
            comp_set_rows = self._fetch_comp_sets(connection, evidence_packet_id=evidence_packet_id)
            comp_item_rows = self._fetch_comp_set_items(connection, evidence_packet_id=evidence_packet_id)

        comp_items_by_set: dict[str, list[AdminEvidenceCompSetItem]] = defaultdict(list)
        for row in comp_item_rows:
            comp_items_by_set[str(row["evidence_comp_set_id"])].append(self._build_comp_set_item(row))

        comp_sets = []
        for row in comp_set_rows:
            comp_sets.append(
                AdminEvidenceCompSet(
                    evidence_comp_set_id=str(row["evidence_comp_set_id"]),
                    basis_type=row["basis_type"],
                    set_label=row["set_label"],
                    notes=row["notes"],
                    metadata_json=dict(row["metadata_json"] or {}),
                    items=comp_items_by_set.get(str(row["evidence_comp_set_id"]), []),
                )
            )

        return AdminEvidencePacketDetail(
            packet=self._build_packet_summary(packet_row),
            items=[self._build_packet_item(row) for row in item_rows],
            comp_sets=comp_sets,
        )

    def create_packet(self, *, request: EvidencePacketCreate) -> CaseMutationResult:
        from psycopg.types.json import Jsonb

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT parcel_id, tax_year, COALESCE(valuation_run_id, %s) AS valuation_run_id
                    FROM protest_cases
                    WHERE protest_case_id = %s
                    """,
                    (request.valuation_run_id, request.protest_case_id),
                )
                case_row = cursor.fetchone()
                if case_row is None:
                    raise LookupError(f"Missing protest case {request.protest_case_id}.")

                cursor.execute(
                    """
                    INSERT INTO evidence_packets (
                      protest_case_id,
                      parcel_id,
                      tax_year,
                      packet_type,
                      packet_status,
                      valuation_run_id,
                      storage_path,
                      packet_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING evidence_packet_id
                    """,
                    (
                        request.protest_case_id,
                        case_row["parcel_id"],
                        case_row["tax_year"],
                        request.packet_type,
                        request.packet_status,
                        case_row["valuation_run_id"],
                        request.storage_path,
                        Jsonb(request.packet_json),
                    ),
                )
                packet_row = cursor.fetchone()
                evidence_packet_id = str(packet_row["evidence_packet_id"])

                for item in request.items:
                    cursor.execute(
                        """
                        INSERT INTO evidence_packet_items (
                          evidence_packet_id,
                          item_type,
                          section_code,
                          title,
                          body_text,
                          source_basis,
                          display_order,
                          metadata_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            evidence_packet_id,
                            item.item_type,
                            item.section_code,
                            item.title,
                            item.body_text,
                            item.source_basis,
                            item.display_order,
                            Jsonb(item.metadata_json),
                        ),
                    )

                for comp_set in request.comp_sets:
                    cursor.execute(
                        """
                        INSERT INTO evidence_comp_sets (
                          evidence_packet_id,
                          basis_type,
                          set_label,
                          notes,
                          metadata_json
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING evidence_comp_set_id
                        """,
                        (
                            evidence_packet_id,
                            comp_set.basis_type,
                            comp_set.set_label,
                            comp_set.notes,
                            Jsonb(comp_set.metadata_json),
                        ),
                    )
                    comp_set_row = cursor.fetchone()
                    comp_set_id = comp_set_row["evidence_comp_set_id"]
                    for comp_item in comp_set.items:
                        cursor.execute(
                            """
                            INSERT INTO evidence_comp_set_items (
                              evidence_comp_set_id,
                              parcel_sale_id,
                              parcel_id,
                              comp_role,
                              comp_rank,
                              rationale_text,
                              adjustment_summary_json
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                comp_set_id,
                                comp_item.parcel_sale_id,
                                comp_item.parcel_id,
                                comp_item.comp_role,
                                comp_item.comp_rank,
                                comp_item.rationale_text,
                                Jsonb(comp_item.adjustment_summary_json),
                            ),
                        )
            connection.commit()

        return CaseMutationResult(
            action="create_evidence_packet",
            protest_case_id=str(request.protest_case_id),
            evidence_packet_id=evidence_packet_id,
            message="Created evidence packet with canonical section and comp-set support rows.",
        )

    def _fetch_case_summary_rows(
        self,
        connection: Any,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        case_status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conditions = ["1=1"]
        params: list[Any] = []
        if county_id is not None:
            conditions.append("p.county_id = %s")
            params.append(county_id)
        if tax_year is not None:
            conditions.append("pc.tax_year = %s")
            params.append(tax_year)
        if case_status is not None:
            conditions.append("pc.case_status = %s")
            params.append(case_status)
        params.append(limit)

        sql = f"""
            SELECT
              pc.protest_case_id,
              p.county_id,
              pc.parcel_id,
              p.account_number,
              pc.tax_year,
              pc.case_status,
              pc.workflow_status_code,
              COALESCE(pa.situs_address, p.situs_address, '') ||
                CASE
                  WHEN COALESCE(pa.situs_city, p.situs_city) IS NOT NULL
                  THEN ', ' || COALESCE(pa.situs_city, p.situs_city) || ', ' || COALESCE(pa.situs_state, p.situs_state, 'TX')
                  ELSE ''
                END ||
                CASE
                  WHEN COALESCE(pa.situs_zip, p.situs_zip) IS NOT NULL
                  THEN ' ' || COALESCE(pa.situs_zip, p.situs_zip)
                  ELSE ''
                END AS address,
              p.owner_name,
              pc.client_id,
              NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '') AS client_name,
              pc.representation_agreement_id,
              pc.valuation_run_id,
              COALESCE(packet_counts.packet_count, 0) AS packet_count,
              COALESCE(note_counts.note_count, 0) AS note_count,
              COALESCE(hearing_counts.hearing_count, 0) AS hearing_count,
              latest_outcome.outcome_code AS latest_outcome_code,
              latest_outcome.outcome_date,
              qr.protest_recommendation AS recommendation_code,
              qr.expected_tax_savings_point,
              pc.created_at,
              pc.updated_at
            FROM protest_cases pc
            JOIN parcels p ON p.parcel_id = pc.parcel_id
            LEFT JOIN parcel_addresses pa
              ON pa.parcel_id = pc.parcel_id
             AND pa.is_current = true
            LEFT JOIN clients c ON c.client_id = pc.client_id
            LEFT JOIN LATERAL (
              SELECT COUNT(*) AS packet_count
              FROM evidence_packets ep
              WHERE ep.protest_case_id = pc.protest_case_id
            ) packet_counts ON true
            LEFT JOIN LATERAL (
              SELECT COUNT(*) AS note_count
              FROM case_notes cn
              WHERE cn.protest_case_id = pc.protest_case_id
            ) note_counts ON true
            LEFT JOIN LATERAL (
              SELECT COUNT(*) AS hearing_count
              FROM hearings h
              WHERE h.protest_case_id = pc.protest_case_id
            ) hearing_counts ON true
            LEFT JOIN LATERAL (
              SELECT co.outcome_code, co.outcome_date
              FROM case_outcomes co
              WHERE co.protest_case_id = pc.protest_case_id
              ORDER BY co.outcome_date DESC NULLS LAST, co.created_at DESC NULLS LAST
              LIMIT 1
            ) latest_outcome ON true
            LEFT JOIN v_quote_read_model qr
              ON qr.county_id = p.county_id
             AND qr.account_number = p.account_number
             AND qr.tax_year = pc.tax_year
            WHERE {" AND ".join(conditions)}
            ORDER BY pc.updated_at DESC NULLS LAST, pc.created_at DESC
            LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall()

    def _fetch_case_detail_row(self, connection: Any, *, protest_case_id: str) -> dict[str, Any] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  pc.protest_case_id,
                  p.county_id,
                  pc.parcel_id,
                  p.account_number,
                  pc.tax_year,
                  pc.case_status,
                  pc.workflow_status_code,
                  COALESCE(pa.situs_address, p.situs_address, '') ||
                    CASE
                      WHEN COALESCE(pa.situs_city, p.situs_city) IS NOT NULL
                      THEN ', ' || COALESCE(pa.situs_city, p.situs_city) || ', ' || COALESCE(pa.situs_state, p.situs_state, 'TX')
                      ELSE ''
                    END ||
                    CASE
                      WHEN COALESCE(pa.situs_zip, p.situs_zip) IS NOT NULL
                      THEN ' ' || COALESCE(pa.situs_zip, p.situs_zip)
                      ELSE ''
                    END AS address,
                  p.owner_name,
                  pc.client_id,
                  NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '') AS client_name,
                  pc.representation_agreement_id,
                  pc.valuation_run_id,
                  COALESCE(packet_counts.packet_count, 0) AS packet_count,
                  COALESCE(note_counts.note_count, 0) AS note_count,
                  COALESCE(hearing_counts.hearing_count, 0) AS hearing_count,
                  latest_outcome.outcome_code AS latest_outcome_code,
                  latest_outcome.outcome_date,
                  qr.protest_recommendation AS recommendation_code,
                  qr.expected_tax_savings_point,
                  pc.created_at,
                  pc.updated_at
                FROM protest_cases pc
                JOIN parcels p ON p.parcel_id = pc.parcel_id
                LEFT JOIN parcel_addresses pa
                  ON pa.parcel_id = pc.parcel_id
                 AND pa.is_current = true
                LEFT JOIN clients c ON c.client_id = pc.client_id
                LEFT JOIN LATERAL (
                  SELECT COUNT(*) AS packet_count
                  FROM evidence_packets ep
                  WHERE ep.protest_case_id = pc.protest_case_id
                ) packet_counts ON true
                LEFT JOIN LATERAL (
                  SELECT COUNT(*) AS note_count
                  FROM case_notes cn
                  WHERE cn.protest_case_id = pc.protest_case_id
                ) note_counts ON true
                LEFT JOIN LATERAL (
                  SELECT COUNT(*) AS hearing_count
                  FROM hearings h
                  WHERE h.protest_case_id = pc.protest_case_id
                ) hearing_counts ON true
                LEFT JOIN LATERAL (
                  SELECT co.outcome_code, co.outcome_date
                  FROM case_outcomes co
                  WHERE co.protest_case_id = pc.protest_case_id
                  ORDER BY co.outcome_date DESC NULLS LAST, co.created_at DESC NULLS LAST
                  LIMIT 1
                ) latest_outcome ON true
                LEFT JOIN v_quote_read_model qr
                  ON qr.county_id = p.county_id
                 AND qr.account_number = p.account_number
                 AND qr.tax_year = pc.tax_year
                WHERE pc.protest_case_id = %s
                """,
                (protest_case_id,),
            )
            return cursor.fetchone()

    def _fetch_case_notes(self, connection: Any, *, protest_case_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  case_note_id,
                  note_text,
                  note_code,
                  author_label,
                  created_at,
                  updated_at
                FROM case_notes
                WHERE protest_case_id = %s
                ORDER BY created_at DESC
                """,
                (protest_case_id,),
            )
            return cursor.fetchall()

    def _fetch_case_assignments(self, connection: Any, *, protest_case_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  case_assignment_id,
                  assignee_name,
                  assignee_role,
                  assignment_status,
                  assigned_at,
                  due_at,
                  active_flag,
                  metadata_json
                FROM case_assignments
                WHERE protest_case_id = %s
                ORDER BY active_flag DESC, assigned_at DESC
                """,
                (protest_case_id,),
            )
            return cursor.fetchall()

    def _fetch_hearings(self, connection: Any, *, protest_case_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  hearing_id,
                  hearing_type_code,
                  hearing_status,
                  scheduled_at,
                  location_text,
                  hearing_reference,
                  result_summary
                FROM hearings
                WHERE protest_case_id = %s
                ORDER BY scheduled_at DESC NULLS LAST, created_at DESC
                """,
                (protest_case_id,),
            )
            return cursor.fetchall()

    def _fetch_case_status_history(
        self,
        connection: Any,
        *,
        protest_case_id: str,
    ) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  case_status_history_id,
                  workflow_status_code,
                  case_status,
                  reason_text,
                  changed_by,
                  created_at
                FROM case_status_history
                WHERE protest_case_id = %s
                ORDER BY created_at DESC
                """,
                (protest_case_id,),
            )
            return cursor.fetchall()

    def _fetch_packet_summary_rows(
        self,
        connection: Any,
        *,
        protest_case_id: str | None = None,
        county_id: str | None = None,
        tax_year: int | None = None,
        packet_status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conditions = ["1=1"]
        params: list[Any] = []
        if protest_case_id is not None:
            conditions.append("ep.protest_case_id = %s")
            params.append(protest_case_id)
        if county_id is not None:
            conditions.append("p.county_id = %s")
            params.append(county_id)
        if tax_year is not None:
            conditions.append("ep.tax_year = %s")
            params.append(tax_year)
        if packet_status is not None:
            conditions.append("ep.packet_status = %s")
            params.append(packet_status)
        params.append(limit)

        sql = f"""
            SELECT
              ep.evidence_packet_id,
              ep.protest_case_id,
              p.county_id,
              ep.parcel_id,
              p.account_number,
              ep.tax_year,
              ep.packet_type,
              ep.packet_status,
              ep.valuation_run_id,
              COALESCE(pa.situs_address, p.situs_address, '') ||
                CASE
                  WHEN COALESCE(pa.situs_city, p.situs_city) IS NOT NULL
                  THEN ', ' || COALESCE(pa.situs_city, p.situs_city) || ', ' || COALESCE(pa.situs_state, p.situs_state, 'TX')
                  ELSE ''
                END ||
                CASE
                  WHEN COALESCE(pa.situs_zip, p.situs_zip) IS NOT NULL
                  THEN ' ' || COALESCE(pa.situs_zip, p.situs_zip)
                  ELSE ''
                END AS address,
              pc.case_status,
              COALESCE(item_counts.item_count, 0) AS item_count,
              COALESCE(comp_counts.comp_set_count, 0) AS comp_set_count,
              ep.generated_at,
              ep.created_at,
              ep.updated_at
            FROM evidence_packets ep
            JOIN parcels p ON p.parcel_id = ep.parcel_id
            LEFT JOIN parcel_addresses pa
              ON pa.parcel_id = ep.parcel_id
             AND pa.is_current = true
            LEFT JOIN protest_cases pc ON pc.protest_case_id = ep.protest_case_id
            LEFT JOIN LATERAL (
              SELECT COUNT(*) AS item_count
              FROM evidence_packet_items epi
              WHERE epi.evidence_packet_id = ep.evidence_packet_id
            ) item_counts ON true
            LEFT JOIN LATERAL (
              SELECT COUNT(*) AS comp_set_count
              FROM evidence_comp_sets ecs
              WHERE ecs.evidence_packet_id = ep.evidence_packet_id
            ) comp_counts ON true
            WHERE {" AND ".join(conditions)}
            ORDER BY ep.updated_at DESC NULLS LAST, ep.created_at DESC
            LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall()

    def _fetch_packet_detail_row(self, connection: Any, *, evidence_packet_id: str) -> dict[str, Any] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ep.evidence_packet_id,
                  ep.protest_case_id,
                  p.county_id,
                  ep.parcel_id,
                  p.account_number,
                  ep.tax_year,
                  ep.packet_type,
                  ep.packet_status,
                  ep.valuation_run_id,
                  COALESCE(pa.situs_address, p.situs_address, '') ||
                    CASE
                      WHEN COALESCE(pa.situs_city, p.situs_city) IS NOT NULL
                      THEN ', ' || COALESCE(pa.situs_city, p.situs_city) || ', ' || COALESCE(pa.situs_state, p.situs_state, 'TX')
                      ELSE ''
                    END ||
                    CASE
                      WHEN COALESCE(pa.situs_zip, p.situs_zip) IS NOT NULL
                      THEN ' ' || COALESCE(pa.situs_zip, p.situs_zip)
                      ELSE ''
                    END AS address,
                  pc.case_status,
                  COALESCE(item_counts.item_count, 0) AS item_count,
                  COALESCE(comp_counts.comp_set_count, 0) AS comp_set_count,
                  ep.generated_at,
                  ep.created_at,
                  ep.updated_at
                FROM evidence_packets ep
                JOIN parcels p ON p.parcel_id = ep.parcel_id
                LEFT JOIN parcel_addresses pa
                  ON pa.parcel_id = ep.parcel_id
                 AND pa.is_current = true
                LEFT JOIN protest_cases pc ON pc.protest_case_id = ep.protest_case_id
                LEFT JOIN LATERAL (
                  SELECT COUNT(*) AS item_count
                  FROM evidence_packet_items epi
                  WHERE epi.evidence_packet_id = ep.evidence_packet_id
                ) item_counts ON true
                LEFT JOIN LATERAL (
                  SELECT COUNT(*) AS comp_set_count
                  FROM evidence_comp_sets ecs
                  WHERE ecs.evidence_packet_id = ep.evidence_packet_id
                ) comp_counts ON true
                WHERE ep.evidence_packet_id = %s
                """,
                (evidence_packet_id,),
            )
            return cursor.fetchone()

    def _fetch_packet_items(self, connection: Any, *, evidence_packet_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  evidence_packet_item_id,
                  item_type,
                  section_code,
                  title,
                  body_text,
                  source_basis,
                  display_order,
                  metadata_json
                FROM evidence_packet_items
                WHERE evidence_packet_id = %s
                ORDER BY display_order ASC, created_at ASC
                """,
                (evidence_packet_id,),
            )
            return cursor.fetchall()

    def _fetch_comp_sets(self, connection: Any, *, evidence_packet_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  evidence_comp_set_id,
                  basis_type,
                  set_label,
                  notes,
                  metadata_json
                FROM evidence_comp_sets
                WHERE evidence_packet_id = %s
                ORDER BY created_at ASC
                """,
                (evidence_packet_id,),
            )
            return cursor.fetchall()

    def _fetch_comp_set_items(self, connection: Any, *, evidence_packet_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ecsi.evidence_comp_set_item_id,
                  ecsi.evidence_comp_set_id,
                  ecsi.parcel_sale_id,
                  ecsi.parcel_id,
                  ecsi.comp_role,
                  ecsi.comp_rank,
                  ecsi.rationale_text,
                  ecsi.adjustment_summary_json
                FROM evidence_comp_set_items ecsi
                JOIN evidence_comp_sets ecs
                  ON ecs.evidence_comp_set_id = ecsi.evidence_comp_set_id
                WHERE ecs.evidence_packet_id = %s
                ORDER BY ecsi.comp_rank ASC NULLS LAST, ecsi.created_at ASC
                """,
                (evidence_packet_id,),
            )
            return cursor.fetchall()

    def _build_case_summary(self, row: dict[str, Any]) -> AdminCaseSummary:
        return AdminCaseSummary(
            protest_case_id=str(row["protest_case_id"]),
            county_id=row["county_id"],
            parcel_id=str(row["parcel_id"]),
            account_number=row["account_number"],
            tax_year=row["tax_year"],
            case_status=row["case_status"],
            workflow_status_code=row.get("workflow_status_code"),
            address=row.get("address"),
            owner_name=row.get("owner_name"),
            client_id=str(row["client_id"]) if row.get("client_id") is not None else None,
            client_name=row.get("client_name"),
            representation_agreement_id=(
                str(row["representation_agreement_id"])
                if row.get("representation_agreement_id") is not None
                else None
            ),
            valuation_run_id=str(row["valuation_run_id"]) if row.get("valuation_run_id") is not None else None,
            packet_count=int(row.get("packet_count") or 0),
            note_count=int(row.get("note_count") or 0),
            hearing_count=int(row.get("hearing_count") or 0),
            latest_outcome_code=row.get("latest_outcome_code"),
            outcome_date=row.get("outcome_date"),
            recommendation_code=row.get("recommendation_code"),
            expected_tax_savings_point=(
                float(row["expected_tax_savings_point"])
                if row.get("expected_tax_savings_point") is not None
                else None
            ),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    def _build_case_note(self, row: dict[str, Any]) -> AdminCaseNote:
        return AdminCaseNote(
            case_note_id=str(row["case_note_id"]),
            note_text=row["note_text"],
            note_code=row.get("note_code") or "general",
            author_label=row.get("author_label"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    def _build_case_assignment(self, row: dict[str, Any]) -> AdminCaseAssignment:
        return AdminCaseAssignment(
            case_assignment_id=str(row["case_assignment_id"]),
            assignee_name=row["assignee_name"],
            assignee_role=row["assignee_role"],
            assignment_status=row["assignment_status"],
            assigned_at=row.get("assigned_at"),
            due_at=row.get("due_at"),
            active_flag=bool(row["active_flag"]),
            metadata_json=dict(row.get("metadata_json") or {}),
        )

    def _build_hearing_summary(self, row: dict[str, Any]) -> AdminHearingSummary:
        return AdminHearingSummary(
            hearing_id=str(row["hearing_id"]),
            hearing_type_code=row["hearing_type_code"],
            hearing_status=row["hearing_status"],
            scheduled_at=row.get("scheduled_at"),
            location_text=row.get("location_text"),
            hearing_reference=row.get("hearing_reference"),
            result_summary=row.get("result_summary"),
        )

    def _build_case_status_history(self, row: dict[str, Any]) -> AdminCaseStatusHistoryEntry:
        return AdminCaseStatusHistoryEntry(
            case_status_history_id=str(row["case_status_history_id"]),
            workflow_status_code=row.get("workflow_status_code"),
            case_status=row["case_status"],
            reason_text=row.get("reason_text"),
            changed_by=row.get("changed_by"),
            created_at=row.get("created_at"),
        )

    def _build_packet_summary(self, row: dict[str, Any]) -> AdminEvidencePacketSummary:
        return AdminEvidencePacketSummary(
            evidence_packet_id=str(row["evidence_packet_id"]),
            protest_case_id=str(row["protest_case_id"]) if row.get("protest_case_id") is not None else None,
            county_id=row["county_id"],
            parcel_id=str(row["parcel_id"]),
            account_number=row["account_number"],
            tax_year=row["tax_year"],
            packet_type=row["packet_type"],
            packet_status=row["packet_status"],
            valuation_run_id=str(row["valuation_run_id"]) if row.get("valuation_run_id") is not None else None,
            address=row.get("address"),
            case_status=row.get("case_status"),
            item_count=int(row.get("item_count") or 0),
            comp_set_count=int(row.get("comp_set_count") or 0),
            generated_at=row.get("generated_at"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    def _build_packet_item(self, row: dict[str, Any]) -> AdminEvidencePacketItem:
        return AdminEvidencePacketItem(
            evidence_packet_item_id=str(row["evidence_packet_item_id"]),
            item_type=row["item_type"],
            section_code=row["section_code"],
            title=row["title"],
            body_text=row.get("body_text"),
            source_basis=row.get("source_basis"),
            display_order=int(row["display_order"]),
            metadata_json=dict(row.get("metadata_json") or {}),
        )

    def _build_comp_set_item(self, row: dict[str, Any]) -> AdminEvidenceCompSetItem:
        return AdminEvidenceCompSetItem(
            evidence_comp_set_item_id=str(row["evidence_comp_set_item_id"]),
            parcel_sale_id=str(row["parcel_sale_id"]) if row.get("parcel_sale_id") is not None else None,
            parcel_id=str(row["parcel_id"]) if row.get("parcel_id") is not None else None,
            comp_role=row["comp_role"],
            comp_rank=row.get("comp_rank"),
            rationale_text=row.get("rationale_text"),
            adjustment_summary_json=dict(row.get("adjustment_summary_json") or {}),
        )
