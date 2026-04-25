from __future__ import annotations

from datetime import date
from typing import Any

from app.db.connection import get_connection
from app.models.admin import (
    AdminLeadAttributionSnapshot,
    AdminLeadContactSnapshot,
    AdminLeadDemandBucketSummary,
    AdminLeadDetail,
    AdminLeadDuplicateGroupSummary,
    AdminLeadDuplicatePeer,
    AdminLeadListResponse,
    AdminLeadQuoteContextSnapshot,
    AdminLeadReportingKpiSummary,
    AdminLeadSummary,
)


class AdminLeadReportingService:
    def list_leads(
        self,
        *,
        county_id: str | None = None,
        requested_tax_year: int | None = None,
        served_tax_year: int | None = None,
        demand_bucket: str | None = None,
        fallback_applied: bool | None = None,
        source_channel: str | None = None,
        duplicate_only: bool = False,
        quote_ready_only: bool = False,
        submitted_from: date | None = None,
        submitted_to: date | None = None,
        limit: int = 50,
    ) -> AdminLeadListResponse:
        filters = self._build_filter_context(
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            served_tax_year=served_tax_year,
            demand_bucket=demand_bucket,
            fallback_applied=fallback_applied,
            source_channel=source_channel,
            quote_ready_only=quote_ready_only,
            submitted_from=submitted_from,
            submitted_to=submitted_to,
        )

        with get_connection() as connection:
            with connection.cursor() as cursor:
                lead_rows = self._fetch_lead_rows(
                    cursor,
                    filters=filters,
                    duplicate_only=duplicate_only,
                    limit=limit,
                )
                kpi_row = self._fetch_kpi_summary(
                    cursor,
                    filters=filters,
                    duplicate_only=duplicate_only,
                )
                demand_bucket_rows = self._fetch_demand_bucket_rows(
                    cursor,
                    filters=filters,
                    duplicate_only=duplicate_only,
                )
                duplicate_group_rows = self._fetch_duplicate_group_rows(
                    cursor,
                    filters=filters,
                    duplicate_only=duplicate_only,
                )

        return AdminLeadListResponse(
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            served_tax_year=served_tax_year,
            demand_bucket=demand_bucket,
            fallback_applied=fallback_applied,
            source_channel=source_channel,
            duplicate_only=duplicate_only,
            quote_ready_only=quote_ready_only,
            submitted_from=submitted_from,
            submitted_to=submitted_to,
            limit=limit,
            kpi_summary=AdminLeadReportingKpiSummary(
                total_count=_as_int(kpi_row.get("total_count")),
                quote_ready_count=_as_int(kpi_row.get("quote_ready_count")),
                reachable_unquoted_count=_as_int(kpi_row.get("reachable_unquoted_count")),
                unsupported_county_count=_as_int(kpi_row.get("unsupported_county_count")),
                unsupported_property_count=_as_int(kpi_row.get("unsupported_property_count")),
                fallback_applied_count=_as_int(kpi_row.get("fallback_applied_count")),
                duplicate_group_count=_as_int(kpi_row.get("duplicate_group_count")),
            ),
            demand_buckets=[
                AdminLeadDemandBucketSummary(
                    demand_bucket=row["demand_bucket"],
                    lead_count=_as_int(row.get("lead_count")),
                )
                for row in demand_bucket_rows
            ],
            duplicate_groups=[
                self._build_duplicate_group_summary(row) for row in duplicate_group_rows
            ],
            leads=[self._build_lead_summary(row) for row in lead_rows],
        )

    def get_lead_detail(self, *, lead_id: str) -> AdminLeadDetail:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                detail_row = self._fetch_lead_detail_row(cursor, lead_id=lead_id)
                if detail_row is None:
                    raise LookupError(f"Missing lead {lead_id}.")
                duplicate_peer_rows = self._fetch_duplicate_peer_rows(
                    cursor,
                    duplicate_group_key=detail_row["duplicate_group_key"],
                    lead_id=lead_id,
                )

        return AdminLeadDetail(
            lead=self._build_lead_summary(detail_row),
            contact=AdminLeadContactSnapshot(
                owner_name=detail_row.get("owner_name"),
                email=detail_row.get("email"),
                phone=detail_row.get("phone"),
                email_present=_as_bool(detail_row.get("email_present")),
                phone_present=_as_bool(detail_row.get("phone_present")),
                consent_to_contact=_as_bool(detail_row.get("contact_consent_to_contact")),
            ),
            quote_context=AdminLeadQuoteContextSnapshot(
                context_status=detail_row["context_status"],
                demand_bucket=detail_row["demand_bucket"],
                county_supported=_as_bool(detail_row.get("county_supported")),
                property_supported=_as_optional_bool(detail_row.get("property_supported")),
                quote_ready=_as_bool(detail_row.get("quote_ready")),
                requested_tax_year=_as_int(detail_row.get("requested_tax_year")),
                served_tax_year=_as_optional_int(detail_row.get("served_tax_year")),
                tax_year_fallback_applied=_as_bool(detail_row.get("tax_year_fallback_applied")),
                tax_year_fallback_reason=detail_row.get("tax_year_fallback_reason"),
                parcel_id=detail_row.get("parcel_id"),
                property_type_code=detail_row.get("property_type_code"),
                protest_recommendation=detail_row.get("protest_recommendation"),
                expected_tax_savings_point=_as_optional_float(
                    detail_row.get("expected_tax_savings_point")
                ),
                defensible_value_point=_as_optional_float(
                    detail_row.get("defensible_value_point")
                ),
            ),
            attribution=AdminLeadAttributionSnapshot(
                anonymous_session_id=detail_row.get("anonymous_session_id"),
                funnel_stage=detail_row.get("funnel_stage"),
                utm_source=_json_value(detail_row.get("utm_payload"), "source"),
                utm_medium=_json_value(detail_row.get("utm_payload"), "medium"),
                utm_campaign=_json_value(detail_row.get("utm_payload"), "campaign"),
                utm_term=_json_value(detail_row.get("utm_payload"), "term"),
                utm_content=_json_value(detail_row.get("utm_payload"), "content"),
            ),
            raw_event_payload=_as_json_dict(detail_row.get("raw_event_payload")),
            duplicate_peers=[
                AdminLeadDuplicatePeer(
                    lead_id=row["lead_id"],
                    submitted_at=row.get("event_created_at"),
                    demand_bucket=row["demand_bucket"],
                    context_status=row["context_status"],
                    served_tax_year=_as_optional_int(row.get("served_tax_year")),
                    fallback_applied=_as_bool(row.get("tax_year_fallback_applied")),
                    source_channel=row.get("source_channel"),
                )
                for row in duplicate_peer_rows
            ],
        )

    def _fetch_lead_rows(
        self,
        cursor: Any,
        *,
        filters: _LeadFilterContext,
        duplicate_only: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        query, params = self._compose_query(
            """
            SELECT *
            FROM final
            ORDER BY event_created_at DESC, lead_id DESC
            LIMIT %s
            """,
            filters=filters,
            duplicate_only=duplicate_only,
            extra_params=[limit],
        )
        cursor.execute(query, params)
        return list(cursor.fetchall())

    def _fetch_kpi_summary(
        self,
        cursor: Any,
        *,
        filters: _LeadFilterContext,
        duplicate_only: bool,
    ) -> dict[str, Any]:
        query, params = self._compose_query(
            """
            SELECT
              COUNT(*) AS total_count,
              COUNT(*) FILTER (WHERE demand_bucket = 'quote_ready_demand') AS quote_ready_count,
              COUNT(*) FILTER (WHERE demand_bucket = 'reachable_unquoted_demand') AS reachable_unquoted_count,
              COUNT(*) FILTER (WHERE demand_bucket = 'unsupported_county_demand') AS unsupported_county_count,
              COUNT(*) FILTER (WHERE demand_bucket = 'unsupported_property_demand') AS unsupported_property_count,
              COUNT(*) FILTER (WHERE tax_year_fallback_applied) AS fallback_applied_count,
              COUNT(DISTINCT duplicate_group_key) FILTER (WHERE duplicate_group_size > 1) AS duplicate_group_count
            FROM final
            """,
            filters=filters,
            duplicate_only=duplicate_only,
        )
        cursor.execute(query, params)
        row = cursor.fetchone()
        return row if row is not None else {}

    def _fetch_demand_bucket_rows(
        self,
        cursor: Any,
        *,
        filters: _LeadFilterContext,
        duplicate_only: bool,
    ) -> list[dict[str, Any]]:
        query, params = self._compose_query(
            """
            SELECT demand_bucket, COUNT(*) AS lead_count
            FROM final
            GROUP BY demand_bucket
            ORDER BY lead_count DESC, demand_bucket ASC
            """,
            filters=filters,
            duplicate_only=duplicate_only,
        )
        cursor.execute(query, params)
        return list(cursor.fetchall())

    def _fetch_duplicate_group_rows(
        self,
        cursor: Any,
        *,
        filters: _LeadFilterContext,
        duplicate_only: bool,
    ) -> list[dict[str, Any]]:
        query, params = self._compose_query(
            """
            SELECT
              duplicate_group_key,
              (ARRAY_AGG(lead_id ORDER BY event_created_at DESC, lead_id DESC))[1] AS latest_lead_id,
              county_id,
              account_number,
              requested_tax_year,
              COUNT(*) AS lead_count,
              MAX(event_created_at) AS latest_submitted_at,
              (ARRAY_AGG(demand_bucket ORDER BY event_created_at DESC, lead_id DESC))[1] AS latest_demand_bucket,
              BOOL_OR(tax_year_fallback_applied) AS fallback_present,
              COUNT(DISTINCT demand_bucket) AS demand_bucket_count
            FROM final
            WHERE duplicate_group_size > 1
            GROUP BY duplicate_group_key, county_id, account_number, requested_tax_year
            ORDER BY latest_submitted_at DESC, account_number ASC
            LIMIT 25
            """,
            filters=filters,
            duplicate_only=False,
        )
        cursor.execute(query, params)
        return list(cursor.fetchall())

    def _fetch_lead_detail_row(self, cursor: Any, *, lead_id: str) -> dict[str, Any] | None:
        filter_context = _LeadFilterContext(
            where_sql="lead_id = %s",
            params=[lead_id],
        )
        query, params = self._compose_query(
            """
            SELECT *
            FROM final
            LIMIT 1
            """,
            filters=filter_context,
            duplicate_only=False,
        )
        cursor.execute(query, params)
        return cursor.fetchone()

    def _fetch_duplicate_peer_rows(
        self,
        cursor: Any,
        *,
        duplicate_group_key: str,
        lead_id: str,
    ) -> list[dict[str, Any]]:
        filter_context = _LeadFilterContext(
            where_sql="duplicate_group_key = %s",
            params=[duplicate_group_key],
        )
        query, params = self._compose_query(
            """
            SELECT *
            FROM final
            WHERE lead_id <> %s
            ORDER BY event_created_at DESC, lead_id DESC
            LIMIT 25
            """,
            filters=filter_context,
            duplicate_only=False,
            extra_params=[lead_id],
        )
        cursor.execute(query, params)
        return list(cursor.fetchall())

    def _compose_query(
        self,
        select_sql: str,
        *,
        filters: _LeadFilterContext,
        duplicate_only: bool,
        extra_params: list[Any] | None = None,
    ) -> tuple[str, list[Any]]:
        duplicate_only_sql = "AND duplicate_group_size > 1" if duplicate_only else ""
        query = f"""
        WITH lead_base AS (
          SELECT
            l.lead_id::text AS lead_id,
            le.lead_event_id::text AS lead_event_id,
            l.created_at AS lead_created_at,
            le.created_at AS event_created_at,
            COALESCE(NULLIF(le.event_payload->'quote_context'->>'county_id', ''), l.county_id) AS county_id,
            COALESCE(
              NULLIF(le.event_payload->'quote_context'->>'requested_tax_year', '')::int,
              l.tax_year
            ) AS requested_tax_year,
            NULLIF(le.event_payload->'quote_context'->>'served_tax_year', '')::int AS served_tax_year,
            COALESCE(
              NULLIF(le.event_payload->'quote_context'->>'tax_year_fallback_applied', '')::boolean,
              false
            ) AS tax_year_fallback_applied,
            NULLIF(le.event_payload->'quote_context'->>'tax_year_fallback_reason', '') AS tax_year_fallback_reason,
            NULLIF(le.event_payload->'quote_context'->>'status', '') AS context_status,
            CASE NULLIF(le.event_payload->'quote_context'->>'status', '')
              WHEN 'quote_ready' THEN 'quote_ready_demand'
              WHEN 'missing_quote_ready_row' THEN 'reachable_unquoted_demand'
              WHEN 'unsupported_property_type' THEN 'unsupported_property_demand'
              WHEN 'unsupported_county' THEN 'unsupported_county_demand'
              ELSE 'unclassified'
            END AS demand_bucket,
            l.account_number,
            l.owner_name,
            l.email,
            l.phone,
            l.source_channel,
            l.consent_to_contact,
            COALESCE(
              NULLIF(le.event_payload->'contact'->>'email_present', '')::boolean,
              l.email IS NOT NULL
            ) AS email_present,
            COALESCE(
              NULLIF(le.event_payload->'contact'->>'phone_present', '')::boolean,
              l.phone IS NOT NULL
            ) AS phone_present,
            COALESCE(
              NULLIF(le.event_payload->'contact'->>'consent_to_contact', '')::boolean,
              l.consent_to_contact
            ) AS contact_consent_to_contact,
            COALESCE(
              NULLIF(le.event_payload->'quote_context'->>'county_supported', '')::boolean,
              false
            ) AS county_supported,
            CASE
              WHEN le.event_payload->'quote_context' ? 'property_supported'
                THEN NULLIF(le.event_payload->'quote_context'->>'property_supported', '')::boolean
              ELSE NULL
            END AS property_supported,
            COALESCE(
              NULLIF(le.event_payload->'quote_context'->>'quote_ready', '')::boolean,
              false
            ) AS quote_ready,
            NULLIF(le.event_payload->'quote_context'->>'parcel_id', '') AS parcel_id,
            NULLIF(le.event_payload->'quote_context'->>'property_type_code', '') AS property_type_code,
            NULLIF(le.event_payload->'quote_context'->>'protest_recommendation', '') AS protest_recommendation,
            NULLIF(le.event_payload->'quote_context'->>'expected_tax_savings_point', '')::numeric AS expected_tax_savings_point,
            NULLIF(le.event_payload->'quote_context'->>'defensible_value_point', '')::numeric AS defensible_value_point,
            NULLIF(le.event_payload->>'anonymous_session_id', '') AS anonymous_session_id,
            NULLIF(le.event_payload->>'funnel_stage', '') AS funnel_stage,
            COALESCE(le.event_payload->'utm', '{{}}'::jsonb) AS utm_payload,
            COALESCE(le.event_payload->'contact', '{{}}'::jsonb) AS contact_payload,
            COALESCE(le.event_payload->'quote_context', '{{}}'::jsonb) AS quote_context_payload,
            COALESCE(le.event_payload, '{{}}'::jsonb) AS raw_event_payload,
            concat_ws(
              '|',
              COALESCE(NULLIF(le.event_payload->'quote_context'->>'county_id', ''), l.county_id, ''),
              COALESCE(l.account_number, ''),
              COALESCE(
                NULLIF(le.event_payload->'quote_context'->>'requested_tax_year', ''),
                l.tax_year::text,
                ''
              )
            ) AS duplicate_group_key
          FROM leads l
          JOIN lead_events le
            ON le.lead_id = l.lead_id
           AND le.event_code = 'lead_submitted'
        ),
        pre_filtered AS (
          SELECT *
          FROM lead_base
          WHERE {filters.where_sql}
        ),
        final AS (
          SELECT
            pre_filtered.*,
            COUNT(*) OVER (PARTITION BY duplicate_group_key) AS duplicate_group_size
          FROM pre_filtered
        )
        {select_sql}
        """
        if duplicate_only:
            query = query.replace("FROM final", f"FROM final\nWHERE 1=1 {duplicate_only_sql}", 1)
        params = list(filters.params)
        if extra_params:
            params.extend(extra_params)
        return query, params

    def _build_filter_context(
        self,
        *,
        county_id: str | None,
        requested_tax_year: int | None,
        served_tax_year: int | None,
        demand_bucket: str | None,
        fallback_applied: bool | None,
        source_channel: str | None,
        quote_ready_only: bool,
        submitted_from: date | None,
        submitted_to: date | None,
    ) -> _LeadFilterContext:
        conditions = ["1=1"]
        params: list[Any] = []

        if county_id is not None:
            conditions.append("county_id = %s")
            params.append(county_id)
        if requested_tax_year is not None:
            conditions.append("requested_tax_year = %s")
            params.append(requested_tax_year)
        if served_tax_year is not None:
            conditions.append("served_tax_year = %s")
            params.append(served_tax_year)
        if demand_bucket is not None:
            conditions.append("demand_bucket = %s")
            params.append(demand_bucket)
        if fallback_applied is not None:
            conditions.append("tax_year_fallback_applied = %s")
            params.append(fallback_applied)
        if source_channel is not None:
            conditions.append("source_channel = %s")
            params.append(source_channel)
        if quote_ready_only:
            conditions.append("quote_ready = %s")
            params.append(True)
        if submitted_from is not None:
            conditions.append("event_created_at::date >= %s")
            params.append(submitted_from)
        if submitted_to is not None:
            conditions.append("event_created_at::date <= %s")
            params.append(submitted_to)

        return _LeadFilterContext(where_sql=" AND ".join(conditions), params=params)

    def _build_duplicate_group_summary(
        self,
        row: dict[str, Any],
    ) -> AdminLeadDuplicateGroupSummary:
        return AdminLeadDuplicateGroupSummary(
            duplicate_group_key=row["duplicate_group_key"],
            latest_lead_id=row["latest_lead_id"],
            county_id=row["county_id"],
            account_number=row["account_number"],
            requested_tax_year=_as_int(row.get("requested_tax_year")),
            lead_count=_as_int(row.get("lead_count")),
            latest_submitted_at=row.get("latest_submitted_at"),
            latest_demand_bucket=row.get("latest_demand_bucket"),
            fallback_present=_as_bool(row.get("fallback_present")),
            demand_bucket_count=_as_int(row.get("demand_bucket_count")),
        )

    def _build_lead_summary(self, row: dict[str, Any]) -> AdminLeadSummary:
        return AdminLeadSummary(
            lead_id=row["lead_id"],
            lead_event_id=row["lead_event_id"],
            submitted_at=row.get("event_created_at"),
            county_id=row["county_id"],
            account_number=row["account_number"],
            requested_tax_year=_as_int(row.get("requested_tax_year")),
            served_tax_year=_as_optional_int(row.get("served_tax_year")),
            demand_bucket=row["demand_bucket"],
            context_status=row["context_status"],
            source_channel=row.get("source_channel"),
            owner_name=row.get("owner_name"),
            fallback_applied=_as_bool(row.get("tax_year_fallback_applied")),
            fallback_reason=row.get("tax_year_fallback_reason"),
            email_present=_as_bool(row.get("email_present")),
            phone_present=_as_bool(row.get("phone_present")),
            consent_to_contact=_as_bool(row.get("contact_consent_to_contact")),
            duplicate_group_key=row["duplicate_group_key"],
            duplicate_group_size=_as_int(row.get("duplicate_group_size")) or 1,
        )


class _LeadFilterContext:
    def __init__(self, *, where_sql: str, params: list[Any]) -> None:
        self.where_sql = where_sql
        self.params = params


def _as_int(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_bool(value: object) -> bool:
    return bool(value)


def _as_optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _as_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_json_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _json_value(payload: object, key: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if value is None:
        return None
    return str(value)
