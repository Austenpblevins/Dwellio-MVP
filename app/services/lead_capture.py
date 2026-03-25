from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.db.connection import get_connection
from app.models.lead import LeadContextStatus, LeadCreateRequest, LeadCreateResponse

SUPPORTED_COUNTIES = {"harris", "fort_bend"}
SUPPORTED_PROPERTY_TYPES = {"sfr"}


@dataclass(frozen=True)
class LeadParcelContext:
    parcel_id: UUID | None
    requested_tax_year: int
    served_tax_year: int | None
    property_type_code: str | None

    @property
    def tax_year_fallback_applied(self) -> bool:
        return self.served_tax_year is not None and self.served_tax_year != self.requested_tax_year

    @property
    def tax_year_fallback_reason(self) -> str | None:
        if self.tax_year_fallback_applied:
            return "requested_year_unavailable"
        return None


@dataclass(frozen=True)
class LeadQuoteContext:
    served_tax_year: int
    parcel_id: UUID | None
    protest_recommendation: str | None
    expected_tax_savings_point: float | None
    defensible_value_point: float | None


@dataclass(frozen=True)
class LeadContractContext:
    status: LeadContextStatus
    county_supported: bool
    property_supported: bool | None
    quote_ready: bool
    parcel_id: UUID | None
    requested_tax_year: int
    served_tax_year: int | None
    tax_year_fallback_applied: bool
    tax_year_fallback_reason: str | None
    property_type_code: str | None = None
    protest_recommendation: str | None = None
    expected_tax_savings_point: float | None = None
    defensible_value_point: float | None = None


class LeadCaptureService:
    def create_lead(self, request: LeadCreateRequest) -> LeadCreateResponse:
        contract_context = self._resolve_contract_context(request)

        with get_connection() as connection:
            with connection.cursor() as cursor:
                lead_id = self._insert_lead(cursor, request=request, context=contract_context)
                self._insert_lead_event(
                    cursor,
                    lead_id=lead_id,
                    request=request,
                    context=contract_context,
                )
            connection.commit()

        return LeadCreateResponse(
            lead_id=lead_id,
            context_status=contract_context.status,
            county_supported=contract_context.county_supported,
            property_supported=contract_context.property_supported,
            quote_ready=contract_context.quote_ready,
            parcel_id=contract_context.parcel_id,
            requested_tax_year=contract_context.requested_tax_year,
            served_tax_year=contract_context.served_tax_year,
            tax_year_fallback_applied=contract_context.tax_year_fallback_applied,
            tax_year_fallback_reason=contract_context.tax_year_fallback_reason,
        )

    def _resolve_contract_context(self, request: LeadCreateRequest) -> LeadContractContext:
        if request.county_id not in SUPPORTED_COUNTIES:
            return LeadContractContext(
                status="unsupported_county",
                county_supported=False,
                property_supported=None,
                quote_ready=False,
                parcel_id=None,
                requested_tax_year=request.tax_year,
                served_tax_year=None,
                tax_year_fallback_applied=False,
                tax_year_fallback_reason=None,
            )

        parcel_context = self._fetch_parcel_context(request)
        property_supported = (
            parcel_context.property_type_code in SUPPORTED_PROPERTY_TYPES
            if parcel_context.property_type_code is not None
            else None
        )

        if property_supported is False:
            return LeadContractContext(
                status="unsupported_property_type",
                county_supported=True,
                property_supported=False,
                quote_ready=False,
                parcel_id=parcel_context.parcel_id,
                requested_tax_year=request.tax_year,
                served_tax_year=parcel_context.served_tax_year,
                tax_year_fallback_applied=parcel_context.tax_year_fallback_applied,
                tax_year_fallback_reason=parcel_context.tax_year_fallback_reason,
                property_type_code=parcel_context.property_type_code,
            )

        quote_context = self._fetch_quote_context(request)
        if quote_context is None:
            return LeadContractContext(
                status="missing_quote_ready_row",
                county_supported=True,
                property_supported=property_supported,
                quote_ready=False,
                parcel_id=parcel_context.parcel_id,
                requested_tax_year=request.tax_year,
                served_tax_year=parcel_context.served_tax_year,
                tax_year_fallback_applied=parcel_context.tax_year_fallback_applied,
                tax_year_fallback_reason=parcel_context.tax_year_fallback_reason,
                property_type_code=parcel_context.property_type_code,
            )

        tax_year_fallback_applied = quote_context.served_tax_year != request.tax_year
        return LeadContractContext(
            status="quote_ready",
            county_supported=True,
            property_supported=property_supported,
            quote_ready=True,
            parcel_id=quote_context.parcel_id or parcel_context.parcel_id,
            requested_tax_year=request.tax_year,
            served_tax_year=quote_context.served_tax_year,
            tax_year_fallback_applied=tax_year_fallback_applied,
            tax_year_fallback_reason=(
                "requested_year_unavailable" if tax_year_fallback_applied else None
            ),
            property_type_code=parcel_context.property_type_code,
            protest_recommendation=quote_context.protest_recommendation,
            expected_tax_savings_point=quote_context.expected_tax_savings_point,
            defensible_value_point=quote_context.defensible_value_point,
        )

    def _fetch_parcel_context(self, request: LeadCreateRequest) -> LeadParcelContext:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT parcel_id, tax_year, property_type_code
                    FROM parcel_summary_view
                    WHERE county_id = %s
                      AND account_number = %s
                      AND tax_year <= %s
                    ORDER BY tax_year DESC
                    LIMIT 1
                    """,
                    (request.county_id, request.account_number, request.tax_year),
                )
                row = cursor.fetchone()

        if row is None:
            return LeadParcelContext(
                parcel_id=None,
                requested_tax_year=request.tax_year,
                served_tax_year=None,
                property_type_code=None,
            )

        served_tax_year = int(row["tax_year"]) if row.get("tax_year") is not None else None
        return LeadParcelContext(
            parcel_id=row.get("parcel_id"),
            requested_tax_year=request.tax_year,
            served_tax_year=served_tax_year,
            property_type_code=row.get("property_type_code"),
        )

    def _fetch_quote_context(self, request: LeadCreateRequest) -> LeadQuoteContext | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      parcel_id,
                      tax_year,
                      protest_recommendation,
                      expected_tax_savings_point,
                      defensible_value_point
                    FROM v_quote_read_model
                    WHERE county_id = %s
                      AND account_number = %s
                      AND tax_year <= %s
                    ORDER BY tax_year DESC, valuation_created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (request.county_id, request.account_number, request.tax_year),
                )
                row = cursor.fetchone()

        if row is None:
            return None

        return LeadQuoteContext(
            parcel_id=row.get("parcel_id"),
            served_tax_year=int(row["tax_year"]),
            protest_recommendation=row.get("protest_recommendation"),
            expected_tax_savings_point=_as_float(row.get("expected_tax_savings_point")),
            defensible_value_point=_as_float(row.get("defensible_value_point")),
        )

    def _insert_lead(
        self,
        cursor: Any,
        *,
        request: LeadCreateRequest,
        context: LeadContractContext,
    ) -> UUID:
        cursor.execute(
            """
            INSERT INTO leads (
              parcel_id,
              county_id,
              tax_year,
              account_number,
              owner_name,
              email,
              phone,
              source_channel,
              consent_to_contact
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING lead_id
            """,
            (
                context.parcel_id,
                request.county_id,
                request.tax_year,
                request.account_number,
                request.owner_name,
                request.email,
                request.phone,
                request.source_channel,
                request.consent_to_contact,
            ),
        )
        row = cursor.fetchone()
        if row is None or row.get("lead_id") is None:
            raise RuntimeError("Lead persistence did not return a lead_id.")
        return row["lead_id"]

    def _insert_lead_event(
        self,
        cursor: Any,
        *,
        lead_id: UUID,
        request: LeadCreateRequest,
        context: LeadContractContext,
    ) -> None:
        payload = {
            "anonymous_session_id": request.anonymous_session_id,
            "funnel_stage": request.funnel_stage,
            "utm": {
                "source": request.utm_source,
                "medium": request.utm_medium,
                "campaign": request.utm_campaign,
                "term": request.utm_term,
                "content": request.utm_content,
            },
            "quote_context": {
                "status": context.status,
                "county_supported": context.county_supported,
                "property_supported": context.property_supported,
                "quote_ready": context.quote_ready,
                "county_id": request.county_id,
                "account_number": request.account_number,
                "requested_tax_year": request.tax_year,
                "served_tax_year": context.served_tax_year,
                "tax_year_fallback_applied": context.tax_year_fallback_applied,
                "tax_year_fallback_reason": context.tax_year_fallback_reason,
                "parcel_id": str(context.parcel_id) if context.parcel_id is not None else None,
                "property_type_code": context.property_type_code,
                "protest_recommendation": context.protest_recommendation,
                "expected_tax_savings_point": context.expected_tax_savings_point,
                "defensible_value_point": context.defensible_value_point,
            },
            "contact": {
                "owner_name": request.owner_name,
                "email_present": request.email is not None,
                "phone_present": request.phone is not None,
                "consent_to_contact": request.consent_to_contact,
            },
        }

        cursor.execute(
            """
            INSERT INTO lead_events (
              lead_id,
              event_code,
              event_payload
            )
            VALUES (%s, %s, %s::jsonb)
            """,
            (lead_id, "lead_submitted", json.dumps(payload)),
        )


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)

