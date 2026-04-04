from __future__ import annotations

from dataclasses import dataclass

from app.db.connection import get_connection
from app.services.instant_quote_tax_rate_basis import (
    TAX_RATE_ADOPTION_STATUS_FINAL_EVIDENCE_SOURCES,
    TAX_RATE_ADOPTION_STATUS_SOURCE_OPERATOR_ASSERTED,
    TAX_RATE_ADOPTION_STATUS_SOURCES,
    TAX_RATE_BASIS_STATUSES,
    TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES,
)


@dataclass(frozen=True)
class TaxRateAdoptionStatusRecord:
    county_id: str
    tax_year: int
    adoption_status: str
    adoption_status_reason: str | None = None
    status_source: str | None = None
    source_note: str | None = None


class InstantQuoteTaxRateAdoptionStatusService:
    def upsert_status(
        self,
        *,
        county_id: str,
        tax_year: int,
        adoption_status: str,
        adoption_status_reason: str | None = None,
        status_source: str | None = None,
        source_note: str | None = None,
    ) -> TaxRateAdoptionStatusRecord:
        if adoption_status not in TAX_RATE_BASIS_STATUSES:
            raise ValueError(f"Unsupported tax-rate adoption status: {adoption_status}")

        normalized_reason = _normalize_text(adoption_status_reason)
        explicit_source = _normalize_text(status_source)
        normalized_source = explicit_source or TAX_RATE_ADOPTION_STATUS_SOURCE_OPERATOR_ASSERTED
        normalized_note = _normalize_text(source_note)
        self._validate_status_inputs(
            adoption_status=adoption_status,
            adoption_status_reason=normalized_reason,
            status_source=normalized_source,
            source_note=normalized_note,
            explicit_source=explicit_source,
        )

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO instant_quote_tax_rate_adoption_statuses (
                      county_id,
                      tax_year,
                      adoption_status,
                      adoption_status_reason,
                      status_source,
                      source_note
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (county_id, tax_year) DO UPDATE
                    SET adoption_status = EXCLUDED.adoption_status,
                        adoption_status_reason = EXCLUDED.adoption_status_reason,
                        status_source = EXCLUDED.status_source,
                        source_note = EXCLUDED.source_note,
                        updated_at = now()
                    RETURNING
                      county_id,
                      tax_year,
                      adoption_status,
                      adoption_status_reason,
                      status_source,
                      source_note
                    """,
                    (
                        county_id,
                        tax_year,
                        adoption_status,
                        normalized_reason,
                        normalized_source,
                        normalized_note,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()

        if row is None:
            raise RuntimeError("Tax-rate adoption status upsert returned no row.")
        return TaxRateAdoptionStatusRecord(
            county_id=str(row["county_id"]),
            tax_year=int(row["tax_year"]),
            adoption_status=str(row["adoption_status"]),
            adoption_status_reason=(
                None
                if row.get("adoption_status_reason") is None
                else str(row["adoption_status_reason"])
            ),
            status_source=None if row.get("status_source") is None else str(row["status_source"]),
            source_note=None if row.get("source_note") is None else str(row["source_note"]),
        )

    def _validate_status_inputs(
        self,
        *,
        adoption_status: str,
        adoption_status_reason: str | None,
        status_source: str,
        source_note: str | None,
        explicit_source: str | None,
    ) -> None:
        if status_source not in TAX_RATE_ADOPTION_STATUS_SOURCES:
            allowed_sources = ", ".join(sorted(TAX_RATE_ADOPTION_STATUS_SOURCES))
            raise ValueError(
                "Unsupported tax-rate adoption status source: "
                f"{status_source}. Allowed values: {allowed_sources}."
            )

        if adoption_status != TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES:
            return

        missing_args: list[str] = []
        if adoption_status_reason is None:
            missing_args.append("--tax-rate-basis-status-reason")
        if explicit_source is None:
            missing_args.append("--tax-rate-basis-status-source")
        if source_note is None:
            missing_args.append("--tax-rate-basis-status-note")
        if missing_args:
            raise ValueError(
                "current_year_final_adopted_rates requires "
                + ", ".join(missing_args)
                + "."
            )

        if status_source not in TAX_RATE_ADOPTION_STATUS_FINAL_EVIDENCE_SOURCES:
            allowed_sources = ", ".join(sorted(TAX_RATE_ADOPTION_STATUS_FINAL_EVIDENCE_SOURCES))
            raise ValueError(
                "current_year_final_adopted_rates requires an evidence-backed "
                f"--tax-rate-basis-status-source. Allowed values: {allowed_sources}."
            )


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None
