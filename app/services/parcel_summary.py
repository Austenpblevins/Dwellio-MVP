from __future__ import annotations

from typing import Any

from app.db.connection import get_connection
from app.models.parcel import (
    ParcelDataCaveat,
    ParcelExemptionSummary,
    ParcelOwnerSummary,
    ParcelSummaryResponse,
    ParcelTaxRateComponent,
    ParcelTaxSummary,
    ParcelValueSummary,
)
from app.services.public_owner import build_public_owner_summary

WARNING_COPY: dict[str, tuple[str, str, str]] = {
    "missing_address": (
        "critical",
        "Address details limited",
        "This parcel is missing full situs address details in the current public summary view.",
    ),
    "missing_characteristics": (
        "warning",
        "Property characteristics limited",
        "Some improvement or home-characteristic details are not yet available.",
    ),
    "missing_improvement": (
        "warning",
        "Improvement data limited",
        "Building detail is incomplete, so square-footage or room data may be partial.",
    ),
    "missing_land": (
        "warning",
        "Land data limited",
        "Lot-size details are incomplete for this parcel-year summary.",
    ),
    "missing_assessment": (
        "critical",
        "Assessment data missing",
        "Current assessed or notice values are incomplete for this parcel-year.",
    ),
    "missing_exemption_data": (
        "warning",
        "Exemption data limited",
        "The exemption rollup is incomplete, so taxability may shift after refresh.",
    ),
    "missing_county_assignment": (
        "warning",
        "County tax assignment limited",
        "County taxing-unit assignments are incomplete, so rate breakdowns may be partial.",
    ),
    "missing_school_assignment": (
        "warning",
        "School tax assignment limited",
        "School taxing-unit assignments are incomplete, so school taxes may be understated.",
    ),
    "missing_effective_tax_rate": (
        "critical",
        "Tax estimate unavailable",
        "An effective tax rate is not available yet, so annual tax estimates cannot be trusted.",
    ),
    "missing_owner_rollup": (
        "warning",
        "Owner summary limited",
        "Owner information is based on limited public records for this parcel-year.",
    ),
    "cad_owner_mismatch": (
        "warning",
        "Ownership sources differ",
        "The selected owner rollup and current CAD snapshot do not fully reconcile yet.",
    ),
    "missing_exemption_amount": (
        "warning",
        "Exemption amount incomplete",
        "At least one exemption is present without a complete amount.",
    ),
    "assessment_exemption_total_mismatch": (
        "warning",
        "Exemption totals differ",
        "Assessment totals and exemption detail do not fully match in the current read model.",
    ),
    "homestead_flag_mismatch": (
        "warning",
        "Homestead flags differ",
        "Homestead status differs between normalized exemption detail and assessment data.",
    ),
    "freeze_without_qualifying_exemption": (
        "warning",
        "Freeze indicator needs review",
        "A tax freeze indicator appears without a matching qualifying exemption flag.",
    ),
    "missing_geometry": (
        "info",
        "Map geometry pending",
        "Map geometry is not linked yet, but the parcel summary can still be reviewed.",
    ),
}


class ParcelSummaryService:
    def get_summary(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> ParcelSummaryResponse:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM parcel_summary_view
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND account_number = %s
                    LIMIT 1
                    """,
                    (county_id, tax_year, account_number),
                )
                row = cursor.fetchone()

        if row is None:
            raise LookupError(
                f"Parcel summary not found for {county_id}/{tax_year}/{account_number}."
            )

        return self._build_summary(row)

    def _build_summary(self, row: dict[str, Any]) -> ParcelSummaryResponse:
        warning_codes = [str(code) for code in row.get("warning_codes") or []]
        owner_summary = build_public_owner_summary(
            row.get("owner_name"),
            confidence_score=_as_float(row.get("owner_confidence_score")),
        )
        component_breakdown = [
            ParcelTaxRateComponent.model_validate(component)
            for component in row.get("component_breakdown_json") or []
            if isinstance(component, dict)
        ]

        return ParcelSummaryResponse(
            county_id=row["county_id"],
            tax_year=int(row["tax_year"]),
            account_number=row["account_number"],
            parcel_id=row["parcel_id"],
            address=row["address"],
            owner_name=owner_summary.display_name,
            property_type_code=row.get("property_type_code"),
            property_class_code=row.get("property_class_code"),
            neighborhood_code=row.get("neighborhood_code"),
            subdivision_name=row.get("subdivision_name"),
            school_district_name=row.get("school_district_name"),
            living_area_sf=_as_float(row.get("living_area_sf")),
            year_built=_as_int(row.get("year_built")),
            effective_age=_as_float(row.get("effective_age")),
            bedrooms=_as_int(row.get("bedrooms")),
            full_baths=_as_float(row.get("full_baths")),
            half_baths=_as_float(row.get("half_baths")),
            land_sf=_as_float(row.get("land_sf")),
            land_acres=_as_float(row.get("land_acres")),
            market_value=_as_float(row.get("market_value")),
            assessed_value=_as_float(row.get("assessed_value")),
            appraised_value=_as_float(row.get("appraised_value")),
            certified_value=_as_float(row.get("certified_value")),
            notice_value=_as_float(row.get("notice_value")),
            exemption_value_total=_as_float(row.get("exemption_value_total")),
            homestead_flag=_as_bool(row.get("homestead_flag")),
            over65_flag=_as_bool(row.get("over65_flag")),
            disabled_flag=_as_bool(row.get("disabled_flag")),
            disabled_veteran_flag=_as_bool(row.get("disabled_veteran_flag")),
            freeze_flag=_as_bool(row.get("freeze_flag")),
            effective_tax_rate=_as_float(row.get("effective_tax_rate")),
            estimated_taxable_value=_as_float(row.get("estimated_taxable_value")),
            estimated_annual_tax=_as_float(row.get("estimated_annual_tax")),
            exemption_type_codes=[str(code) for code in row.get("exemption_type_codes") or []],
            raw_exemption_codes=[str(code) for code in row.get("raw_exemption_codes") or []],
            completeness_score=float(row.get("completeness_score") or 0.0),
            warning_codes=warning_codes,
            public_summary_ready_flag=bool(row.get("public_summary_ready_flag")),
            owner_summary=owner_summary,
            value_summary=ParcelValueSummary(
                market_value=_as_float(row.get("market_value")),
                assessed_value=_as_float(row.get("assessed_value")),
                appraised_value=_as_float(row.get("appraised_value")),
                certified_value=_as_float(row.get("certified_value")),
                notice_value=_as_float(row.get("notice_value")),
            ),
            exemption_summary=ParcelExemptionSummary(
                exemption_value_total=_as_float(row.get("exemption_value_total")),
                homestead_flag=_as_bool(row.get("homestead_flag")),
                over65_flag=_as_bool(row.get("over65_flag")),
                disabled_flag=_as_bool(row.get("disabled_flag")),
                disabled_veteran_flag=_as_bool(row.get("disabled_veteran_flag")),
                freeze_flag=_as_bool(row.get("freeze_flag")),
                exemption_type_codes=[str(code) for code in row.get("exemption_type_codes") or []],
                raw_exemption_codes=[str(code) for code in row.get("raw_exemption_codes") or []],
            ),
            tax_summary=ParcelTaxSummary(
                effective_tax_rate=_as_float(row.get("effective_tax_rate")),
                estimated_taxable_value=_as_float(row.get("estimated_taxable_value")),
                estimated_annual_tax=_as_float(row.get("estimated_annual_tax")),
                component_breakdown=component_breakdown,
            ),
            caveats=self._build_caveats(
                warning_codes=warning_codes,
                completeness_score=float(row.get("completeness_score") or 0.0),
                owner_summary=owner_summary,
                public_summary_ready_flag=bool(row.get("public_summary_ready_flag")),
            ),
        )

    def _build_caveats(
        self,
        *,
        warning_codes: list[str],
        completeness_score: float,
        owner_summary: ParcelOwnerSummary,
        public_summary_ready_flag: bool,
    ) -> list[ParcelDataCaveat]:
        caveats: list[ParcelDataCaveat] = []
        for warning_code in warning_codes:
            copy = WARNING_COPY.get(warning_code)
            if copy is None:
                caveats.append(
                    ParcelDataCaveat(
                        code=warning_code,
                        severity="warning",
                        title="Data caveat",
                        message="Some parcel summary fields need additional validation.",
                    )
                )
                continue
            severity, title, message = copy
            caveats.append(
                ParcelDataCaveat(
                    code=warning_code,
                    severity=severity,
                    title=title,
                    message=message,
                )
            )

        if completeness_score < 85:
            caveats.append(
                ParcelDataCaveat(
                    code="limited_data_completeness",
                    severity="warning" if public_summary_ready_flag else "critical",
                    title="Summary completeness is limited",
                    message=(
                        "This parcel summary is still usable, but some public fields are incomplete "
                        "for the current parcel-year."
                    ),
                )
            )

        if owner_summary.confidence_label == "limited":
            caveats.append(
                ParcelDataCaveat(
                    code="owner_confidence_limited",
                    severity="info",
                    title="Owner name is conservatively displayed",
                    message=(
                        "Owner details are shown using a privacy-safe display because ownership "
                        "confidence is limited or the owner record may change."
                    ),
                )
            )

        return list({caveat.code: caveat for caveat in caveats}.values())


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)
