from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

from app.county_adapters.common.config_loader import CountyAdapterConfig, load_county_adapter_config
from app.db.connection import get_connection

CapabilityStatus = Literal["supported", "limited", "unsupported", "unknown"]


@dataclass(frozen=True)
class InstantQuoteCountyTaxCapability:
    county_id: str
    tax_year: int
    exemption_normalization_confidence: str
    over65_reliability: str
    disabled_reliability: str
    disabled_veteran_reliability: str
    freeze_reliability: str
    tax_unit_assignment_reliability: str
    tax_rate_reliability: str
    school_ceiling_amount_available: bool
    unit_exemption_policy_available: bool
    local_option_policy_available: bool
    profile_support_level: str
    notes: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class InstantQuoteCountyTaxCapabilityService:
    def build_capability(
        self,
        *,
        county_id: str,
        tax_year: int,
    ) -> InstantQuoteCountyTaxCapability:
        config = load_county_adapter_config(county_id)
        with get_connection() as connection:
            with connection.cursor() as cursor:
                observed_signals = self._observed_signal_metrics(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                latest_refresh_run = self._latest_refresh_run(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )

        exemption_normalization_confidence = self._exemption_normalization_confidence(
            config=config,
            observed_signals=observed_signals,
        )
        over65_reliability = self._signal_reliability(
            config=config,
            capability_code="instant_quote_over65_reliability",
            observed_rows=int(observed_signals["over65_rows"]),
        )
        disabled_reliability = self._signal_reliability(
            config=config,
            capability_code="instant_quote_disabled_reliability",
            observed_rows=int(observed_signals["disabled_rows"]),
        )
        disabled_veteran_reliability = self._signal_reliability(
            config=config,
            capability_code="instant_quote_disabled_veteran_reliability",
            observed_rows=int(observed_signals["disabled_veteran_rows"]),
        )
        freeze_reliability = self._signal_reliability(
            config=config,
            capability_code="instant_quote_freeze_reliability",
            observed_rows=int(observed_signals["freeze_rows"]),
        )
        tax_unit_assignment_reliability = self._tax_unit_assignment_reliability(
            latest_refresh_run=latest_refresh_run
        )
        tax_rate_reliability = self._tax_rate_reliability(latest_refresh_run=latest_refresh_run)
        school_ceiling_amount_available = self._availability_flag(
            config=config,
            capability_code="instant_quote_school_ceiling_amount_available",
        )
        local_option_policy_available = self._availability_flag(
            config=config,
            capability_code="instant_quote_local_option_policy_available",
        )
        profile_support_level = self._profile_support_level(config=config)

        note_lines = [
            self._capability_notes(config, "instant_quote_exemption_normalization_confidence"),
            self._capability_notes(config, "instant_quote_over65_reliability"),
            self._capability_notes(config, "instant_quote_disabled_reliability"),
            self._capability_notes(config, "instant_quote_disabled_veteran_reliability"),
            self._capability_notes(config, "instant_quote_freeze_reliability"),
            self._dynamic_signal_note(
                label="over65_rows",
                observed_rows=int(observed_signals["over65_rows"]),
                downgraded=(over65_reliability == "limited"),
            ),
            self._dynamic_signal_note(
                label="missing_exemption_amount_rows",
                observed_rows=int(observed_signals["missing_exemption_amount_rows"]),
                downgraded=(exemption_normalization_confidence == "limited"),
            ),
            self._dynamic_refresh_note(latest_refresh_run=latest_refresh_run),
        ]
        notes = " | ".join(note for note in note_lines if note)

        return InstantQuoteCountyTaxCapability(
            county_id=county_id,
            tax_year=tax_year,
            exemption_normalization_confidence=exemption_normalization_confidence,
            over65_reliability=over65_reliability,
            disabled_reliability=disabled_reliability,
            disabled_veteran_reliability=disabled_veteran_reliability,
            freeze_reliability=freeze_reliability,
            tax_unit_assignment_reliability=tax_unit_assignment_reliability,
            tax_rate_reliability=tax_rate_reliability,
            school_ceiling_amount_available=school_ceiling_amount_available,
            unit_exemption_policy_available=False,
            local_option_policy_available=local_option_policy_available,
            profile_support_level=profile_support_level,
            notes=notes or None,
        )

    def materialize_capability(
        self,
        *,
        county_id: str,
        tax_year: int,
    ) -> InstantQuoteCountyTaxCapability:
        capability = self.build_capability(county_id=county_id, tax_year=tax_year)
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO instant_quote_county_tax_capability (
                      county_id,
                      tax_year,
                      exemption_normalization_confidence,
                      over65_reliability,
                      disabled_reliability,
                      disabled_veteran_reliability,
                      freeze_reliability,
                      tax_unit_assignment_reliability,
                      tax_rate_reliability,
                      school_ceiling_amount_available,
                      unit_exemption_policy_available,
                      local_option_policy_available,
                      profile_support_level,
                      notes
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (county_id, tax_year) DO UPDATE
                    SET exemption_normalization_confidence = EXCLUDED.exemption_normalization_confidence,
                        over65_reliability = EXCLUDED.over65_reliability,
                        disabled_reliability = EXCLUDED.disabled_reliability,
                        disabled_veteran_reliability = EXCLUDED.disabled_veteran_reliability,
                        freeze_reliability = EXCLUDED.freeze_reliability,
                        tax_unit_assignment_reliability = EXCLUDED.tax_unit_assignment_reliability,
                        tax_rate_reliability = EXCLUDED.tax_rate_reliability,
                        school_ceiling_amount_available = EXCLUDED.school_ceiling_amount_available,
                        unit_exemption_policy_available = EXCLUDED.unit_exemption_policy_available,
                        local_option_policy_available = EXCLUDED.local_option_policy_available,
                        profile_support_level = EXCLUDED.profile_support_level,
                        notes = EXCLUDED.notes,
                        updated_at = now()
                    """,
                    (
                        capability.county_id,
                        capability.tax_year,
                        capability.exemption_normalization_confidence,
                        capability.over65_reliability,
                        capability.disabled_reliability,
                        capability.disabled_veteran_reliability,
                        capability.freeze_reliability,
                        capability.tax_unit_assignment_reliability,
                        capability.tax_rate_reliability,
                        capability.school_ceiling_amount_available,
                        capability.unit_exemption_policy_available,
                        capability.local_option_policy_available,
                        capability.profile_support_level,
                        capability.notes,
                    ),
                )
            connection.commit()
        return capability

    def fetch_materialized_capability(
        self,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT county_id,
                           tax_year,
                           exemption_normalization_confidence,
                           over65_reliability,
                           disabled_reliability,
                           disabled_veteran_reliability,
                           freeze_reliability,
                           tax_unit_assignment_reliability,
                           tax_rate_reliability,
                           school_ceiling_amount_available,
                           unit_exemption_policy_available,
                           local_option_policy_available,
                           profile_support_level,
                           notes
                    FROM instant_quote_county_tax_capability
                    WHERE county_id = %s
                      AND tax_year = %s
                    """,
                    (county_id, tax_year),
                )
                row = cursor.fetchone()
        return None if row is None else dict(row)

    def _observed_signal_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT COUNT(*)::integer AS subject_cache_row_count,
                   COUNT(*) FILTER (
                     WHERE COALESCE(over65_flag, false)
                        OR 'over65' = ANY(exemption_type_codes)
                   )::integer AS over65_rows,
                   COUNT(*) FILTER (
                     WHERE COALESCE(disabled_flag, false)
                        OR 'disabled_person' = ANY(exemption_type_codes)
                   )::integer AS disabled_rows,
                   COUNT(*) FILTER (
                     WHERE COALESCE(disabled_veteran_flag, false)
                        OR 'disabled_veteran' = ANY(exemption_type_codes)
                   )::integer AS disabled_veteran_rows,
                   COUNT(*) FILTER (
                     WHERE COALESCE(freeze_flag, false)
                        OR 'freeze_ceiling' = ANY(exemption_type_codes)
                   )::integer AS freeze_rows,
                   COUNT(*) FILTER (
                     WHERE 'missing_exemption_amount' = ANY(warning_codes)
                   )::integer AS missing_exemption_amount_rows,
                   COUNT(*) FILTER (
                     WHERE 'assessment_exemption_total_mismatch' = ANY(warning_codes)
                   )::integer AS assessment_exemption_total_mismatch_rows,
                   COUNT(*) FILTER (
                     WHERE 'homestead_flag_mismatch' = ANY(warning_codes)
                   )::integer AS homestead_flag_mismatch_rows
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            """,
            (county_id, tax_year),
        )
        return dict(cursor.fetchone() or {})

    def _latest_refresh_run(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT tax_rate_basis_status,
                   tax_rate_basis_reason,
                   tax_rate_basis_fallback_applied,
                   tax_rate_basis_effective_tax_rate_coverage_ratio,
                   tax_rate_basis_assignment_coverage_ratio,
                   tax_rate_basis_warning_codes
            FROM instant_quote_refresh_runs
            WHERE county_id = %s
              AND tax_year = %s
              AND refresh_status = 'completed'
            ORDER BY refresh_started_at DESC
            LIMIT 1
            """,
            (county_id, tax_year),
        )
        row = cursor.fetchone()
        return None if row is None else dict(row)

    def _exemption_normalization_confidence(
        self,
        *,
        config: CountyAdapterConfig,
        observed_signals: dict[str, Any],
    ) -> str:
        base_status = self._capability_status(
            config,
            "instant_quote_exemption_normalization_confidence",
        )
        if base_status == "supported" and (
            int(observed_signals["missing_exemption_amount_rows"]) > 0
            or int(observed_signals["assessment_exemption_total_mismatch_rows"]) > 0
            or int(observed_signals["homestead_flag_mismatch_rows"]) > 0
        ):
            return "limited"
        return base_status

    def _signal_reliability(
        self,
        *,
        config: CountyAdapterConfig,
        capability_code: str,
        observed_rows: int,
    ) -> str:
        base_status = self._capability_status(config, capability_code)
        if base_status == "supported" and observed_rows <= 0:
            return "limited"
        return base_status

    def _tax_unit_assignment_reliability(
        self,
        *,
        latest_refresh_run: dict[str, Any] | None,
    ) -> str:
        if latest_refresh_run is None:
            return "unknown"
        coverage_ratio = float(
            latest_refresh_run.get("tax_rate_basis_assignment_coverage_ratio") or 0.0
        )
        if coverage_ratio >= 0.99:
            return "supported"
        if coverage_ratio >= 0.95:
            return "limited"
        return "unsupported"

    def _tax_rate_reliability(
        self,
        *,
        latest_refresh_run: dict[str, Any] | None,
    ) -> str:
        if latest_refresh_run is None:
            return "unknown"
        basis_status = str(latest_refresh_run.get("tax_rate_basis_status") or "")
        coverage_ratio = float(
            latest_refresh_run.get("tax_rate_basis_effective_tax_rate_coverage_ratio") or 0.0
        )
        fallback_applied = bool(latest_refresh_run.get("tax_rate_basis_fallback_applied"))
        if (
            basis_status == "current_year_final_adopted_rates"
            and coverage_ratio >= 0.99
            and not fallback_applied
        ):
            return "supported"
        if coverage_ratio >= 0.95:
            return "limited"
        return "unsupported"

    def _availability_flag(
        self,
        *,
        config: CountyAdapterConfig,
        capability_code: str,
    ) -> bool:
        return self._capability_status(config, capability_code) == "supported"

    def _profile_support_level(self, *, config: CountyAdapterConfig) -> str:
        capability = config.capability_matrix.get("instant_quote_profile_support_level")
        if capability is None:
            return "summary_only"
        return capability.status or "summary_only"

    def _capability_status(
        self,
        config: CountyAdapterConfig,
        capability_code: str,
    ) -> str:
        capability = config.capability_matrix.get(capability_code)
        if capability is None:
            return "unknown"
        return capability.status or "unknown"

    def _capability_notes(
        self,
        config: CountyAdapterConfig,
        capability_code: str,
    ) -> str | None:
        capability = config.capability_matrix.get(capability_code)
        if capability is None:
            return None
        notes = str(capability.notes or "").strip()
        return notes or None

    def _dynamic_signal_note(
        self,
        *,
        label: str,
        observed_rows: int,
        downgraded: bool,
    ) -> str | None:
        if not downgraded:
            return None
        return f"{label}={observed_rows}"

    def _dynamic_refresh_note(
        self,
        *,
        latest_refresh_run: dict[str, Any] | None,
    ) -> str | None:
        if latest_refresh_run is None:
            return "No completed instant-quote refresh run is available yet."
        basis_status = str(latest_refresh_run.get("tax_rate_basis_status") or "unknown")
        basis_reason = str(latest_refresh_run.get("tax_rate_basis_reason") or "unknown")
        effective_rate_coverage_ratio = float(
            latest_refresh_run.get("tax_rate_basis_effective_tax_rate_coverage_ratio") or 0.0
        )
        assignment_coverage_ratio = float(
            latest_refresh_run.get("tax_rate_basis_assignment_coverage_ratio") or 0.0
        )
        return (
            "latest_refresh="
            f"{basis_status}/{basis_reason}"
            f"/rate_cov={effective_rate_coverage_ratio:.4f}"
            f"/assign_cov={assignment_coverage_ratio:.4f}"
        )
