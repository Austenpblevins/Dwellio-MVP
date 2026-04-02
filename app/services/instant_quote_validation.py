from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.instant_quote import InstantQuoteService


@dataclass(frozen=True)
class InstantQuoteExampleResult:
    account_number: str
    served_tax_year: int
    supported: bool
    unsupported_reason: str | None = None
    estimate_bucket: str | None = None
    estimate_strength_label: str | None = None


@dataclass(frozen=True)
class InstantQuoteValidationReport:
    county_id: str
    tax_year: int
    parcel_rows_with_living_area: int
    parcel_rows_with_effective_tax_rate: int
    subject_cache_row_count: int
    instant_quote_supportable_rows: int
    supported_neighborhood_stats_rows: int
    supported_segment_stats_rows: int
    latest_refresh_status: str | None = None
    latest_refresh_finished_at: str | None = None
    latest_validation_at: str | None = None
    cache_view_row_delta: int | None = None
    blocker_distribution: dict[str, int] = field(default_factory=dict)
    supported_public_quote_exists: bool = False
    examples: list[InstantQuoteExampleResult] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["examples"] = [asdict(example) for example in self.examples]
        return payload


class InstantQuoteValidationService:
    def __init__(self, *, quote_service: InstantQuoteService | None = None) -> None:
        self.quote_service = quote_service or InstantQuoteService()

    def build_report(self, *, county_id: str, tax_year: int) -> InstantQuoteValidationReport:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                parcel_rows_with_living_area = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND COALESCE(living_area_sf, 0) > 0
                    """,
                    (county_id, tax_year),
                )
                parcel_rows_with_effective_tax_rate = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND COALESCE(effective_tax_rate, 0) > 0
                    """,
                    (county_id, tax_year),
                )
                instant_quote_supportable_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_blocker_code IS NULL
                    """,
                    (county_id, tax_year),
                )
                subject_cache_row_count = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                    """,
                    (county_id, tax_year),
                )
                supported_neighborhood_stats_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_neighborhood_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, tax_year),
                )
                supported_segment_stats_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_segment_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, tax_year),
                )
                blocker_distribution = self._blocker_distribution(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                candidate_accounts = self._candidate_accounts(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                latest_refresh_run = self._latest_refresh_run(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )

        examples: list[InstantQuoteExampleResult] = []
        supported_public_quote_exists = False
        for account_number in candidate_accounts:
            try:
                response = self.quote_service.get_quote(
                    county_id=county_id,
                    tax_year=tax_year,
                    account_number=account_number,
                )
            except LookupError:
                continue

            if response.supported:
                supported_public_quote_exists = True
            examples.append(
                InstantQuoteExampleResult(
                    account_number=account_number,
                    served_tax_year=response.served_tax_year,
                    supported=response.supported,
                    unsupported_reason=response.unsupported_reason,
                    estimate_bucket=(
                        None
                        if response.estimate is None
                        else response.estimate.estimate_bucket
                    ),
                    estimate_strength_label=(
                        None
                        if response.estimate is None
                        else response.estimate.estimate_strength_label
                    ),
                )
            )
            if len(examples) >= 6 and supported_public_quote_exists:
                break

        self._persist_validation_report(
            county_id=county_id,
            tax_year=tax_year,
            report_payload={
                "county_id": county_id,
                "tax_year": tax_year,
                "parcel_rows_with_living_area": parcel_rows_with_living_area,
                "parcel_rows_with_effective_tax_rate": parcel_rows_with_effective_tax_rate,
                "subject_cache_row_count": subject_cache_row_count,
                "instant_quote_supportable_rows": instant_quote_supportable_rows,
                "supported_neighborhood_stats_rows": supported_neighborhood_stats_rows,
                "supported_segment_stats_rows": supported_segment_stats_rows,
                "blocker_distribution": blocker_distribution,
                "supported_public_quote_exists": supported_public_quote_exists,
                "examples": [asdict(example) for example in examples],
            },
        )

        return InstantQuoteValidationReport(
            county_id=county_id,
            tax_year=tax_year,
            parcel_rows_with_living_area=parcel_rows_with_living_area,
            parcel_rows_with_effective_tax_rate=parcel_rows_with_effective_tax_rate,
            subject_cache_row_count=subject_cache_row_count,
            instant_quote_supportable_rows=instant_quote_supportable_rows,
            supported_neighborhood_stats_rows=supported_neighborhood_stats_rows,
            supported_segment_stats_rows=supported_segment_stats_rows,
            latest_refresh_status=(
                None if latest_refresh_run is None else str(latest_refresh_run["refresh_status"])
            ),
            latest_refresh_finished_at=(
                None
                if latest_refresh_run is None or latest_refresh_run.get("refresh_finished_at") is None
                else latest_refresh_run["refresh_finished_at"].isoformat()
            ),
            latest_validation_at=(
                None
                if latest_refresh_run is None or latest_refresh_run.get("validated_at") is None
                else latest_refresh_run["validated_at"].isoformat()
            ),
            cache_view_row_delta=(
                None if latest_refresh_run is None else int(latest_refresh_run["cache_view_row_delta"] or 0)
            ),
            blocker_distribution=blocker_distribution,
            supported_public_quote_exists=supported_public_quote_exists,
            examples=examples,
        )

    def _count(self, cursor: Any, sql: str, params: tuple[object, ...]) -> int:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)

    def _blocker_distribution(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, int]:
        cursor.execute(
            """
            SELECT
              COALESCE(support_blocker_code, 'supportable') AS blocker_code,
              COUNT(*) AS count
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            GROUP BY COALESCE(support_blocker_code, 'supportable')
            ORDER BY count DESC, blocker_code ASC
            """,
            (county_id, tax_year),
        )
        return {str(row["blocker_code"]): int(row["count"]) for row in cursor.fetchall()}

    def _latest_refresh_run(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT
              refresh_status,
              refresh_finished_at,
              validated_at,
              cache_view_row_delta
            FROM instant_quote_refresh_runs
            WHERE county_id = %s
              AND tax_year = %s
            ORDER BY refresh_started_at DESC
            LIMIT 1
            """,
            (county_id, tax_year),
        )
        return cursor.fetchone()

    def _candidate_accounts(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> list[str]:
        account_numbers: list[str] = []
        seen: set[str] = set()
        preferred_blockers: tuple[str | None, ...] = (
            None,
            "missing_effective_tax_rate",
            "missing_living_area",
            "missing_assessment_basis",
        )
        for blocker_code in preferred_blockers:
            cursor.execute(
                """
                SELECT account_number
                FROM instant_quote_subject_cache
                WHERE county_id = %s
                  AND tax_year = %s
                  AND (
                    (%s::text IS NULL AND support_blocker_code IS NULL)
                    OR support_blocker_code = %s
                  )
                ORDER BY account_number ASC
                LIMIT 2
                """,
                (county_id, tax_year, blocker_code, blocker_code),
            )
            for row in cursor.fetchall():
                account_number = str(row["account_number"])
                if account_number in seen:
                    continue
                seen.add(account_number)
                account_numbers.append(account_number)

        if len(account_numbers) >= 12:
            return account_numbers[:12]

        cursor.execute(
            """
            SELECT account_number
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            ORDER BY account_number ASC
            LIMIT 12
            """,
            (county_id, tax_year),
        )
        for row in cursor.fetchall():
            account_number = str(row["account_number"])
            if account_number in seen:
                continue
            seen.add(account_number)
            account_numbers.append(account_number)
            if len(account_numbers) >= 12:
                break
        return account_numbers

    def _persist_validation_report(
        self,
        *,
        county_id: str,
        tax_year: int,
        report_payload: dict[str, Any],
    ) -> None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE instant_quote_refresh_runs
                    SET validated_at = now(),
                        validation_report = %s::jsonb
                    WHERE instant_quote_refresh_run_id = (
                      SELECT instant_quote_refresh_run_id
                      FROM instant_quote_refresh_runs
                      WHERE county_id = %s
                        AND tax_year = %s
                      ORDER BY refresh_started_at DESC
                      LIMIT 1
                    )
                    """,
                    (Jsonb(report_payload), county_id, tax_year),
                )
            connection.commit()
