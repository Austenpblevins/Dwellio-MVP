from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.decision_tree import ProtestDecisionTreeService
from app.services.equity_model import EquityModelService
from app.services.explanation_builder import ExplanationBuilderService
from app.services.market_model import MarketModelInputs, MarketModelService
from app.services.savings_engine import SavingsEngineService

GENERATION_MODE = "stage17_reasonableness_benchmark"
MODEL_VERSION = "stage17_reasonableness_benchmark_v1"
CONTINGENCY_RATE = 0.35
SEGMENT_MIN_COUNT = 6
NEIGHBORHOOD_MIN_COUNT = 20
MARKET_SEGMENT_WEIGHT = 0.65
MARKET_NEIGHBORHOOD_WEIGHT = 0.35
EQUITY_SEGMENT_WEIGHT = 0.65
EQUITY_NEIGHBORHOOD_WEIGHT = 0.35
BENCHMARK_ASSUMPTION = (
    "Quote support data was generated from cohort-scoped peer distributions built from "
    "instant_quote_subject_cache and parcel_assessments. This benchmark is stronger than the "
    "prior public-summary proxy, but it is not the future AVM hybrid."
)


@dataclass(frozen=True)
class QuoteGenerationInputs:
    parcel_id: str
    county_id: str
    tax_year: int
    account_number: str
    neighborhood_code: str | None
    size_bucket: str | None
    age_bucket: str | None
    living_area_sf: float
    notice_value: float
    market_value: float
    assessed_value: float
    assessment_basis_value: float
    subject_assessed_psf: float
    effective_tax_rate: float
    homestead_flag: bool = False
    freeze_flag: bool = False


@dataclass(frozen=True)
class QuoteGenerationSummary:
    processed_count: int = 0
    created_count: int = 0
    skipped_count: int = 0

    def as_log_extra(self) -> dict[str, int]:
        return {
            "processed_count": self.processed_count,
            "created_count": self.created_count,
            "skipped_count": self.skipped_count,
        }


@dataclass(frozen=True)
class QuotePeerStats:
    peer_count: int
    market_p25_psf: float | None
    market_p50_psf: float | None
    market_p75_psf: float | None
    market_cv: float | None
    assessed_p25_psf: float | None
    assessed_p50_psf: float | None
    assessed_p75_psf: float | None
    assessed_cv: float | None


@dataclass(frozen=True)
class SelectedPeerSource:
    support_scope: str
    valid_comp_count: int
    segment_peer_count: int
    neighborhood_peer_count: int
    market_neighborhood_p50_psf: float
    market_p25_psf: float
    market_p50_psf: float
    market_p75_psf: float
    market_cv: float | None
    assessed_p25_psf: float
    assessed_p50_psf: float
    assessed_p75_psf: float
    assessed_cv: float | None
    market_comp_weight: float
    market_neighborhood_weight: float


class QuoteGenerationService:
    def __init__(self) -> None:
        self._market_model = MarketModelService()
        self._equity_model = EquityModelService()
        self._savings_engine = SavingsEngineService()
        self._decision_tree = ProtestDecisionTreeService()
        self._explanation_builder = ExplanationBuilderService()

    def score_models(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: Sequence[str] | None = None,
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0
        normalized_accounts = _normalize_account_numbers(account_numbers)

        with get_connection() as connection:
            with connection.cursor() as cursor:
                rows = self._fetch_subject_rows(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                    account_numbers=normalized_accounts,
                )
                existing_runs = self._fetch_existing_valuation_run_ids(
                    cursor,
                    parcel_ids=[str(row["parcel_id"]) for row in rows],
                    tax_year=tax_year,
                )

                for row in rows:
                    processed_count += 1
                    inputs = self._build_inputs(row)
                    if (inputs.parcel_id, inputs.tax_year) in existing_runs:
                        continue

                    valuation = self._build_valuation(
                        inputs,
                        segment_stats=self._peer_stats_from_row(row, prefix="segment"),
                        neighborhood_stats=self._peer_stats_from_row(row, prefix="neighborhood"),
                    )
                    if valuation is None:
                        continue

                    cursor.execute(
                        """
                        INSERT INTO valuation_runs (
                          parcel_id,
                          county_id,
                          tax_year,
                          run_status,
                          market_value_low,
                          market_value_point,
                          market_value_high,
                          equity_value_low,
                          equity_value_point,
                          equity_value_high,
                          defensible_value_low,
                          defensible_value_point,
                          defensible_value_high,
                          confidence_score,
                          market_model_version,
                          equity_model_version,
                          defensible_value_rule,
                          model_inputs_json
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            inputs.parcel_id,
                            inputs.county_id,
                            inputs.tax_year,
                            "completed",
                            valuation["market_value_low"],
                            valuation["market_value_point"],
                            valuation["market_value_high"],
                            valuation["equity_value_low"],
                            valuation["equity_value_point"],
                            valuation["equity_value_high"],
                            valuation["defensible_value_low"],
                            valuation["defensible_value_point"],
                            valuation["defensible_value_high"],
                            valuation["confidence_score"],
                            MODEL_VERSION,
                            MODEL_VERSION,
                            "min(market,equity)",
                            Jsonb(valuation["model_inputs_json"]),
                        ),
                    )
                    created_count += 1
            connection.commit()

        return QuoteGenerationSummary(
            processed_count=processed_count,
            created_count=created_count,
            skipped_count=processed_count - created_count,
        )

    def score_savings(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: Sequence[str] | None = None,
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0
        normalized_accounts = _normalize_account_numbers(account_numbers)

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      vr.valuation_run_id,
                      vr.parcel_id,
                      vr.county_id,
                      vr.tax_year,
                      vr.defensible_value_low,
                      vr.defensible_value_point,
                      vr.defensible_value_high,
                      vr.confidence_score,
                      iqsc.account_number,
                      iqsc.notice_value,
                      iqsc.effective_tax_rate
                    FROM valuation_runs vr
                    JOIN instant_quote_subject_cache iqsc
                      ON iqsc.parcel_id = vr.parcel_id
                     AND iqsc.tax_year = vr.tax_year
                    LEFT JOIN parcel_savings_estimates pse
                      ON pse.valuation_run_id = vr.valuation_run_id
                    WHERE (%s::text IS NULL OR vr.county_id = %s)
                      AND (%s::integer IS NULL OR vr.tax_year = %s)
                      AND (%s::text[] IS NULL OR iqsc.account_number = ANY(%s))
                      AND vr.market_model_version = %s
                      AND vr.equity_model_version = %s
                      AND pse.valuation_run_id IS NULL
                    ORDER BY vr.county_id, vr.tax_year, iqsc.account_number
                    """,
                    (
                        county_id,
                        county_id,
                        tax_year,
                        tax_year,
                        normalized_accounts,
                        normalized_accounts,
                        MODEL_VERSION,
                        MODEL_VERSION,
                    ),
                )
                rows = cursor.fetchall()

                for row in rows:
                    processed_count += 1
                    success_probability = self.derive_success_probability(row.get("confidence_score"))
                    savings = self._savings_engine.run(
                        current_notice_value=float(row["notice_value"]),
                        defensible_value_low=float(row["defensible_value_low"]),
                        defensible_value_point=float(row["defensible_value_point"]),
                        defensible_value_high=float(row["defensible_value_high"]),
                        effective_tax_rate=float(row["effective_tax_rate"]),
                        success_probability=success_probability,
                        contingency_rate=CONTINGENCY_RATE,
                    )
                    cursor.execute(
                        """
                        INSERT INTO parcel_savings_estimates (
                          parcel_id,
                          tax_year,
                          valuation_run_id,
                          projected_reduction_low,
                          projected_reduction_point,
                          projected_reduction_high,
                          effective_tax_rate,
                          gross_tax_savings_low,
                          gross_tax_savings_point,
                          gross_tax_savings_high,
                          success_probability,
                          expected_tax_savings_low,
                          expected_tax_savings_point,
                          expected_tax_savings_high,
                          estimated_contingency_fee
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            row["parcel_id"],
                            row["tax_year"],
                            row["valuation_run_id"],
                            savings["projected_reduction_low"],
                            savings["projected_reduction_point"],
                            savings["projected_reduction_high"],
                            row["effective_tax_rate"],
                            savings["gross_tax_savings_low"],
                            savings["gross_tax_savings_point"],
                            savings["gross_tax_savings_high"],
                            success_probability,
                            savings["expected_tax_savings_low"],
                            savings["expected_tax_savings_point"],
                            savings["expected_tax_savings_high"],
                            savings["estimated_contingency_fee"],
                        ),
                    )
                    created_count += 1
            connection.commit()

        return QuoteGenerationSummary(
            processed_count=processed_count,
            created_count=created_count,
            skipped_count=processed_count - created_count,
        )

    def refresh_quote_cache(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: Sequence[str] | None = None,
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0
        normalized_accounts = _normalize_account_numbers(account_numbers)

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      vr.valuation_run_id,
                      vr.parcel_id,
                      vr.county_id,
                      vr.tax_year,
                      vr.market_value_point,
                      vr.equity_value_point,
                      vr.defensible_value_point,
                      vr.confidence_score,
                      vr.model_inputs_json,
                      iqsc.account_number,
                      iqsc.notice_value,
                      pse.expected_tax_savings_point
                    FROM valuation_runs vr
                    JOIN instant_quote_subject_cache iqsc
                      ON iqsc.parcel_id = vr.parcel_id
                     AND iqsc.tax_year = vr.tax_year
                    JOIN parcel_savings_estimates pse
                      ON pse.valuation_run_id = vr.valuation_run_id
                    WHERE (%s::text IS NULL OR vr.county_id = %s)
                      AND (%s::integer IS NULL OR vr.tax_year = %s)
                      AND (%s::text[] IS NULL OR iqsc.account_number = ANY(%s))
                      AND vr.market_model_version = %s
                      AND vr.equity_model_version = %s
                    ORDER BY vr.county_id, vr.tax_year, iqsc.account_number
                    """,
                    (
                        county_id,
                        county_id,
                        tax_year,
                        tax_year,
                        normalized_accounts,
                        normalized_accounts,
                        MODEL_VERSION,
                        MODEL_VERSION,
                    ),
                )
                rows = cursor.fetchall()

                for row in rows:
                    processed_count += 1
                    created_for_run = False
                    confidence_label = self.derive_confidence_label(row.get("confidence_score"))
                    model_inputs = dict(row.get("model_inputs_json") or {})
                    valid_comp_count = int(model_inputs.get("valid_comp_count") or 0)
                    support_scope = str(model_inputs.get("support_scope") or "unsupported")
                    segment_peer_count = int(model_inputs.get("segment_peer_count") or 0)
                    neighborhood_peer_count = int(model_inputs.get("neighborhood_peer_count") or 0)
                    value_gap_percent = (
                        max(0.0, float(row["notice_value"]) - float(row["defensible_value_point"]))
                        / float(row["notice_value"])
                        if float(row["notice_value"]) > 0
                        else 0.0
                    )
                    decision_tree = self._decision_tree.evaluate(
                        current_notice_value=float(row["notice_value"]),
                        defensible_value_point=float(row["defensible_value_point"]),
                        valid_comp_count=valid_comp_count,
                        expected_tax_savings_point=float(row["expected_tax_savings_point"]),
                        confidence_score=float(row.get("confidence_score") or 0.0),
                    )
                    recommendation_code = self.derive_recommendation_code(decision_tree)
                    explanation = self._explanation_builder.build(
                        market_value_point=float(row["market_value_point"]),
                        equity_value_point=float(row["equity_value_point"]),
                        defensible_value_point=float(row["defensible_value_point"]),
                        confidence_label=confidence_label,
                        recommendation_code=recommendation_code,
                        support_scope=support_scope,
                        segment_peer_count=segment_peer_count,
                        neighborhood_peer_count=neighborhood_peer_count,
                        value_gap_percent=value_gap_percent,
                        generation_mode=GENERATION_MODE,
                    )
                    explanation_bullets = list(explanation.get("explanation_bullets") or [])
                    explanation_bullets.append(BENCHMARK_ASSUMPTION)
                    explanation_json = dict(explanation.get("explanation_json") or {})
                    explanation_json["generation_mode"] = GENERATION_MODE
                    explanation_json["source_assumption"] = BENCHMARK_ASSUMPTION

                    if not self._decision_tree_exists(cursor, valuation_run_id=row["valuation_run_id"]):
                        for result in decision_tree:
                            cursor.execute(
                                """
                                INSERT INTO decision_tree_results (
                                  parcel_id,
                                  tax_year,
                                  valuation_run_id,
                                  rule_code,
                                  rule_result,
                                  rule_score,
                                  rule_payload_json
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    row["parcel_id"],
                                    row["tax_year"],
                                    row["valuation_run_id"],
                                    result["rule_code"],
                                    result["rule_result"],
                                    self._rule_score(result["rule_result"]),
                                    Jsonb(
                                        {
                                            "generation_mode": GENERATION_MODE,
                                            "account_number": row["account_number"],
                                            "support_scope": support_scope,
                                            "valid_comp_count": valid_comp_count,
                                        }
                                    ),
                                ),
                            )
                        created_for_run = True

                    if not self._recommendation_exists(cursor, valuation_run_id=row["valuation_run_id"]):
                        cursor.execute(
                            """
                            INSERT INTO protest_recommendations (
                              parcel_id,
                              tax_year,
                              valuation_run_id,
                              recommendation_code,
                              recommendation_reason,
                              confidence
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                row["parcel_id"],
                                row["tax_year"],
                                row["valuation_run_id"],
                                recommendation_code,
                                self._build_recommendation_reason(
                                    recommendation_code=recommendation_code,
                                    confidence_label=confidence_label,
                                    support_scope=support_scope,
                                    value_gap_percent=value_gap_percent,
                                ),
                                row["confidence_score"],
                            ),
                        )
                        created_for_run = True

                    if not self._explanation_exists(cursor, valuation_run_id=row["valuation_run_id"]):
                        cursor.execute(
                            """
                            INSERT INTO quote_explanations (
                              parcel_id,
                              tax_year,
                              valuation_run_id,
                              explanation_json,
                              basis,
                              confidence_label,
                              explanation_bullets
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                row["parcel_id"],
                                row["tax_year"],
                                row["valuation_run_id"],
                                Jsonb(explanation_json),
                                explanation["basis"],
                                confidence_label,
                                Jsonb(explanation_bullets),
                            ),
                        )
                        created_for_run = True

                    if created_for_run:
                        created_count += 1
            connection.commit()

        return QuoteGenerationSummary(
            processed_count=processed_count,
            created_count=created_count,
            skipped_count=processed_count - created_count,
        )

    def _fetch_subject_rows(
        self,
        cursor: Any,
        *,
        county_id: str | None,
        tax_year: int | None,
        account_numbers: list[str] | None,
    ) -> list[dict[str, Any]]:
        cursor.execute("DROP TABLE IF EXISTS tmp_quote_generation_subjects")
        cursor.execute("DROP TABLE IF EXISTS tmp_quote_generation_peer_pool")
        cursor.execute("DROP TABLE IF EXISTS tmp_quote_generation_neighborhood_stats")
        cursor.execute("DROP TABLE IF EXISTS tmp_quote_generation_segment_stats")

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_quote_generation_subjects AS
            SELECT
              cache.parcel_id,
              cache.county_id,
              cache.tax_year,
              cache.account_number,
              cache.neighborhood_code,
              cache.size_bucket,
              cache.age_bucket,
              cache.living_area_sf::double precision AS living_area_sf,
              cache.notice_value::double precision AS notice_value,
              pa.market_value::double precision AS market_value,
              cache.assessed_value::double precision AS assessed_value,
              cache.assessment_basis_value::double precision AS assessment_basis_value,
              cache.subject_assessed_psf::double precision AS subject_assessed_psf,
              cache.effective_tax_rate::double precision AS effective_tax_rate,
              COALESCE(cache.homestead_flag, false) AS homestead_flag,
              COALESCE(cache.freeze_flag, false) AS freeze_flag
            FROM instant_quote_subject_cache cache
            JOIN parcel_assessments pa
              ON pa.parcel_id = cache.parcel_id
             AND pa.tax_year = cache.tax_year
            WHERE (%s::text IS NULL OR cache.county_id = %s)
              AND (%s::integer IS NULL OR cache.tax_year = %s)
              AND (%s::text[] IS NULL OR cache.account_number = ANY(%s))
              AND cache.property_type_code = 'sfr'
              AND cache.support_blocker_code IS NULL
              AND cache.neighborhood_code IS NOT NULL
              AND btrim(cache.neighborhood_code) <> ''
              AND cache.living_area_sf IS NOT NULL
              AND cache.living_area_sf > 0
              AND cache.notice_value IS NOT NULL
              AND cache.notice_value > 0
              AND pa.market_value IS NOT NULL
              AND pa.market_value > 0
              AND cache.assessment_basis_value IS NOT NULL
              AND cache.assessment_basis_value > 0
              AND cache.subject_assessed_psf IS NOT NULL
              AND cache.subject_assessed_psf > 0
              AND cache.effective_tax_rate IS NOT NULL
              AND cache.effective_tax_rate > 0
            ORDER BY cache.county_id, cache.tax_year, cache.account_number
            """,
            (county_id, county_id, tax_year, tax_year, account_numbers, account_numbers),
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX idx_tmp_quote_generation_subjects_key
              ON tmp_quote_generation_subjects(parcel_id, tax_year)
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_tmp_quote_generation_subjects_segment
              ON tmp_quote_generation_subjects(county_id, tax_year, neighborhood_code, size_bucket, age_bucket)
            """
        )
        cursor.execute("ANALYZE tmp_quote_generation_subjects")
        cursor.execute("SELECT COUNT(*)::integer AS count FROM tmp_quote_generation_subjects")
        if int((cursor.fetchone() or {}).get("count") or 0) == 0:
            return []

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_quote_generation_peer_pool AS
            WITH target_neighborhoods AS (
              SELECT DISTINCT county_id, tax_year, neighborhood_code
              FROM tmp_quote_generation_subjects
            )
            SELECT
              cache.county_id,
              cache.tax_year,
              cache.parcel_id,
              cache.neighborhood_code,
              cache.size_bucket,
              cache.age_bucket,
              cache.subject_assessed_psf::double precision AS assessed_psf,
              (pa.market_value / NULLIF(cache.living_area_sf, 0))::double precision AS market_psf
            FROM instant_quote_subject_cache cache
            JOIN target_neighborhoods target
              ON target.county_id = cache.county_id
             AND target.tax_year = cache.tax_year
             AND target.neighborhood_code = cache.neighborhood_code
            JOIN parcel_assessments pa
              ON pa.parcel_id = cache.parcel_id
             AND pa.tax_year = cache.tax_year
            WHERE cache.property_type_code = 'sfr'
              AND cache.support_blocker_code IS NULL
              AND cache.living_area_sf IS NOT NULL
              AND cache.living_area_sf > 0
              AND cache.subject_assessed_psf IS NOT NULL
              AND cache.subject_assessed_psf > 0
              AND pa.market_value IS NOT NULL
              AND pa.market_value > 0
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_tmp_quote_generation_peer_pool_neighborhood
              ON tmp_quote_generation_peer_pool(county_id, tax_year, neighborhood_code)
            """
        )
        cursor.execute(
            """
            CREATE INDEX idx_tmp_quote_generation_peer_pool_segment
              ON tmp_quote_generation_peer_pool(county_id, tax_year, neighborhood_code, size_bucket, age_bucket)
            """
        )
        cursor.execute("ANALYZE tmp_quote_generation_peer_pool")

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_quote_generation_neighborhood_stats AS
            SELECT
              county_id,
              tax_year,
              neighborhood_code,
              COUNT(*)::integer AS peer_count,
              percentile_cont(0.25) WITHIN GROUP (ORDER BY market_psf) AS market_p25_psf,
              percentile_cont(0.50) WITHIN GROUP (ORDER BY market_psf) AS market_p50_psf,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY market_psf) AS market_p75_psf,
              CASE
                WHEN AVG(market_psf) = 0 THEN NULL
                ELSE COALESCE(stddev_pop(market_psf), 0) / AVG(market_psf)
              END AS market_cv,
              percentile_cont(0.25) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p25_psf,
              percentile_cont(0.50) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p50_psf,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p75_psf,
              CASE
                WHEN AVG(assessed_psf) = 0 THEN NULL
                ELSE COALESCE(stddev_pop(assessed_psf), 0) / AVG(assessed_psf)
              END AS assessed_cv
            FROM tmp_quote_generation_peer_pool
            GROUP BY county_id, tax_year, neighborhood_code
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX idx_tmp_quote_generation_neighborhood_stats
              ON tmp_quote_generation_neighborhood_stats(county_id, tax_year, neighborhood_code)
            """
        )
        cursor.execute("ANALYZE tmp_quote_generation_neighborhood_stats")

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_quote_generation_segment_stats AS
            SELECT
              county_id,
              tax_year,
              neighborhood_code,
              size_bucket,
              age_bucket,
              COUNT(*)::integer AS peer_count,
              percentile_cont(0.25) WITHIN GROUP (ORDER BY market_psf) AS market_p25_psf,
              percentile_cont(0.50) WITHIN GROUP (ORDER BY market_psf) AS market_p50_psf,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY market_psf) AS market_p75_psf,
              CASE
                WHEN AVG(market_psf) = 0 THEN NULL
                ELSE COALESCE(stddev_pop(market_psf), 0) / AVG(market_psf)
              END AS market_cv,
              percentile_cont(0.25) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p25_psf,
              percentile_cont(0.50) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p50_psf,
              percentile_cont(0.75) WITHIN GROUP (ORDER BY assessed_psf) AS assessed_p75_psf,
              CASE
                WHEN AVG(assessed_psf) = 0 THEN NULL
                ELSE COALESCE(stddev_pop(assessed_psf), 0) / AVG(assessed_psf)
              END AS assessed_cv
            FROM tmp_quote_generation_peer_pool
            GROUP BY county_id, tax_year, neighborhood_code, size_bucket, age_bucket
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX idx_tmp_quote_generation_segment_stats
              ON tmp_quote_generation_segment_stats(county_id, tax_year, neighborhood_code, size_bucket, age_bucket)
            """
        )
        cursor.execute("ANALYZE tmp_quote_generation_segment_stats")

        cursor.execute(
            """
            SELECT
              subjects.*,
              neighborhood.peer_count AS neighborhood_peer_count,
              neighborhood.market_p25_psf AS neighborhood_market_p25_psf,
              neighborhood.market_p50_psf AS neighborhood_market_p50_psf,
              neighborhood.market_p75_psf AS neighborhood_market_p75_psf,
              neighborhood.market_cv AS neighborhood_market_cv,
              neighborhood.assessed_p25_psf AS neighborhood_assessed_p25_psf,
              neighborhood.assessed_p50_psf AS neighborhood_assessed_p50_psf,
              neighborhood.assessed_p75_psf AS neighborhood_assessed_p75_psf,
              neighborhood.assessed_cv AS neighborhood_assessed_cv,
              segment.peer_count AS segment_peer_count,
              segment.market_p25_psf AS segment_market_p25_psf,
              segment.market_p50_psf AS segment_market_p50_psf,
              segment.market_p75_psf AS segment_market_p75_psf,
              segment.market_cv AS segment_market_cv,
              segment.assessed_p25_psf AS segment_assessed_p25_psf,
              segment.assessed_p50_psf AS segment_assessed_p50_psf,
              segment.assessed_p75_psf AS segment_assessed_p75_psf,
              segment.assessed_cv AS segment_assessed_cv
            FROM tmp_quote_generation_subjects subjects
            LEFT JOIN tmp_quote_generation_neighborhood_stats neighborhood
              ON neighborhood.county_id = subjects.county_id
             AND neighborhood.tax_year = subjects.tax_year
             AND neighborhood.neighborhood_code = subjects.neighborhood_code
            LEFT JOIN tmp_quote_generation_segment_stats segment
              ON segment.county_id = subjects.county_id
             AND segment.tax_year = subjects.tax_year
             AND segment.neighborhood_code = subjects.neighborhood_code
             AND segment.size_bucket = subjects.size_bucket
             AND segment.age_bucket = subjects.age_bucket
            ORDER BY subjects.county_id, subjects.tax_year, subjects.account_number
            """
        )
        return list(cursor.fetchall())

    def _build_inputs(self, row: dict[str, Any]) -> QuoteGenerationInputs:
        return QuoteGenerationInputs(
            parcel_id=str(row["parcel_id"]),
            county_id=str(row["county_id"]),
            tax_year=int(row["tax_year"]),
            account_number=str(row["account_number"]),
            neighborhood_code=(
                str(row["neighborhood_code"]) if row.get("neighborhood_code") is not None else None
            ),
            size_bucket=str(row["size_bucket"]) if row.get("size_bucket") is not None else None,
            age_bucket=str(row["age_bucket"]) if row.get("age_bucket") is not None else None,
            living_area_sf=float(row["living_area_sf"]),
            notice_value=float(row["notice_value"]),
            market_value=float(row["market_value"]),
            assessed_value=float(row["assessed_value"]),
            assessment_basis_value=float(row["assessment_basis_value"]),
            subject_assessed_psf=float(row["subject_assessed_psf"]),
            effective_tax_rate=float(row["effective_tax_rate"]),
            homestead_flag=bool(row.get("homestead_flag")),
            freeze_flag=bool(row.get("freeze_flag")),
        )

    def _build_valuation(
        self,
        inputs: QuoteGenerationInputs,
        *,
        segment_stats: QuotePeerStats | None,
        neighborhood_stats: QuotePeerStats | None,
    ) -> dict[str, Any] | None:
        selected_source = self._select_peer_source(
            segment_stats=segment_stats,
            neighborhood_stats=neighborhood_stats,
        )
        if selected_source is None:
            return None

        confidence_score = self._derive_confidence_score(
            inputs=inputs,
            selected_source=selected_source,
        )
        market_inputs = MarketModelInputs(
            subject_living_area_sf=inputs.living_area_sf,
            neighborhood_median_sale_psf_12m=selected_source.market_neighborhood_p50_psf,
            adjusted_comp_value_psf=[
                selected_source.market_p25_psf,
                selected_source.market_p50_psf,
                selected_source.market_p75_psf,
            ],
            comp_rank_weights=[0.2, 0.6, 0.2],
            low_value_psf=selected_source.market_p25_psf,
            high_value_psf=selected_source.market_p75_psf,
            confidence_score=confidence_score,
            neighborhood_weight=selected_source.market_neighborhood_weight,
            comp_weight=selected_source.market_comp_weight,
        )
        market_result = self._market_model.run(market_inputs)
        equity_result = self._equity_model.run(
            subject_living_area_sf=inputs.living_area_sf,
            adjusted_equity_comp_psf=[
                selected_source.assessed_p25_psf,
                selected_source.assessed_p50_psf,
                selected_source.assessed_p50_psf,
                selected_source.assessed_p75_psf,
            ],
            low_value_psf=selected_source.assessed_p25_psf,
            high_value_psf=selected_source.assessed_p75_psf,
            confidence_score=confidence_score,
        )

        defensible_low = min(
            float(market_result["market_value_low"]),
            float(equity_result["equity_value_low"]),
        )
        defensible_point = min(
            float(market_result["market_value_point"]),
            float(equity_result["equity_value_point"]),
        )
        defensible_high = min(
            float(market_result["market_value_high"]),
            float(equity_result["equity_value_high"]),
        )
        notice_gap_ratio = (
            max(inputs.notice_value - defensible_point, 0.0) / inputs.notice_value
            if inputs.notice_value > 0
            else 0.0
        )

        return {
            "market_value_low": float(market_result["market_value_low"]),
            "market_value_point": float(market_result["market_value_point"]),
            "market_value_high": float(market_result["market_value_high"]),
            "equity_value_low": float(equity_result["equity_value_low"]),
            "equity_value_point": float(equity_result["equity_value_point"]),
            "equity_value_high": float(equity_result["equity_value_high"]),
            "defensible_value_low": defensible_low,
            "defensible_value_point": defensible_point,
            "defensible_value_high": defensible_high,
            "confidence_score": confidence_score,
            "model_inputs_json": {
                "generation_mode": GENERATION_MODE,
                "source_view": "instant_quote_subject_cache",
                "source_assumptions": [BENCHMARK_ASSUMPTION],
                "account_number": inputs.account_number,
                "county_id": inputs.county_id,
                "tax_year": inputs.tax_year,
                "neighborhood_code": inputs.neighborhood_code,
                "size_bucket": inputs.size_bucket,
                "age_bucket": inputs.age_bucket,
                "support_scope": selected_source.support_scope,
                "segment_peer_count": selected_source.segment_peer_count,
                "neighborhood_peer_count": selected_source.neighborhood_peer_count,
                "valid_comp_count": selected_source.valid_comp_count,
                "subject_assessed_psf": inputs.subject_assessed_psf,
                "notice_to_defensible_gap_ratio": round(notice_gap_ratio, 6),
                "market_inputs": {
                    "subject_living_area_sf": inputs.living_area_sf,
                    "neighborhood_median_sale_psf_12m": selected_source.market_neighborhood_p50_psf,
                    "adjusted_comp_value_psf": [
                        selected_source.market_p25_psf,
                        selected_source.market_p50_psf,
                        selected_source.market_p75_psf,
                    ],
                    "comp_rank_weights": [0.2, 0.6, 0.2],
                    "low_value_psf": selected_source.market_p25_psf,
                    "high_value_psf": selected_source.market_p75_psf,
                    "neighborhood_weight": selected_source.market_neighborhood_weight,
                    "comp_weight": selected_source.market_comp_weight,
                    "market_cv": selected_source.market_cv,
                },
                "equity_inputs": {
                    "subject_living_area_sf": inputs.living_area_sf,
                    "adjusted_equity_comp_psf": [
                        selected_source.assessed_p25_psf,
                        selected_source.assessed_p50_psf,
                        selected_source.assessed_p50_psf,
                        selected_source.assessed_p75_psf,
                    ],
                    "low_value_psf": selected_source.assessed_p25_psf,
                    "high_value_psf": selected_source.assessed_p75_psf,
                    "assessed_cv": selected_source.assessed_cv,
                },
            },
        }

    def _select_peer_source(
        self,
        *,
        segment_stats: QuotePeerStats | None,
        neighborhood_stats: QuotePeerStats | None,
    ) -> SelectedPeerSource | None:
        if (
            neighborhood_stats is None
            or neighborhood_stats.peer_count < NEIGHBORHOOD_MIN_COUNT
            or neighborhood_stats.market_p50_psf is None
            or neighborhood_stats.assessed_p50_psf is None
            or neighborhood_stats.market_p25_psf is None
            or neighborhood_stats.market_p75_psf is None
            or neighborhood_stats.assessed_p25_psf is None
            or neighborhood_stats.assessed_p75_psf is None
        ):
            return None

        if (
            segment_stats is not None
            and segment_stats.peer_count >= SEGMENT_MIN_COUNT
            and segment_stats.market_p50_psf is not None
            and segment_stats.assessed_p50_psf is not None
            and segment_stats.market_p25_psf is not None
            and segment_stats.market_p75_psf is not None
            and segment_stats.assessed_p25_psf is not None
            and segment_stats.assessed_p75_psf is not None
        ):
            return SelectedPeerSource(
                support_scope="segment_within_neighborhood",
                valid_comp_count=segment_stats.peer_count,
                segment_peer_count=segment_stats.peer_count,
                neighborhood_peer_count=neighborhood_stats.peer_count,
                market_neighborhood_p50_psf=float(neighborhood_stats.market_p50_psf),
                market_p25_psf=_blend(
                    float(segment_stats.market_p25_psf),
                    float(neighborhood_stats.market_p25_psf),
                    segment_weight=MARKET_SEGMENT_WEIGHT,
                    neighborhood_weight=MARKET_NEIGHBORHOOD_WEIGHT,
                ),
                market_p50_psf=_blend(
                    float(segment_stats.market_p50_psf),
                    float(neighborhood_stats.market_p50_psf),
                    segment_weight=MARKET_SEGMENT_WEIGHT,
                    neighborhood_weight=MARKET_NEIGHBORHOOD_WEIGHT,
                ),
                market_p75_psf=_blend(
                    float(segment_stats.market_p75_psf),
                    float(neighborhood_stats.market_p75_psf),
                    segment_weight=MARKET_SEGMENT_WEIGHT,
                    neighborhood_weight=MARKET_NEIGHBORHOOD_WEIGHT,
                ),
                market_cv=max_defined(segment_stats.market_cv, neighborhood_stats.market_cv),
                assessed_p25_psf=_blend(
                    float(segment_stats.assessed_p25_psf),
                    float(neighborhood_stats.assessed_p25_psf),
                    segment_weight=EQUITY_SEGMENT_WEIGHT,
                    neighborhood_weight=EQUITY_NEIGHBORHOOD_WEIGHT,
                ),
                assessed_p50_psf=_blend(
                    float(segment_stats.assessed_p50_psf),
                    float(neighborhood_stats.assessed_p50_psf),
                    segment_weight=EQUITY_SEGMENT_WEIGHT,
                    neighborhood_weight=EQUITY_NEIGHBORHOOD_WEIGHT,
                ),
                assessed_p75_psf=_blend(
                    float(segment_stats.assessed_p75_psf),
                    float(neighborhood_stats.assessed_p75_psf),
                    segment_weight=EQUITY_SEGMENT_WEIGHT,
                    neighborhood_weight=EQUITY_NEIGHBORHOOD_WEIGHT,
                ),
                assessed_cv=max_defined(segment_stats.assessed_cv, neighborhood_stats.assessed_cv),
                market_comp_weight=MARKET_SEGMENT_WEIGHT,
                market_neighborhood_weight=MARKET_NEIGHBORHOOD_WEIGHT,
            )

        return SelectedPeerSource(
            support_scope="neighborhood_only",
            valid_comp_count=neighborhood_stats.peer_count,
            segment_peer_count=segment_stats.peer_count if segment_stats is not None else 0,
            neighborhood_peer_count=neighborhood_stats.peer_count,
            market_neighborhood_p50_psf=float(neighborhood_stats.market_p50_psf),
            market_p25_psf=float(neighborhood_stats.market_p25_psf),
            market_p50_psf=float(neighborhood_stats.market_p50_psf),
            market_p75_psf=float(neighborhood_stats.market_p75_psf),
            market_cv=neighborhood_stats.market_cv,
            assessed_p25_psf=float(neighborhood_stats.assessed_p25_psf),
            assessed_p50_psf=float(neighborhood_stats.assessed_p50_psf),
            assessed_p75_psf=float(neighborhood_stats.assessed_p75_psf),
            assessed_cv=neighborhood_stats.assessed_cv,
            market_comp_weight=0.5,
            market_neighborhood_weight=0.5,
        )

    def _derive_confidence_score(
        self,
        *,
        inputs: QuoteGenerationInputs,
        selected_source: SelectedPeerSource,
    ) -> float:
        score = 0.74 if selected_source.support_scope == "segment_within_neighborhood" else 0.64

        if selected_source.valid_comp_count < 10:
            score -= 0.03
        elif selected_source.valid_comp_count >= 20:
            score += 0.02

        if selected_source.neighborhood_peer_count < 30:
            score -= 0.02

        for cv in (selected_source.market_cv, selected_source.assessed_cv):
            if cv is None:
                score -= 0.02
            elif cv > 0.40:
                score -= 0.08
            elif cv > 0.25:
                score -= 0.04

        target_psf = selected_source.assessed_p50_psf
        outlier_gap = (
            abs(inputs.subject_assessed_psf - target_psf) / target_psf
            if target_psf > 0
            else 1.0
        )
        if outlier_gap > 0.50:
            score -= 0.08
        elif outlier_gap > 0.35:
            score -= 0.05
        elif outlier_gap > 0.20:
            score -= 0.02

        if inputs.freeze_flag:
            score -= 0.01

        return round(min(max(score, 0.35), 0.82), 4)

    def _peer_stats_from_row(
        self,
        row: dict[str, Any],
        *,
        prefix: str,
    ) -> QuotePeerStats | None:
        peer_count = int(row.get(f"{prefix}_peer_count") or 0)
        if peer_count <= 0:
            return None
        return QuotePeerStats(
            peer_count=peer_count,
            market_p25_psf=_as_float(row.get(f"{prefix}_market_p25_psf")),
            market_p50_psf=_as_float(row.get(f"{prefix}_market_p50_psf")),
            market_p75_psf=_as_float(row.get(f"{prefix}_market_p75_psf")),
            market_cv=_as_float(row.get(f"{prefix}_market_cv")),
            assessed_p25_psf=_as_float(row.get(f"{prefix}_assessed_p25_psf")),
            assessed_p50_psf=_as_float(row.get(f"{prefix}_assessed_p50_psf")),
            assessed_p75_psf=_as_float(row.get(f"{prefix}_assessed_p75_psf")),
            assessed_cv=_as_float(row.get(f"{prefix}_assessed_cv")),
        )

    def _fetch_existing_valuation_run_ids(
        self,
        cursor: Any,
        *,
        parcel_ids: Sequence[str],
        tax_year: int | None,
    ) -> set[tuple[str, int]]:
        if not parcel_ids:
            return set()
        cursor.execute(
            """
            SELECT DISTINCT parcel_id::text AS parcel_id, tax_year
            FROM valuation_runs
            WHERE parcel_id = ANY(%s::uuid[])
              AND (%s::integer IS NULL OR tax_year = %s)
              AND market_model_version = %s
              AND equity_model_version = %s
            """,
            (list(parcel_ids), tax_year, tax_year, MODEL_VERSION, MODEL_VERSION),
        )
        return {(str(row["parcel_id"]), int(row["tax_year"])) for row in cursor.fetchall()}

    def _decision_tree_exists(self, cursor: Any, *, valuation_run_id: str) -> bool:
        cursor.execute(
            "SELECT 1 AS exists_flag FROM decision_tree_results WHERE valuation_run_id = %s LIMIT 1",
            (valuation_run_id,),
        )
        return cursor.fetchone() is not None

    def _recommendation_exists(self, cursor: Any, *, valuation_run_id: str) -> bool:
        cursor.execute(
            "SELECT 1 AS exists_flag FROM protest_recommendations WHERE valuation_run_id = %s LIMIT 1",
            (valuation_run_id,),
        )
        return cursor.fetchone() is not None

    def _explanation_exists(self, cursor: Any, *, valuation_run_id: str) -> bool:
        cursor.execute(
            "SELECT 1 AS exists_flag FROM quote_explanations WHERE valuation_run_id = %s LIMIT 1",
            (valuation_run_id,),
        )
        return cursor.fetchone() is not None

    def _build_recommendation_reason(
        self,
        *,
        recommendation_code: str,
        confidence_label: str,
        support_scope: str,
        value_gap_percent: float,
    ) -> str:
        if recommendation_code == "file_protest":
            return (
                "Peer-backed benchmark support is actionable and the estimated notice gap cleared "
                f"the current protest gates ({value_gap_percent:.1%} gap, {confidence_label} confidence)."
            )
        if recommendation_code == "manual_review":
            return (
                "Peer-backed benchmark found some protest signal, but confidence remains limited "
                f"for a {support_scope} quote."
            )
        return (
            "Peer-backed benchmark did not clear the current savings or confidence gates for a "
            f"{support_scope} recommendation."
        )

    @staticmethod
    def derive_success_probability(confidence_score: float | None) -> float:
        score = float(confidence_score or 0.0)
        if score >= 0.72:
            return 0.80
        if score >= 0.66:
            return 0.65
        if score >= 0.60:
            return 0.50
        return 0.30

    @staticmethod
    def derive_confidence_label(confidence_score: float | None) -> str:
        score = float(confidence_score or 0.0)
        if score >= 0.75:
            return "high"
        if score >= 0.62:
            return "medium"
        return "limited"

    @staticmethod
    def derive_recommendation_code(decision_tree_results: list[dict[str, Any]]) -> str:
        results = {str(result["rule_result"]) for result in decision_tree_results}
        if results == {"pass"}:
            return "file_protest"
        if "manual_review" in results:
            return "manual_review"
        return "reject"

    @staticmethod
    def _rule_score(rule_result: str) -> float:
        if rule_result == "pass":
            return 1.0
        if rule_result == "manual_review":
            return 0.5
        return 0.0


def _normalize_account_numbers(account_numbers: Sequence[str] | None) -> list[str] | None:
    if not account_numbers:
        return None
    normalized = [value.strip() for value in account_numbers if value and value.strip()]
    if not normalized:
        return None
    return list(dict.fromkeys(normalized))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _blend(
    segment_value: float,
    neighborhood_value: float,
    *,
    segment_weight: float,
    neighborhood_weight: float,
) -> float:
    total_weight = segment_weight + neighborhood_weight
    if total_weight <= 0:
        return neighborhood_value
    return ((segment_value * segment_weight) + (neighborhood_value * neighborhood_weight)) / total_weight


def max_defined(*values: float | None) -> float | None:
    defined = [value for value in values if value is not None]
    if not defined:
        return None
    return max(defined)
