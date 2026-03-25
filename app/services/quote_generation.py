from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.decision_tree import ProtestDecisionTreeService
from app.services.equity_model import EquityModelService
from app.services.explanation_builder import ExplanationBuilderService
from app.services.market_model import MarketModelInputs, MarketModelService
from app.services.savings_engine import SavingsEngineService

GENERATION_MODE = "stage13_public_summary_proxy"
MODEL_VERSION = "stage13_public_summary_proxy_v1"
CONTINGENCY_RATE = 0.35
PROXY_ASSUMPTION = (
    "Quote support data was generated from parcel_summary_view because deeper comp and feature "
    "tables are not populated yet for this county-year."
)


@dataclass(frozen=True)
class QuoteGenerationInputs:
    parcel_id: str
    county_id: str
    tax_year: int
    account_number: str
    neighborhood_code: str | None
    living_area_sf: float
    notice_value: float
    market_value: float
    assessed_value: float
    effective_tax_rate: float


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
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0

        with get_connection() as connection:
            with connection.cursor() as cursor:
                for row in self._fetch_subject_rows(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                ):
                    processed_count += 1
                    inputs = self._build_inputs(row)
                    if self._fetch_existing_valuation_run_id(
                        cursor,
                        parcel_id=inputs.parcel_id,
                        tax_year=inputs.tax_year,
                    ):
                        continue

                    valuation = self._build_valuation(inputs)
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
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0

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
                      psv.account_number,
                      psv.notice_value,
                      psv.effective_tax_rate
                    FROM valuation_runs vr
                    JOIN parcel_summary_view psv
                      ON psv.parcel_id = vr.parcel_id
                     AND psv.tax_year = vr.tax_year
                    LEFT JOIN parcel_savings_estimates pse
                      ON pse.valuation_run_id = vr.valuation_run_id
                    WHERE (%s::text IS NULL OR vr.county_id = %s)
                      AND (%s::integer IS NULL OR vr.tax_year = %s)
                      AND vr.market_model_version = %s
                      AND vr.equity_model_version = %s
                      AND pse.valuation_run_id IS NULL
                    ORDER BY vr.county_id, vr.tax_year, psv.account_number
                    """,
                    (county_id, county_id, tax_year, tax_year, MODEL_VERSION, MODEL_VERSION),
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
    ) -> QuoteGenerationSummary:
        processed_count = 0
        created_count = 0

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
                      psv.account_number,
                      psv.notice_value,
                      pse.expected_tax_savings_point
                    FROM valuation_runs vr
                    JOIN parcel_summary_view psv
                      ON psv.parcel_id = vr.parcel_id
                     AND psv.tax_year = vr.tax_year
                    JOIN parcel_savings_estimates pse
                      ON pse.valuation_run_id = vr.valuation_run_id
                    WHERE (%s::text IS NULL OR vr.county_id = %s)
                      AND (%s::integer IS NULL OR vr.tax_year = %s)
                      AND vr.market_model_version = %s
                      AND vr.equity_model_version = %s
                    ORDER BY vr.county_id, vr.tax_year, psv.account_number
                    """,
                    (county_id, county_id, tax_year, tax_year, MODEL_VERSION, MODEL_VERSION),
                )
                rows = cursor.fetchall()

                for row in rows:
                    processed_count += 1
                    created_for_run = False
                    confidence_label = self.derive_confidence_label(row.get("confidence_score"))
                    decision_tree = self._decision_tree.evaluate(
                        current_notice_value=float(row["notice_value"]),
                        defensible_value_point=float(row["defensible_value_point"]),
                        valid_comp_count=3,
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
                    )
                    explanation_bullets = list(explanation.get("explanation_bullets") or [])
                    explanation_bullets.append(PROXY_ASSUMPTION)
                    explanation_json = dict(explanation.get("explanation_json") or {})
                    explanation_json["generation_mode"] = GENERATION_MODE
                    explanation_json["source_assumption"] = PROXY_ASSUMPTION

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
                                PROXY_ASSUMPTION,
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
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              parcel_id,
              county_id,
              tax_year,
              account_number,
              neighborhood_code,
              living_area_sf,
              notice_value,
              market_value,
              assessed_value,
              effective_tax_rate
            FROM parcel_summary_view
            WHERE (%s::text IS NULL OR county_id = %s)
              AND (%s::integer IS NULL OR tax_year = %s)
              AND property_type_code = 'sfr'
              AND living_area_sf IS NOT NULL
              AND living_area_sf > 0
              AND notice_value IS NOT NULL
              AND notice_value > 0
              AND market_value IS NOT NULL
              AND market_value > 0
              AND assessed_value IS NOT NULL
              AND assessed_value > 0
              AND effective_tax_rate IS NOT NULL
              AND effective_tax_rate > 0
            ORDER BY county_id, tax_year, account_number
            """,
            (county_id, county_id, tax_year, tax_year),
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
            living_area_sf=float(row["living_area_sf"]),
            notice_value=float(row["notice_value"]),
            market_value=float(row["market_value"]),
            assessed_value=float(row["assessed_value"]),
            effective_tax_rate=float(row["effective_tax_rate"]),
        )

    def _build_valuation(self, inputs: QuoteGenerationInputs) -> dict[str, Any]:
        market_psf = max(inputs.market_value / inputs.living_area_sf, 1.0)
        equity_psf = max(inputs.assessed_value / inputs.living_area_sf, 1.0)

        market_inputs = MarketModelInputs(
            subject_living_area_sf=inputs.living_area_sf,
            neighborhood_median_sale_psf_12m=market_psf,
            adjusted_comp_value_psf=[
                round(market_psf * 0.98, 4),
                round(market_psf, 4),
                round(market_psf * 1.02, 4),
            ],
            comp_rank_weights=[0.5, 0.3, 0.2],
        )
        market_result = self._market_model.run(market_inputs)
        equity_result = self._equity_model.run(
            subject_living_area_sf=inputs.living_area_sf,
            adjusted_equity_comp_psf=[
                round(equity_psf * 0.96, 4),
                round(equity_psf * 0.99, 4),
                round(equity_psf, 4),
                round(equity_psf * 1.02, 4),
            ],
        )
        confidence_score = round(
            (
                float(market_result["confidence_score"])
                + float(equity_result["confidence_score"])
            )
            / 2,
            4,
        )

        return {
            "market_value_low": float(market_result["market_value_low"]),
            "market_value_point": float(market_result["market_value_point"]),
            "market_value_high": float(market_result["market_value_high"]),
            "equity_value_low": float(equity_result["equity_value_low"]),
            "equity_value_point": float(equity_result["equity_value_point"]),
            "equity_value_high": float(equity_result["equity_value_high"]),
            "defensible_value_low": min(
                float(market_result["market_value_low"]),
                float(equity_result["equity_value_low"]),
            ),
            "defensible_value_point": min(
                float(market_result["market_value_point"]),
                float(equity_result["equity_value_point"]),
            ),
            "defensible_value_high": min(
                float(market_result["market_value_high"]),
                float(equity_result["equity_value_high"]),
            ),
            "confidence_score": confidence_score,
            "model_inputs_json": {
                "generation_mode": GENERATION_MODE,
                "source_view": "parcel_summary_view",
                "source_assumptions": [PROXY_ASSUMPTION],
                "account_number": inputs.account_number,
                "county_id": inputs.county_id,
                "tax_year": inputs.tax_year,
                "neighborhood_code": inputs.neighborhood_code,
                "market_inputs": {
                    "subject_living_area_sf": inputs.living_area_sf,
                    "neighborhood_median_sale_psf_12m": market_inputs.neighborhood_median_sale_psf_12m,
                    "adjusted_comp_value_psf": market_inputs.adjusted_comp_value_psf,
                    "comp_rank_weights": market_inputs.comp_rank_weights,
                },
                "equity_inputs": {
                    "subject_living_area_sf": inputs.living_area_sf,
                    "adjusted_equity_comp_psf": [
                        round(equity_psf * 0.96, 4),
                        round(equity_psf * 0.99, 4),
                        round(equity_psf, 4),
                        round(equity_psf * 1.02, 4),
                    ],
                },
            },
        }

    def _fetch_existing_valuation_run_id(
        self,
        cursor: Any,
        *,
        parcel_id: str,
        tax_year: int,
    ) -> str | None:
        cursor.execute(
            """
            SELECT valuation_run_id
            FROM valuation_runs
            WHERE parcel_id = %s
              AND tax_year = %s
              AND market_model_version = %s
              AND equity_model_version = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (parcel_id, tax_year, MODEL_VERSION, MODEL_VERSION),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return str(row["valuation_run_id"])

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
