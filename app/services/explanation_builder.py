class ExplanationBuilderService:
    def build(
        self,
        *,
        market_value_point: float,
        equity_value_point: float,
        defensible_value_point: float,
        confidence_label: str,
        recommendation_code: str,
        support_scope: str = "unknown",
        segment_peer_count: int = 0,
        neighborhood_peer_count: int = 0,
        value_gap_percent: float = 0.0,
        generation_mode: str | None = None,
    ) -> dict:
        if (
            defensible_value_point > 0
            and abs(market_value_point - equity_value_point) / defensible_value_point <= 0.03
        ):
            basis = 'market_and_equity'
        else:
            basis = 'market' if market_value_point <= equity_value_point else 'equity'
        peer_bullet = (
            f"The benchmark used {segment_peer_count} segment peers inside a "
            f"{neighborhood_peer_count}-parcel neighborhood support set."
            if support_scope == "segment_within_neighborhood"
            else f"The benchmark relied on a {neighborhood_peer_count}-parcel neighborhood support set."
        )
        return {
            'basis': basis,
            'confidence_label': confidence_label,
            'explanation_bullets': [
                'The county notice appears above the lowest supportable protest value.'
                if value_gap_percent >= 0.05
                else 'The benchmark found only a limited value gap versus the county notice.',
                peer_bullet,
                f'The current recommendation is {recommendation_code}.',
            ],
            'explanation_json': {
                'market_value_point': market_value_point,
                'equity_value_point': equity_value_point,
                'defensible_value_point': defensible_value_point,
                'support_scope': support_scope,
                'segment_peer_count': segment_peer_count,
                'neighborhood_peer_count': neighborhood_peer_count,
                'value_gap_percent': round(value_gap_percent, 6),
                'generation_mode': generation_mode,
            },
        }
