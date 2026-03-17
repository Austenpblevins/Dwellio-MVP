class ExplanationBuilderService:
    def build(self, *, market_value_point: float, equity_value_point: float, defensible_value_point: float, confidence_label: str, recommendation_code: str) -> dict:
        basis = 'market' if market_value_point <= equity_value_point else 'equity'
        return {
            'basis': basis,
            'confidence_label': confidence_label,
            'explanation_bullets': [
                'The county notice appears above the lowest supportable protest value.',
                'Comparable properties and neighborhood benchmarks suggest a lower supportable value range.',
                f'The current recommendation is {recommendation_code}.',
            ],
            'explanation_json': {
                'market_value_point': market_value_point,
                'equity_value_point': equity_value_point,
                'defensible_value_point': defensible_value_point,
            },
        }
