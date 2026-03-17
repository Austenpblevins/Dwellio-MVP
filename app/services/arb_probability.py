class ArbProbabilityService:
    def run(self, neighborhood_historical_reduction_rate: float, comp_strength_score: float, panel_variance_factor: float) -> dict:
        probability = max(0.0, min(1.0, 0.40 + (0.30 * neighborhood_historical_reduction_rate) + (0.20 * comp_strength_score) - (0.10 * panel_variance_factor)))
        return {'probability_of_reduction': probability, 'expected_reduction_amount': 0.0, 'expected_tax_savings': 0.0}
