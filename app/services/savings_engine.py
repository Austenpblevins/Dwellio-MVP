class SavingsEngineService:
    def run(self, *, current_notice_value: float, defensible_value_low: float, defensible_value_point: float, defensible_value_high: float, effective_tax_rate: float, success_probability: float, contingency_rate: float) -> dict:
        reduction_low = max(0.0, current_notice_value - defensible_value_high)
        reduction_point = max(0.0, current_notice_value - defensible_value_point)
        reduction_high = max(0.0, current_notice_value - defensible_value_low)
        gross_low = reduction_low * effective_tax_rate
        gross_point = reduction_point * effective_tax_rate
        gross_high = reduction_high * effective_tax_rate
        expected_low = gross_low * success_probability
        expected_point = gross_point * success_probability
        expected_high = gross_high * success_probability
        return {
            'projected_reduction_low': reduction_low,
            'projected_reduction_point': reduction_point,
            'projected_reduction_high': reduction_high,
            'gross_tax_savings_low': gross_low,
            'gross_tax_savings_point': gross_point,
            'gross_tax_savings_high': gross_high,
            'expected_tax_savings_low': expected_low,
            'expected_tax_savings_point': expected_point,
            'expected_tax_savings_high': expected_high,
            'estimated_contingency_fee': expected_point * contingency_rate,
        }
