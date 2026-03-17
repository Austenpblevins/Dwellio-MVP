class ProtestDecisionTreeService:
    def evaluate(self, *, current_notice_value: float, defensible_value_point: float, valid_comp_count: int, expected_tax_savings_point: float, confidence_score: float) -> list[dict]:
        gap_percent = ((current_notice_value - defensible_value_point) / current_notice_value) if current_notice_value else 0
        return [
            {'rule_code': 'value_gap_detection', 'rule_result': 'pass' if gap_percent >= 0.05 else 'reject'},
            {'rule_code': 'comp_validation', 'rule_result': 'pass' if valid_comp_count >= 3 else 'reject'},
            {'rule_code': 'minimum_savings_threshold', 'rule_result': 'pass' if expected_tax_savings_point >= 300 else 'reject'},
            {'rule_code': 'final_confidence_threshold', 'rule_result': 'pass' if confidence_score > 0.65 else 'manual_review'},
        ]
