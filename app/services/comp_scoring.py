def market_comp_similarity_penalty(sf_diff_pct: float, distance_miles: float, age_diff: float, bed_diff: float, bath_diff: float, lot_diff_pct: float, story_penalty: float, quality_penalty: float) -> float:
    return (
        0.30 * abs(sf_diff_pct)
        + 0.15 * min(distance_miles / 5.0, 1.0)
        + 0.15 * min(age_diff / 20.0, 1.0)
        + 0.10 * abs(bed_diff)
        + 0.10 * abs(bath_diff)
        + 0.10 * abs(lot_diff_pct)
        + 0.05 * story_penalty
        + 0.05 * quality_penalty
    )

def rank_score_from_penalty(penalty: float) -> float:
    return 1.0 / (0.1 + penalty)
