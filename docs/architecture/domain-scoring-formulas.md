# Domain Scoring Formulas (Final Reconciled)

Market comp penalty:
0.30 * abs(sf_diff_pct)
+ 0.15 * min(distance_miles / 5, 1)
+ 0.15 * min(age_diff / 20, 1)
+ 0.10 * abs(bed_diff)
+ 0.10 * abs(bath_diff)
+ 0.10 * abs(lot_diff_pct)
+ 0.05 * story_penalty
+ 0.05 * quality_penalty
