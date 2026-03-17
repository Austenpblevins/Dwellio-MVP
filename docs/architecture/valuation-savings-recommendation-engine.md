# Valuation, Savings, and Recommendation Engine (Final Reconciled)

Market model:
- neighborhood baseline
- weighted comp estimate
- final market value point

Equity model:
- median adjusted comp $/sf of top 15 equity comps

Defensible value:
- `defensible_value_point = min(market_value_point, equity_value_point)`

Persist:
- `valuation_runs`
- `parcel_savings_estimates`
- `decision_tree_results`
- `quote_explanations`
- `protest_recommendations`
