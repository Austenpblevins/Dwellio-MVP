# Dwellio Schema Reference (Final Reconciled)

## MVP backbone
`counties -> appraisal_districts -> parcels -> parcel_assessments -> parcel_improvements -> parcel_features -> valuation_runs -> parcel_savings_estimates -> quote_explanations -> protest_recommendations`

Business chain:
`parcels -> leads -> clients -> representation_agreements -> protest_cases -> case_outcomes`

## Synchronization targets
- `parcel_sales`
- `neighborhood_stats`
- `comp_candidates`
- `valuation_runs`
- `parcel_savings_estimates`
- `decision_tree_results`
