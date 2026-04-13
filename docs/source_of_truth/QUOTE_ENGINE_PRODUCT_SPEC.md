# QUOTE_ENGINE_PRODUCT_SPEC.md

## Purpose
This document defines the quote engine, valuation, savings, and protest-decision product logic for Dwellio.

This is the controlling specification for:
- quote behavior
- valuation logic
- comp selection
- defensible value rules
- savings estimation
- protest recommendation
- confidence scoring
- public quote outputs
- lead-to-case product flow

This document is not the primary authority for backbone schema or ingestion design except where product interfaces depend on those systems.
Those belong in `PLATFORM_IMPLEMENTATION_SPEC.md`.

---

## Product Overview
Dwellio is a Texas property tax protest platform.
Its core product job is to:
1. resolve a homeowner address to a county appraisal parcel
2. estimate a defensible protest value using both market-value and unequal-appraisal logic
3. convert that value into a tax-savings estimate and protest recommendation
4. capture and qualify leads
5. manage signed protest cases through protest workflow stages
6. generate explanation-ready outputs and future evidence support
7. learn from outcomes over time

For MVP, support:
- Harris County
- Fort Bend County
- single-family residential only
- precomputed quote-ready records
- sub-2-second quote response path

---

## Core Product Rule
Dwellio is not a generic consumer AVM.
The key output is the **lowest supportable protest value**.

For each parcel-year:
- compute a market-value model
- compute an unequal-appraisal model
- final defensible value = `min(market_value_model_output, unequal_appraisal_model_output)`

This rule governs quote output and recommendation behavior.

---

## Product Read Path
Public flow:
1. normalize address
2. resolve parcel
3. optionally fetch instant quote support for top-of-funnel savings-range UX
4. fetch latest quote-ready parcel-year row
5. return quote, explanation, and recommendation

Target:
- sub-2-second quote response
- no heavy live comp analysis in request path
- request path should read precomputed or near-precomputed data
- additive instant-quote request paths must remain read-model driven and separate from the refined quote contract

Public route note:
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}` is an additive instant-quote endpoint for public savings-range UX
- it must remain separate from the refined quote contract and must not replace the defensible-value quote response

---

## Required Product Layers
1. subject feature layer
2. market feature layer
3. equity feature layer
4. appeal/protest feature layer
5. dual valuation core
6. savings engine
7. quote explanation layer
8. protest recommendation layer
9. ARB/win-probability support layer
10. lead and case-conversion support layer

---

## Compliance Rules
### MLS / HAR data
If MLS data is used:
- use internally for analysis
- use to support derived metrics or features
- do not expose raw MLS records publicly
- do not expose raw listing history publicly
- do not expose agent remarks publicly

Required pattern:
- restricted raw/restricted internal tables -> derived metrics/features -> model outputs -> public-safe quote output

### County data
- prefer bulk downloads over fragile page scraping
- keep raw files archived
- support semi-manual acquisition where necessary
- maintain import history and checksums

---

## Dual Valuation Core
The valuation engine must support two independent value paths.

### 1. Market-value model
Estimate a supportable market value using subject property features and recent comparable sales.

High-level requirements:
- use filtered residential sales comparables
- require defensible recency and similarity
- persist model inputs and outputs
- return low / point / high range where feasible

### 2. Unequal-appraisal model
Estimate a supportable unequal-appraisal value based on assessed-value comparables and neighborhood equalization logic.

High-level requirements:
- compare subject assessed $/sf to relevant comparable assessed $/sf distribution
- use neighborhood or market-area aware peer logic
- persist comp basis and supporting metrics
- return low / point / high range where feasible

### 3. Defensible value
```text
defensible_value_point = min(market_value_point, equity_value_point)
defensible_value_low   = min(market_value_low, equity_value_low)
defensible_value_high  = min(market_value_high, equity_value_high)
```

---

## Comp Selection Requirement
Support two comp universes:
1. market-value sales comps
2. unequal-appraisal assessed comps

General comp principles:
- filter for same broad property class and residential use
- strongly prefer close geography or same neighborhood/market area
- use recency constraints
- use size/age/condition similarity constraints where possible
- persist scoring and rejection reasons
- keep comp selection deterministic and inspectable

Do not let the request-time API perform full heavy comp analysis.
Compute candidate pools and quote-ready outputs ahead of time when practical.

---

## Product Valuation Logic
Persist and inspect major formula inputs.

Example high-level unequal-appraisal point logic:
```text
equity_value_point =
  median(adjusted_comp_psf of top equity comps) * subject_living_area_sf
```

The exact field names can evolve, but the model must keep:
- subject basis
- comp pool basis
- selected comps
- comp adjustments or normalization basis
- resulting low/point/high value outputs

---

## Protest Decision Tree
Persist every rule result.

Required decision checks:
1. value gap detection
   - compute difference between county notice value and defensible value logic
   - reject or downgrade weak value gaps

2. neighborhood equalization
   - reject or downgrade when subject assessed value is not meaningfully above relevant peer basis

3. comp validation
   - reject or downgrade when valid comps are insufficient

4. minimum savings threshold
   - reject or downgrade when estimated savings is below threshold
   - initial threshold can be configurable

5. homestead cap / assessed limitation check
   - if capped value already suppresses practical tax impact, reject or downgrade

6. condition / functional issues
   - allow confidence uplift when adverse condition flags exist and are supportable

7. final confidence threshold
   - recommend protest only when confidence exceeds configured threshold

The product must persist:
- each rule outcome
- final recommendation basis
- explanation-friendly rule summaries

---

## Savings Engine
The savings engine must translate defensible value into projected savings.

High-level structure:
```text
projected_reduction_point =
  max(0, current_notice_value - defensible_value_point)

gross_tax_savings_point =
  projected_reduction_point * effective_tax_rate
```

For MVP, success probability may start as rule-based.
Example tiering:
- high-confidence / strong reduction -> higher probability
- medium-high -> moderate-high probability
- moderate -> moderate probability
- weak -> low probability

Then:
```text
expected_tax_savings_point =
  gross_tax_savings_point * success_probability

estimated_contingency_fee =
  expected_tax_savings_point * contingency_rate
```

Return at minimum:
- low savings
- point savings
- high savings
- confidence
- recommendation
- explanation bullets
- fee estimate

---

## ARB / Win Probability Layer
After the MVP rule-based layer, support a second model for win probability.

Inputs may include:
- neighborhood historical reduction rate
- comp strength score
- hearing type
- county
- value tier
- prior outcomes
- panel/assessor variance proxies if available

Outputs:
- probability_of_reduction
- expected_reduction_amount
- expected_tax_savings

This can be scaffolded for MVP and expanded later.

---

## Public API Contract
### Public APIs
- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

### Internal APIs
- case CRUD
- hearing scheduling
- packet generation trigger
- model run trigger
- ETL job status
- evidence upload
- outcome closeout

### Quote response fields
At minimum return:
- `county_id`
- `tax_year`
- `account_number`
- `address`
- `current_notice_value`
- `defensible_value_point`
- `expected_tax_savings_point`
- `expected_tax_savings_low`
- `expected_tax_savings_high`
- `confidence`
- `basis`
- `protest_recommendation`

---

## Product-Side Schema Domains
The product layer should support these groups of tables or equivalent models:

### Reference / admin
- states
- counties
- appraisal_districts
- tax_years
- county_configs
- property_type_codes
- class_code_mappings
- exemption_types
- hearing_types
- workflow_statuses

### Product raw / staging support
- source_systems
- import_batches
- raw_files
- ingest_errors
- county_field_mappings
- job_runs
- staging support tables where needed

### Parcel / property master interfaces
- parcels
- parcel_aliases
- parcel_addresses
- parcel_geometries
- parcel_ownership_history
- parcel_improvements
- parcel_lands
- parcel_assessments
- parcel_exemptions
- parcel_taxing_units

### Market / valuation support
- parcel_sales
- neighborhood_stats
- market_areas
- parcel_market_areas
- time_adjustment_factors
- adjustment_factors
- condition_premiums
- comp_candidate_pools
- comp_candidates
- comp_adjustments
- parcel_features
- market_features
- equity_features
- appeal_features

### Quote / recommendation support
- valuation_runs
- valuation_run_inputs
- quote_runs
- parcel_savings_estimates
- quote_explanations
- protest_recommendations
- lead_scores
- decision_tree_results
- arb_win_probability_runs

### Lead / client / protest workflow
- leads
- lead_events
- clients
- client_parcels
- representation_agreements
- protest_cases
- case_assignments
- case_notes
- hearings
- hearing_participants
- case_outcomes
- case_status_history

Exact naming can be aligned to the platform spec as long as the behavior and coverage remain intact.

---

## Product Acceptance Criteria
The first product milestone is complete when:
- a parcel-year can be quoted from precomputed data
- quote output includes defensible value, savings range, confidence, and recommendation
- explanation output is human-readable and tied to stored rule/model results
- lead submission creates a lead tied to the quote or parcel-year
- signed representation flow can create a protest case
- case outcomes can be stored and linked back to valuation runs or parcel-year records
- restricted source data is segregated from public output
- the product can expand to additional counties without redesigning the quote engine interfaces

---

## Product Execution Order
Implement product logic in this order:
1. quote-ready read model contract
2. subject/market/equity feature contracts
3. comp candidate generation
4. market-value model
5. unequal-appraisal model
6. defensible value logic
7. savings engine
8. decision tree persistence
9. recommendation logic
10. explanation builder
11. quote endpoints
12. lead capture linkage
13. protest case creation linkage
14. ARB probability scaffolding
15. packet/evidence scaffolding

---

## Final Product Instruction
Use this file as the source of truth for quote and recommendation behavior.
Do not replace canonical platform rigor with quick product shortcuts.
The quote engine must sit on precomputed, traceable parcel-year data and produce inspectable outputs suitable for future evidence and operations use.
