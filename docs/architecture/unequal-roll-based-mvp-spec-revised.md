
# DWELLIO_UNEQUAL_ROLL_BASED_MVP_PRODUCTION_SPEC.md

## Dwellio — Unequal Appraisal MVP (Roll-Based Only)
### Production-Ready Architecture Spec + Codex Implementation Prompt
### Status: MVP Implementation Spec
### Intended placement:
- `docs/architecture/unequal-roll-based-mvp-spec.md`
- cross-reference from `docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md`
- cross-reference from `docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md`
- cross-reference from `docs/architecture/comp-engine.md`
- cross-reference from `docs/architecture/protest-packet-engine.md`
- cross-reference from `docs/architecture/api-contracts.md`

---

## 0. Document Purpose

This document defines the **MVP unequal-appraisal valuation engine** for Dwellio’s 2026 Texas protest workflow.

This is **not** the public instant quote model.  
This is **not** the current internal/public quote benchmark path built around `valuation_runs` and instant-quote-style cohort artifacts.  
This is **not** the broader v2 unequal-appraisal framework with roll and ratio lanes.  
This is the **leanest production-ready model** Dwellio can deploy today while still generating business-ready, defendable, roll-based unequal-appraisal protest packages.

The MVP is intentionally narrow:

- **single-family residential only**
- **Texas only**
- **roll-based unequal appraisal only**
- **sole unequal-appraisal protest posture**
- **no subject market-value evidence in the valuation lane**
- **no market comps**
- **no MLS dependency**
- **no ratio method in MVP**
- **no consumer-facing output**

The MVP is designed to capitalize on the 2026 legal posture for protests filed solely on unequal appraisal under Tax Code §41.43(b)(3), while minimizing dependency on public bulk sales data and minimizing implementation complexity.

This specification is implementation-grade and Codex-ready. It assumes Dwellio’s current architectural patterns:

- source-of-truth-first documentation
- deterministic pipelines
- versioned valuation runs
- strong admin/public separation
- structured evidence persistence
- packet-safe outputs
- explicit readiness and support states
- full override audit trails

### Current repo alignment rule

This MVP should be implemented as a **separate internal unequal-roll engine** that reuses Dwellio’s current canonical parcel stack and persistence patterns without forcing a redesign of public quote services.

For MVP:

- reuse canonical parcel-year data already prepared in the repo
- reuse run/version/audit patterns where they fit
- keep this engine separate from `InstantQuoteService` and the public `/quote/instant` contract
- do not require `parcel_features`, `market_features`, or `equity_features` as launch dependencies unless they are actually populated and in active use

---

## 1. Canonical Authority Order

Before implementing anything in this specification, Codex must read and obey the following files in this order:

1. `docs/source_of_truth/CANONICAL_CONTEXT.md`
2. `docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md`
3. `docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md`
4. `docs/source_of_truth/DWELLIO_BUILD_PLAN.md`
5. `docs/source_of_truth/AGENT_RULES.md`
6. `docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md`
7. `docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md`

Also review and align to:

- `docs/runbooks/CANONICAL_PRECEDENCE.md`
- `docs/runbooks/ARCHITECTURE_STATE.md`
- `docs/runbooks/ARCHITECTURE_MAP.md`
- `docs/architecture/implementation-spec.md`
- `docs/architecture/api-contracts.md`
- `docs/architecture/comp-engine.md`
- `docs/architecture/valuation-savings-recommendation-engine.md`
- `docs/architecture/testing-observability-security.md`
- `docs/architecture/schema-reference.md`

### Canonical implementation rule

If this spec conflicts with a higher-authority source-of-truth document, the higher-authority document wins.  
If existing repo contracts, schema, or valuation-run patterns are more mature than this spec, preserve canonical repo behavior unless this spec explicitly defines a safe extension.

### MVP integration rule

This spec should prefer **repo-fit extensions** over greenfield architecture.

That means:

- prefer current canonical tables and read models over inventing a new countywide feature store
- prefer run-scoped immutable snapshots over broad precomputed feature infrastructure when the latter is not yet operational
- prefer separate unequal-roll persistence over forcing semantic reuse of public-quote tables when the business meaning diverges

---

## 2. Why This MVP Exists

This MVP exists because Dwellio does **not** need the full unequal-appraisal surface area to launch a defendable 2026 protest product.

The MVP should exploit four practical realities:

1. The 2026 Texas legal posture makes the **roll-based unequal lane unusually attractive** for protests filed solely on unequal appraisal.
2. Texas remains a difficult environment for broad public sale-price coverage, especially at scale.
3. County public data quality varies, so the MVP should favor methods that can be built from appraisal-roll data and property-card details.
4. Dwellio’s near-term business need is not a fully generalized valuation research platform; it is a **production-ready protest engine** that reliably creates defendable packets for straightforward SFR cases.

### What the MVP is optimizing for

- fastest path to a defendable 2026 product
- minimal dependency on external market data
- strong auditability
- low legal ambiguity
- bounded implementation scope
- strong admin reviewability
- county-by-county rollout
- easy future expansion into ratio/hybrid methods later

### What the MVP is intentionally not optimizing for

- every county on day one
- every property type
- maximum valuation sophistication
- fully generalized hedonic modeling
- court-first engineering
- public quote UX
- market value support

---

## 3. Legal and Procedural Alignment

The MVP must align to the **roll-based unequal-appraisal path** under Texas law and 2026 procedures.

### Operating legal theory

The MVP’s valuation conclusion is based on the proposition that the subject property’s appraised value exceeds the **median appraised value of a reasonable number of comparable properties appropriately adjusted**.

### Core procedural posture

The MVP is designed for cases where the protest is filed **solely on unequal appraisal**.  
Under that posture, the packet and valuation lane must not rely on the subject property’s market value evidence.

### Consequences for the MVP design

Because of that posture, the MVP must:

- treat the **roll-based lane** as the only valuation lane
- avoid market comps entirely in the valuation engine
- avoid subject AVM market estimates in the protest packet
- avoid purchase price, MLS, Zillow, Redfin, and similar market-value content in the packet narrative
- derive value strictly from comparable properties on the roll, appropriately adjusted
- preserve documented comparability and adjustment logic because ARBs are trained to focus on reasonableness of sample, comparability, and appropriateness of adjustments

### Important legal posture rule

The MVP is **not** a market-value model disguised as unequal appraisal.  
It must remain cleanly roll-based from end to end.

---

## 4. Product Positioning

The Unequal Appraisal MVP is Dwellio’s **final protest-stage roll-based unequal valuation engine**.

### It is intended for

- internal/admin use
- final protest preparation
- packet evidence generation
- repeatable county-specific automation
- analyst QA and override
- lean production launch for 2026 SFR cases

### It is not intended for

- public instant quotes
- consumer-facing home-value estimates
- broad AVM marketing output
- ratio-lane unequal appraisal
- hybrid market-and-equity conclusions
- unsupported counties or sparse/rural edge cases without review

### Core philosophy

This MVP should answer:

> Based solely on tax-roll evidence and appropriately adjusted comparable properties, what is the most defensible roll-based unequal-appraisal value conclusion for this subject?

---

## 5. Production-Ready Design Principles

The MVP must be narrow, but it may **not** be flimsy.

### Principles

1. **One lane only**
   - Only the roll-based method is implemented.
   - No hidden ratio logic.
   - No market-comp backdoor.

2. **Median of adjusted appraised values**
   - The final requested value is the median of adjusted comparable appraised values.
   - Similarity scores select comps; they do not replace the statutory median.

3. **Lean feature set, not weak feature set**
   - Fewer methods.
   - Fewer data dependencies.
   - Stronger governance around the one method that remains.

4. **County- and data-aware**
   - The MVP must refuse to auto-support county-years that do not expose enough reliable roll fields.

5. **Selection and adjustment must be separable**
   - Filtering and ranking determine which comps are credible.
   - Adjustment logic determines how each selected comp is normalized to the subject.
   - The final median is computed from adjusted values, not from raw scores.

6. **Auditability over cleverness**
   - Every comp must have an inclusion path, an exclusion path, and a visible adjustment path.

7. **Packet fidelity**
   - The comps shown in the packet must be the finalized selected comps.
   - The adjustment lines shown in the packet must match the persisted adjustment lines.

8. **High confidence should be hard to earn**
   - Sparse data, heavy fallback, high adjustment burden, and many overrides must lower support.

9. **No silent market contamination**
   - No market value conclusion, market comps, or subject market value narrative may leak into the sole unequal packet.

10. **Build for extension**
    - The MVP remains roll-based only, but its entities should not prevent later addition of ratio lanes.

---

## 6. Non-Negotiable Design Constraints

1. Do not use the subject property’s market value evidence in the MVP valuation lane.
2. Do not use MLS sales, AVM sale estimates, purchase price, or public portal estimates in the packet for sole unequal protests.
3. Do not use taxable value as the comparison field for unequal valuation.
4. Do not reduce the model to a neighborhood median without comp filtering and comp adjustments.
5. Do not compute the final requested value from a weighted median of raw peer $/SF.
6. Do not treat similarity score as value.
7. Do not permit silent analyst overrides.
8. Do not generate a packet from an unfinalized or non-audited run.
9. Do not treat 5 comps as automatically production-safe.
10. Do not allow unsupported county-years to pass as auto-supported.
11. Do not double-count the same feature through multiple adjustment channels.
12. Do not let packet evidence cite comps excluded from the final set.

---

## 7. Scope and Operating Envelope

### In-scope for MVP

- Texas
- tax years beginning with 2026 protest season use
- single-family detached residential
- standard suburban and tract-style neighborhood cases
- owner-occupied and non-owner-occupied SFR where roll comparability is still meaningful
- county-years where public or licensed roll/property-card data is sufficient
- straightforward cases where comparability can be established with a reasonable number of peers

### Out of scope for MVP

- condos
- townhomes unless explicitly modeled later
- manufactured housing
- luxury custom homes with sparse neighbors
- acreage properties with major land-utility differences
- waterfront, golf-course, or other premium-location segments unless county config explicitly supports them
- income-producing commercial property
- ratio method
- representative-sample ratio method
- sales-based market value method
- public quote use

### Degraded or manual-only cases

The MVP may allow manual-review-only handling for:

- small neighborhoods with only 6–7 very strong peers
- incomplete county feature coverage
- adjacent-subdivision fallback cases
- properties with one material data defect that can be corrected from evidence

Unsupported status must be explicit and persisted.

---

## 8. Canonical Value Field Ontology for MVP

Even though the MVP uses only the roll-based lane, it must still keep value fields straight.

### Required canonical fields

- `district_market_value`
- `appraised_value`
- `taxable_value`
- `land_value`
- `improvement_value`
- `homestead_flag`
- `homestead_cap_flag`
- `circuit_breaker_flag`
- `exemption_total`
- `comparison_appraised_value_roll_method`

### Required comparison rule

For the roll-based MVP, the comparison field is:

- **subject appraised value**
- **comparable property appraised values**
- **appropriately adjusted comparable appraised values**

### Fields that do not drive the MVP valuation conclusion

- subject market value estimate
- subject purchase price
- subject taxable value
- comparable taxable values
- market comps
- AVM values

### Exemptions and caps

- Exemptions affect taxation, not comparability.
- Taxable value is not the primary unequal-appraisal comparison field.
- Homestead and circuit-breaker flags must still be stored because they matter for internal tax-impact estimates, data interpretation, and future expansion.
- Packet-safe MVP logic must not confuse these fields.

### Internal-only savings rule

If Dwellio computes estimated tax impact, it must be derived from taxable-value logic in a separate internal-only step.  
That tax-impact step must not change the valuation conclusion or pollute the packet narrative.

---

## 9. County Readiness and Data Availability Model

The MVP must be realistic about Texas public-data constraints and about the data contracts that Dwellio already has in the repo today.

### County readiness tiers

#### Tier 1 — Fully supported
County-year has enough structured or reliably normalized canonical data to automate:

- parcel identity
- address
- appraised value
- land value
- improvement value
- living area
- year built or effective age proxy
- property class/use code
- neighborhood / market area / subdivision indicator
- bedrooms
- enough bath support for defendable filtering and adjustment
- story count or story proxy
- pool indicator
- lot size
- quality/class and condition, where exposed
- source lineage sufficient for internal audit and packet support

#### Tier 2 — Supported with review
Most core fields are available, but some adjustment fields require county-specific fallback, additive valuation-only features, or analyst review.

#### Tier 3 — Manual review only
The county does not expose enough structured data for an automated run, but analysts can assemble a case with bounded supplementation and explicit support downgrade.

#### Tier 4 — Unsupported
County-year lacks enough reliable canonical or licensed roll data for a defendable automated or semi-automated run.

### County contract examples in the current repo

#### Harris
Current upstream/canonical support is strong for:

- `bedrooms`
- `full_baths`
- `half_baths`
- `total_rooms`
- `stories`
- `pool_flag`

These may be used directly in filtering, scoring, and governed adjustment logic.

#### Fort Bend
Current upstream/canonical support is intentionally narrower:

- `bedrooms`
- `half_baths`
- `stories`
- `pool_flag`

Fort Bend also has an additive valuation-only bathroom layer in:

- `fort_bend_valuation_bathroom_features`

That additive layer may support unequal-roll filtering, scoring, and governed adjustment logic when the spec explicitly allows it.

### Important Fort Bend governance rule

Fort Bend canonical `full_baths` remains unsupported for MVP canonical truth.  
Fort Bend `full_baths_derived` and related additive bathroom fields may be used only as valuation-only features with explicit status/confidence gating.

### MVP launch rule

Auto-supported launch counties should be limited to Tier 1 and strong Tier 2 counties.  
Tier 3 is allowed only through explicit analyst workflow.  
Tier 4 must be blocked.

---

## 10. Data Acquisition Strategy for a Lean MVP

The MVP should be designed around **canonical DB-first sourcing**, not around request-time county scraping or a new countywide feature-store buildout.

### Default sourcing rule

For MVP, the unequal-roll engine should source subject and candidate data primarily from Dwellio’s current canonical database objects, including:

- `parcel_summary_view`
- `parcel_improvements`
- `parcel_assessments`
- `parcel_lands`
- `parcels`
- `parcel_year_snapshots`
- `fort_bend_valuation_bathroom_features` for additive Fort Bend valuation-only bath support

### Why this is the right MVP posture

Because the repo already has substantial upstream county normalization work, the MVP should:

- avoid request-time raw county parsing
- avoid request-time property-card scraping as the primary engine path
- avoid making countywide `parcel_features` population a launch prerequisite
- favor immutable run-scoped snapshots built from existing canonical parcel-year records
- preserve lineage by carrying source/version metadata from upstream normalized data and run-time snapshot timestamps

### Candidate discovery order

1. subject parcel snapshot from canonical parcel-year data
2. same neighborhood / subdivision candidate discovery from canonical parcel-year data
3. bounded same-market-area or configured fallback discovery only if needed
4. manual-review-only fallback when support is still thin
5. persist the final run-scoped snapshot, candidate set, and evidence lineage

### Required source provenance fields

For every subject and candidate record used in the final run, persist:

- canonical source family or route identifier
- source tax year
- run snapshot timestamp
- parser / normalization version where available
- raw artifact hash or source file/version reference where available
- normalized extraction / materialization version where available
- source completeness status

---

## 11. High-Level System Concept

The MVP should be built as a deterministic multi-stage pipeline:

1. county-year readiness gate
2. subject snapshot acquisition and normalization
3. candidate universe discovery
4. hard eligibility filtering
5. similarity scoring and ranking
6. outlier and conflict filtering
7. final comp selection
8. comp-level roll-based adjustments
9. adjusted-value median calculation
10. sensitivity and stability checks
11. support/confidence scoring
12. packet-ready output generation
13. admin review and override
14. finalization for packet use

### Important conceptual split

The MVP has **three different math layers**:

- **filtering layer** — determines who may be considered
- **ranking layer** — determines who is most comparable
- **valuation layer** — determines the requested value using the median of adjusted appraised values

Those layers must remain distinct.

### Current repo runtime substrate

The unequal-roll MVP should sit on top of the current canonical parcel stack, not on top of the public instant-quote serving contract.

Important implications:

- `instant_quote_subject_cache` may be useful for diagnostics or readiness heuristics, but it is not the primary unequal-roll feature source because it omits many characteristics this engine needs
- `parcel_features` exists in schema but is not currently populated or operational enough to be an MVP dependency
- immutable run-scoped snapshots are sufficient for MVP auditability and reproducibility

---

## 12. Subject Snapshot Requirements

The subject snapshot must be immutable per run and include all fields needed for selection, adjustment, packet evidence, and later audit.

### Required fields

- parcel/account identifier
- county identifier
- tax year
- situs address
- legal description where available
- neighborhood code / market area / subdivision
- property type / use code
- appraised value
- taxable value
- district market value
- land value
- improvement value
- living area SF
- year built
- effective age if available
- lot size
- bedroom count
- full bath count
- half bath count
- story count
- garage type / garage spaces / carport indicator
- pool flag
- quality / class grade
- condition rating
- remodel flags if visible
- homestead flag
- cap flag(s)
- exemptions summary
- source artifacts and timestamps

### Normalized derived fields

The subject snapshot should also compute:

- `subject_appraised_psf = appraised_value / living_area_sf`
- `effective_baths = full_baths + 0.5 * half_baths`
- `land_share = land_value / appraised_value`
- `improvement_psf = improvement_value / living_area_sf`
- `age_band`
- `lot_band`
- `quality_condition_band`
- `story_layout_band`

### Data defect handling

If a critical subject field is missing, inconsistent, or obviously wrong:

- the run should not auto-finalize
- the defect must be persisted
- the subject should enter analyst review or unsupported status

---

## 13. Candidate Universe Assembly

The candidate universe must start broad enough to avoid cherry-picking but narrow enough to remain local and defendable.

### Candidate discovery sequence

#### Stage A — same neighborhood / same subdivision
Default first-pass discovery should attempt to collect candidates from:

- same CAD neighborhood code, or
- same subdivision, or
- same county-defined residential market area

#### Stage B — same neighborhood plus tightly bounded nearby fallbacks
If Stage A is insufficient, expand to:

- competing subdivision explicitly configured as comparable, or
- nearby properties within a bounded radius and same broad market area

#### Stage C — manual-review-only fallback
If the universe remains insufficient, analyst workflow may assemble adjacent-subdivision comps, but the run must downgrade support.

### Candidate pool targets

- preferred raw candidate pool before scoring: **25–75**
- acceptable raw candidate pool before scoring: **20–90**
- maximum auto-harvest without manual expansion: **100**
- if fewer than **15** plausible candidates exist after geography discovery, trigger early sparse-data warning

### Why these targets

This is enough breadth to defend against obvious cherry-picking arguments, while still lean enough for neighborhood-bound public-data retrieval.

---

## 14. Hard Eligibility Filters

The MVP should rely on **config-driven tiers** of hard filters.

### 14.1 Preferred filter tier (Tier P)

Use this first.

| Variable | Preferred filter |
|---|---|
| Property type / use code | same SFR type only |
| Neighborhood / subdivision | same neighborhood or same subdivision |
| Living area | ±12% default, configurable to ±15% |
| Year built / effective age | ±8 years default, configurable to ±10 |
| Lot size | ±20% when land utility is standard |
| Quality / class | same preferred; adjacent only if adjustment path exists |
| Condition | same preferred; adjacent only if adjustment path exists |
| Bedrooms | within ±1 |
| Effective baths | within ±1.0 |
| Half baths | within ±1 |
| Stories | same preferred; ±1 story only if supported |
| Garage / car storage | same or functionally similar preferred |
| Pool | same preferred; mismatch allowed only if adjustment path exists |

### 14.2 Controlled fallback tier (Tier F1)

Use only if Tier P produces too few candidates.

| Variable | Fallback F1 |
|---|---|
| Living area | ±15% |
| Year built / effective age | ±10 years |
| Lot size | ±25% if land value impact is minor |
| Quality / class | same or adjacent |
| Condition | same or adjacent |
| Bedrooms | within ±1 |
| Effective baths | within ±1.5 |
| Stories | same or ±1 |
| Garage | mismatch allowed with adjustment |
| Pool | mismatch allowed with adjustment |

### 14.3 Extended fallback tier (Tier F2)

Use only if F1 still fails and only in same market area or manual-review-eligible geography.

| Variable | Fallback F2 |
|---|---|
| Living area | ±20% |
| Year built / effective age | ±15 years |
| Lot size | ±35% if land share is low and lot utility is standard |
| Quality / class | adjacent only, never distant |
| Condition | adjacent only, never distant |
| Bedrooms | within ±2, but score penalty required |
| Effective baths | within ±2.0, but score penalty required |
| Stories | same or ±1 with explicit penalty |
| Garage / pool | mismatch allowed only with defendable adjustment |
| Neighborhood | same market area / competing subdivision only |

### Hard rejection conditions

Reject automatically if:

- different property type
- different residential form with materially different utility
- extreme site premium mismatch
- acreage or irregular lot utility when subject is standard suburban
- luxury/custom segment mixed into tract segment
- missing critical living area or value field
- non-adjacent quality or condition class
- feature profile too different to normalize credibly
- no defendable adjustment path for a material mismatch

---

## 15. Similarity Scoring Framework

The MVP needs similarity scoring, but only for ranking and selection.

### Scoring objective

The score should answer:

> How close is this candidate to the subject before line-item adjustment?

### Similarity score range

- normalized 0–100
- deterministic
- configuration-backed
- interpretable

### Methodology guardrail for scoring

Similarity scoring may be used to:

- rank candidate comparable properties
- select the most similar and defensible final comp set
- support QA and confidence scoring
- explain why specific comps were included or excluded

Similarity scoring must **not** directly determine the final requested value.

Scoring weights are:

- selection / ranking weights only
- not valuation weights
- not percentages of appraised value contribution

Regression coefficients or matched-pair coefficients must not be blindly substituted for similarity weights.

### Recommended score weights

| Factor | Weight |
|---|---:|
| Same neighborhood / subdivision / market area | 25 |
| Quality / class / condition alignment | 20 |
| Living area similarity | 18 |
| Year built / effective age similarity | 12 |
| Beds / baths utility similarity | 10 |
| Lot size / land influence | 5 |
| Story count / layout utility | 5 |
| Major features (garage, pool, major amenity profile) | 5 |

### Why this scoring is better than the draft

- keeps neighborhood/locality as the most important ranking factor
- elevates quality and condition because ARBs care about comparability
- keeps living area strong but not dominant
- makes beds/baths first-class rather than implicit
- keeps lot size and amenities material but not overpowering
- avoids using score as a disguised value estimate

### Example component logic

`neighborhood_component`
- 1.00 = same neighborhood or subdivision
- 0.85 = configured competing subdivision in same market area
- 0.60 = fallback nearby same market area
- 0.00 = outside allowed geography

`gla_component`
- `max(0, 1 - abs(gla_diff_pct) / gla_max_pct)`

`age_component`
- `max(0, 1 - abs(age_diff) / age_max_years)`

`beds_baths_component`
- weighted blend of bedroom, full-bath, half-bath, and effective-bath proximity

### Important bedroom / room-count rule

`bedrooms` is a first-class channel for both Harris and Fort Bend.

Harris `total_rooms` is a separate secondary utility/layout field. It may strengthen comparability analysis where available, but it must never be treated as a synonym for `bedrooms` and must never silently replace bedroom count in filters, scoring, or adjustments.

`quality_condition_component`
- 1.00 exact / 0.65 adjacent / 0.00 otherwise

### Important rule

A high score does **not** mean a comp is automatically selected.  
It must still survive adjustment-governance and outlier checks.

---

## 16. Sample Governance and Anti-Cherry-Picking Controls

ARBs are trained to look at whether the number of properties is reasonable, whether the properties are comparable, and whether the values were appropriately adjusted. The MVP must therefore formalize sample governance.

### Final comp count rules

- **preferred final comp set:** 12–18
- **acceptable production target:** 10–20
- **auto-supported minimum:** 8
- **manual-review exception:** 6–7 only if all of the following are true:
  - same neighborhood or same subdivision
  - low adjustment burden
  - low dispersion
  - no material feature mismatches
  - analyst approves
- **below 6:** unsupported

### Diversity / concentration rules

The final set should not be a random grab of nearby homes. It should be representative of the subject’s local comparable universe.

Apply these controls:

- at least 70% of final selected comps should come from the same neighborhood/subdivision when such supply exists
- do not over-concentrate on a single micro-street if broader same-neighborhood supply exists
- prefer a spread across the same local cohort rather than serial duplicates
- if using fallback geography, persist exactly why the same-neighborhood supply was insufficient

### Selection log requirement

Persist a formal `selection_log` that includes:

- candidate pool size by geography tier
- filters applied
- reasons candidates failed
- score rank
- why each selected comp survived
- why each near-miss comp was excluded

This is required for both packet defense and internal QA.

---

## 17. Adjustment Philosophy and Source Precedence

The MVP should use the **minimum number of adjustment channels necessary**, but each adjustment channel must be production-safe.

### General adjustment principle

For each selected comp `i`, calculate:

`adjusted_appraised_value_i = raw_appraised_value_i + sum(material feature adjustments)`

### Source precedence for adjustment rates

Use the following precedence order:

1. **canonical CAD roll fields and direct CAD contributory component values**
   - preferred where canonical public or licensed county data exposes land value, extra-feature value, pool value, garage value, etc.

2. **county-published cost / class / depreciation schedules**
   - preferred for age, class, condition, and improvement normalization where available

3. **local roll-only matched-pair or regression-derived calibration**
   - use only if there is enough internal evidence in the candidate universe, county cache, or approved county calibration set
   - regression is internal-only support for schedule calibration, not the final valuation method

4. **county-configured fallback schedules**
   - explicit config values maintained by Dwellio
   - must be versioned
   - must be conservative

5. **exclude instead of guessing**
   - if no defendable adjustment path exists, exclude the comp

### Adjustment governance rules

- no comp should survive on the strength of speculative adjustments
- a comp requiring too many material adjustments should usually be dropped
- if the county does not expose enough information to support the required adjustment source, downgrade support or require review

### Anti-double-counting rules

- if `land_value_delta` is used, do not also apply a separate lot-size dollar adjustment unless explicitly configured
- if a county exposes direct pool value, do not also apply generic amenity adjustment for the same pool
- bedroom adjustments must not duplicate utility already captured by GLA and bath adjustments
- class and condition adjustments must not both independently apply the same effective depreciation concept if the county schedule already bundles them
- if Fort Bend `bathroom_equivalent_derived` is used as the bathroom adjustment channel, do not also separately adjust full-bath and half-bath in the same comp pair unless county config explicitly defines a safe decomposition

---

## 18. Material Feature Framework

The MVP must explicitly define which features are:

- required in filtering
- required in scoring
- required in adjustments
- optional
- exclusion-only

### 18.1 Required in all valuation runs

#### Living area
- role: filter, score, adjustment
- importance: highest among physical attributes
- default: always adjusted if still materially different after selection

#### Year built / effective age
- role: filter, score, adjustment
- importance: high
- default: adjust when rate source exists; otherwise rely on tighter filters

#### Lot size
- role: filter, score, adjustment when material
- importance: high
- default: lot size is a distinct channel from land utility and land value

#### Bedrooms
- role: filter, score, reasonableness check
- importance: high but secondary to GLA
- default: bedroom mismatch over ±1 heavily penalized or excluded
- monetized adjustment: optional and controlled

#### Full baths
- role: filter, score, adjustment
- importance: high
- default: monetized when materially different

#### Half baths
- role: filter, score, adjustment
- importance: moderate
- default: monetized at lower rate than full bath

#### Quality / class
- role: filter, score, adjustment or exclusion
- importance: very high
- default: exact preferred; adjacent only with defendable adjustment

#### Condition
- role: filter, score, adjustment or exclusion
- importance: very high
- default: exact preferred; adjacent only with defendable adjustment

### 18.2 Required in most runs

#### Land utility / land value reasonableness
- role: exclusion, review, or monetary support layer
- default: use to decide whether a lot-size difference is adjustable or instead should trigger exclusion

#### Stories
- role: filter, score, adjustment when material
- default: same preferred, ±1 allowed with penalty and possible adjustment

#### Garage / car storage
- role: filter, score, adjustment if material
- default: separate adjustment or exclusion if materially different

#### Pool
- role: filter, score, adjustment if material
- default: direct feature-value delta preferred; otherwise exclude if too material

### 18.3 Usually filter-only or review-only in MVP

#### Layout functionality
- role: reasonableness / manual review
- examples: odd floor plans, converted garages, oversized bonus rooms
- default: not automatically monetized in MVP

#### Remodel / renovation
- role: condition override, exclusion, or review
- default: use only when clearly visible in county data or subject evidence

#### View / premium lot location
- role: exclusion or land-value delta when clearly reflected
- default: usually exclude premium mismatch unless direct land value handles it

---

## 19. Adjustment Methods by Feature

### 19.1 Living area adjustment

Preferred formula:

`gla_adjustment_i = (subject_gla - comp_gla) * gla_rate`

### Preferred `gla_rate` source order

1. median improvement-value-per-building-SF from matched same-class local peers
2. county cost schedule for the relevant class/quality
3. county-configured fallback rate

### Governance

- use improvement-driven rate, not full appraised $/SF, whenever feasible
- cap rate by county config to avoid runaway adjustments
- if `abs(subject_gla - comp_gla)` is large and the resulting adjustment burden is material, downgrade comp or exclude

### 19.2 Age / effective age adjustment

Preferred formula:

`age_adjustment_i = age_adjust(subject_effective_age, comp_effective_age, age_rate_or_schedule)`

### Source order

1. county depreciation schedule
2. county class-specific adjustment table
3. local matched-pair estimate
4. config fallback

### Governance

- if age difference is within a no-adjust band, score penalty may be enough
- if age difference exceeds fallback max and no rate exists, exclude

### 19.3 Full-bath adjustment

Preferred formula:

`full_bath_adjustment_i = (subject_full_baths - comp_full_baths) * full_bath_rate`

### Governance

- rate must be lower than adding a full room of equivalent GLA utility
- if bath mismatch is large and no rate exists, exclude

### 19.4 Half-bath adjustment

Preferred formula:

`half_bath_adjustment_i = (subject_half_baths - comp_half_baths) * half_bath_rate`

### Governance

- half-bath rate should usually be materially lower than full-bath rate
- if county exposes only effective baths, preserve raw fields anyway and map carefully

### 19.5 Bedroom treatment

Default MVP position:

- bedroom count is always used in filtering and scoring
- bedroom count is shown in packet evidence
- bedroom count is monetized only when all of the following are true:
  - bedroom delta remains material after GLA and bath screening
  - local utility difference is credible
  - county config enables bedroom adjustment
  - anti-double-counting check passes

Otherwise, use bedrooms as a comparability guardrail rather than a cash adjustment.

### 19.6 Lot-size adjustment

Preferred mode:

- lot size is always a distinct filter and scoring channel
- monetary lot-size adjustment is allowed only when county config explicitly enables it and anti-double-counting checks pass

### 19.7 Land / site adjustment

Preferred mode:

`land_adjustment_i = subject_land_value - comp_land_value`

Use only when:

- land utility is reasonably comparable
- the county’s land allocation appears credible
- the difference is primarily site-size / site-value, not a premium-location mismatch that should cause exclusion

If land utility mismatch is too large or site premium is non-standard, exclude instead of forcing a land adjustment.

### Important lot-size / land rule

`lot_size`, `land utility`, and `land value` are related but distinct concepts.

- `lot_size` is the measurable site-size field such as `land_sf` or `land_acres`
- `land utility` is whether the site is functionally comparable
- `land value` is the monetary support layer that may back an adjustment when credible

The MVP must not collapse these into one vague “lot” concept.

### 19.8 Garage / car storage adjustment

Preferred mode:

- direct contributory delta from county feature values, if available
- otherwise county-configured per-space or per-type fallback

### 19.9 Pool adjustment

Preferred mode:

- direct county pool value delta if exposed
- otherwise county-configured pool adjustment
- if pool mismatch is material and no credible source exists, exclude

### 19.10 Story count adjustment

Preferred mode:

- same stories preferred
- separate adjustment only if county config demonstrates material appraised-value difference not already captured elsewhere
- otherwise treat as score penalty or exclusion

### 19.11 Quality / class and condition adjustments

These adjustments are allowed only with strong governance.

Use in this order:

1. county class multiplier / cost schedule
2. county condition schedule
3. matched-pair roll-only estimate
4. otherwise exclude

If class/condition is not reliably obtainable for a county, tighten filters and reduce support.

### 19.12 Bathroom handling by county

#### Harris

Harris may use canonical:

- `full_baths`
- `half_baths`
- `bedrooms`
- `total_rooms`

`total_rooms` is secondary to `bedrooms` and must not replace it.

#### Fort Bend

Fort Bend may use canonical:

- `bedrooms`
- `half_baths`

Fort Bend may also use additive valuation-only bathroom features from `fort_bend_valuation_bathroom_features` when status/confidence are allowed by config.

Recommended auto-usable statuses:

- `exact_supported`
- `reconciled_fractional_plumbing`
- `quarter_bath_present`

Rows in:

- `ambiguous_bathroom_count`
- `incomplete_bathroom_count`
- `no_bathroom_source`

should require downgrade, exclusion, or manual review rather than being treated as exact numeric bathroom truth.

---

## 20. Comp Burden Limits and Exclusion Triggers

The MVP should avoid “Frankenstein comps.”

### Comp burden metrics

For each comp compute:

- `absolute_adjustment_total`
- `absolute_adjustment_pct_of_raw_value`
- `material_adjustment_count`
- `nontrivial_adjustment_sources_count`

### Default review / exclusion rules

- if total absolute adjustment exceeds **20%** of raw appraised value -> manual review
- if total absolute adjustment exceeds **25%** -> exclude by default
- if comp needs more than **4** material adjustment channels -> manual review
- if comp needs more than **5** material adjustment channels -> exclude by default
- if a material mismatch exists with no defendable adjustment path -> exclude

These are default config-driven thresholds and should be conservative.

---

## 21. Outlier and Conflict Filtering

Even after filtering and scoring, the MVP needs conflict control.

### Outlier checks

Perform robust checks on both raw and adjusted sets:

- raw appraised $/SF outlier check
- raw appraised value outlier check within same size band
- adjusted appraised value outlier check
- inconsistent feature profile check
- contradictory class/condition check
- anomalous land share check
- suspicious data defect check

### Recommended methods

Use simple, deterministic methods:

- percentile trimming
- median absolute deviation
- IQR fences
- configuration-backed hard caps

Do **not** rely on opaque anomaly models in MVP.

### Important rule

Outlier logic must not become hidden cherry-picking.  
Every exclusion must be persisted with a human-readable reason code.

---

## 22. Final Comp Set Selection

After filtering, scoring, and conflict review, select the final set.

### Selection rules

1. sort by normalized similarity score descending
2. apply comp-burden screens
3. apply diversity and geography controls
4. select the top defensible comps
5. stop at target count when quality begins to degrade

### Recommended target counts

- default target: **12**
- preferred range: **10–20**
- hard auto minimum: **8**

### Selection philosophy

The goal is not to maximize the number of comps.  
The goal is to obtain a **reasonable number of strongly comparable properties with manageable adjustment burden**.

### Manual-review exception

A 6–7 comp set may be used only if:

- the neighborhood is genuinely small
- the selection log proves the universe was exhausted
- dispersion is low
- adjustment burden is low
- analyst signs off

The system must not label that result as high confidence.

---

## 23. Roll-Based Valuation Methodology

This is the heart of the MVP.

### MVP methodology guardrail

The MVP is a roll-based unequal-appraisal model. Its final value conclusion is:

- not a market value estimate
- not a regression-predicted value
- not a raw `$ / SF` benchmark

Similarity scoring is used only to select comparable properties.

Dwellio may use `$ / SF` to screen, compare, and explain comps, but the final MVP roll-based unequal value must be the median of adjusted appraised values.

Any regression, matched-pair, or quantitative roll analysis is internal calibration / QA only unless separately approved and documented as a transparent adjustment-rate source.

### 23.1 Comp-level adjusted value

For each selected comp `i`:

`A_adj_i = A_i + Δ_gla_i + Δ_age_i + Δ_fullbath_i + Δ_halfbath_i + Δ_bed_i + Δ_land_i + Δ_garage_i + Δ_pool_i + Δ_story_i + Δ_quality_i + Δ_condition_i + Δ_other_i`

Where:

- `A_i` = raw appraised value of comparable property `i`
- each `Δ` term is a signed adjustment that normalizes the comp toward the subject
- `Δ_bed_i` is optional and controlled
- `Δ_other_i` exists only for county-configured channels approved in MVP scope

Equivalent generalized form:

`A_i_adj = A_i + SUM((x_subject,k - x_comp,k) * gamma_k)`

Where:

- `x_subject,k` and `x_comp,k` are feature values for channel `k`
- `gamma_k` is the approved adjustment rate or schedule output for feature `k`
- every approved rate or schedule must be versioned and persisted with source metadata

### 23.2 Final requested value

The MVP requested value is:

`requested_roll_value = median(A_adj_i for all selected comps)`

Equivalent generalized form:

`A_request = median(A_i_adj for all selected final comparables)`

`Reduction_indication = max(0, Subject_Appraised_Value - A_request)`

### 23.3 Important methodological rule

The final value is **not**:

- direct output of the similarity score
- direct output of regression
- weighted median of peer appraised $/SF
- average appraised value
- raw median without adjustments
- public estimate of market value

### 23.4 Why this is production-safe for MVP

This aligns the engine with the roll-based unequal methodology itself:

- comp selection is local and defendable
- adjustments are explicit
- the final value is the median of adjusted comparable appraised values
- no subject market value is needed

### 23.5 Optional secondary display metrics

The run may also compute, for internal QA only:

- subject raw appraised $/SF
- final comp raw appraised $/SF distribution
- final comp adjusted value distribution
- implied adjusted $/SF distribution
- subject rank among adjusted values

But these are support metrics, not the core legal conclusion.

### 23.6 Required selected-comp adjustment table

For every selected comp, persist an explicit adjustment table that includes at minimum:

- adjustment category
- subject value
- comp value
- difference
- adjustment rate or basis
- signed adjustment amount
- adjustment source
- confidence / reliability flag

The packet and admin evidence views must render from these persisted finalized adjustment records rather than recomputing ad hoc display math.

---

## 24. Sensitivity, Stability, and Robustness Checks

The MVP should include simple but meaningful robustness checks.

### Required sensitivity checks

1. **leave-one-out median drift**
   - recompute the median with each comp removed one at a time

2. **top/bottom removal check**
   - recompute median excluding highest and lowest adjusted comp

3. **trim check**
   - where sample size permits, recompute on lightly trimmed adjusted set

4. **adjustment burden stability**
   - test whether the result is overly driven by the most heavily adjusted comps

5. **raw `$ / SF` divergence check**
   - flag when raw appraised `$ / SF` diagnostics and adjusted-value conclusions diverge materially

### Required metrics

Persist at least:

- `median_all`
- `median_minus_high_low`
- `max_leave_one_out_delta`
- `median_absolute_deviation_adjusted_values`
- `adjusted_value_iqr`
- `average_adjustment_pct`
- `max_adjustment_pct`
- `trimmed_median_adjusted_value`
- `dominant_adjustment_channel`

### Manual review triggers

Require manual review if:

- `max_leave_one_out_delta` exceeds config tolerance
- removing top and bottom comps moves the median materially
- a small number of heavily adjusted comps drive the result
- dispersion remains high after adjustment
- the result hinges on fallback geography
- one adjustment category drives most of the value conclusion
- raw `$ / SF` diagnostics and adjusted-value conclusions diverge materially

---

## 24A. Regression, Quantitative Calibration, and QA

Regression is internal-only for MVP.

### Acceptable uses

- validate or calibrate adjustment schedules
- identify whether adjustment rates are directionally reasonable
- flag unusual county or neighborhood roll behavior
- test whether selected comps are behaving consistently with the broader roll
- support analyst review and future model calibration

### Prohibited uses

Regression must not be used as:

- the primary ARB-facing proof method
- a black-box final valuation engine
- a direct substitute for the median adjusted appraised value method
- a way to introduce subject market value evidence
- a direct source of comp-selection scoring weights without review and configuration approval

### Governance when regression-derived rates are used

If regression-derived rates are promoted into an adjustment schedule, they must be:

- roll-data based, not market-sale based, for the sole unequal MVP packet
- versioned and stored with model/config metadata
- subject to reasonableness limits
- explainable as internal support for adjustment schedules
- overridable by analyst review
- excluded from packet narrative unless Dwellio intentionally creates a transparent supporting appendix

### MVP scope note

Full regression diagnostics are not required for every county-year launch path.

However, if regression-derived schedules are used, the calibration workflow should define:

- minimum sample-size requirements
- coefficient stability checks
- multicollinearity review for multivariate models
- reasonableness bands
- analyst approval and config-promotion process

---

## 25. Confidence and Support Scoring

The MVP needs strong support scoring, but it should remain understandable.

### Two outputs, not one

Produce both:

- `support_status`
- `confidence_score`

### Support status values

- `supported`
- `supported_with_review`
- `manual_review_required`
- `unsupported`

### Confidence score range

- 0–100
- conservative
- not consumer-facing

### Suggested confidence components

| Component | Suggested weight |
|---|---:|
| final comp count | 20 |
| average similarity score | 20 |
| neighborhood purity | 10 |
| adjustment burden | 15 |
| adjusted-value stability | 15 |
| adjusted-value dispersion | 10 |
| field completeness / source quality | 5 |
| fallback tier depth | 5 |

### Additional confidence governance

Confidence should reflect not only generic comp quality, but also the reliability of the feature channels actually used in the run.

Examples:

- additive Fort Bend bathroom-derived fields should lower confidence less when status is `exact_supported`
- Fort Bend rows using medium-confidence additive bathroom status should receive an explicit downgrade
- low-confidence or ambiguous bathroom-derived rows should usually force review, major downgrade, or exclusion
- missing `quality_code` or `condition_code` where material should lower confidence materially

### Penalty ideas

Subtract confidence for:

- comp count below 10
- heavy F1/F2 fallback
- average similarity below threshold
- high adjustment burden
- missing class/condition where material
- large bath/bed mismatches
- unstable median
- analyst override dependence

### Confidence labels

- `85–100` = high
- `70–84` = medium_high
- `55–69` = medium
- `40–54` = low
- `<40` = unsupported

### Important rule

High confidence should be rare.  
An 8-comp fallback case with meaningful adjustments should not earn the same label as a 14-comp same-neighborhood case with minimal adjustments.

The unequal-roll confidence model is separate from current public instant-quote confidence logic because the methodology, artifacts, and failure modes are different.

---

## 26. Minimum Auto-Supported Packet Standard

The MVP must define the actual minimum for an automated production packet.

### Minimum auto-supported packet requirements

- at least **8** final comps
- same neighborhood or tightly bounded same market area
- same property type / use code
- living area generally within fallback limits
- quality / class and condition comparable
- line-item adjustments documented
- median adjusted comparable value calculated
- source artifacts for all comps persisted
- no prohibited market-value content in packet
- support status at least `supported_with_review`

### Manual-review-only minimum

A manual-review packet may proceed with **6–7** comps only if:

- very strong same-neighborhood comparability
- low dispersion
- low adjustment burden
- documented scarcity
- analyst approval
- packet clearly marked as reviewed

### Unsupported packet conditions

Do not auto-generate packet value conclusion if:

- fewer than 6 comps
- high adjustment burden and high dispersion
- critical source fields missing
- county readiness insufficient
- too many comps lack source artifacts
- final median is unstable

---

## 27. Packet Integration Requirements

The MVP’s primary business output is the packet.

### Packet must include

- subject identification
- current appraised value
- final requested roll-based value
- percent and dollar requested reduction from appraised value
- final selected comps only
- comp grid with key fields
- comp adjustment lines
- adjusted comp values
- ordered list of adjusted values
- median adjusted value calculation
- concise narrative explaining roll-based unequal appraisal
- appendix or source references for county records/property cards

The packet may show raw or adjusted `$ / SF` only as explanatory support. It must not imply that `$ / SF × subject SF` is the final requested-value formula.

### Packet comp grid fields

At minimum:

- parcel/account ID
- address
- neighborhood/subdivision
- appraised value
- land value
- improvement value
- living area
- year built / effective age
- bedrooms
- full baths
- half baths
- stories
- garage
- pool
- quality / class
- condition
- lot size
- raw appraised $/SF
- total adjustments
- adjusted value

### Prohibited packet content for sole unequal MVP

Do not include:

- MLS sold comps
- subject market value estimate
- subject purchase price
- Dwellio AVM market estimate
- Zillow / Redfin / Realtor values
- “Your home is worth only $X on the open market”
- ratio-lane charts
- sales verification exhibits

### Packet narrative posture

The packet should say, in substance:

- these are comparable properties from the roll
- they were selected using consistent filters
- each was adjusted to the subject
- the median adjusted comparable value is $X
- the subject’s appraised value exceeds that median
- the requested value is $X

---

## 28. Narrative Generation

Narratives must be template-driven from structured fields.

### Required narrative outputs

- admin summary
- packet summary
- support rationale
- adjustment rationale summary
- fallback rationale summary
- confidence rationale summary

### Example packet narrative concept

> The subject property is appraised above the median adjusted appraised value of a reasonable number of comparable properties in the same neighborhood or market area. Each comparable was selected using consistent filters and adjusted to the subject using documented roll-based adjustment logic. The resulting median adjusted comparable value is the requested unequal-appraisal value.

### Narrative rules

- no free-form AI-only value claims
- no market value language in sole unequal packet
- mention only methods actually used in the run
- if fallback geography was used, say so
- if analyst override materially changed the result, that must be visible in admin and optionally disclosed internally
- do not present scoring weights as valuation weights
- do not present regression outputs as the final proof method

---

## 29. Internal Tax-Impact Logic (Internal Only)

This is not the core valuation conclusion, but the business may still need a tax-impact estimate.

### Rules

- tax impact is internal-only
- tax impact must be downstream from the requested roll-based value
- tax impact must not alter the packet valuation conclusion
- tax impact must use taxable-value logic, exemptions, and caps separately

### Internal formula concept

1. compute requested appraised value from median adjusted comps
2. estimate resulting taxable value by unit
3. apply exemptions and caps as appropriate
4. estimate tax delta using current known rates or approved estimate method

### Important separation rule

`requested_roll_value` and `estimated_tax_savings` are different objects and must remain separate in schema, services, and packet logic.

---

## 30. Required Conceptual Data Model

The exact schema should follow repo conventions, but conceptually the MVP should stay close to the current repo and avoid unnecessary greenfield tables.

### MVP schema-fit principle

For launch, prefer:

- current canonical parcel-year data as the runtime substrate
- run-scoped immutable snapshots
- explicit run, candidate, adjustment, and override persistence

Do **not** make the MVP depend on:

- `parcel_features`
- `market_features`
- `equity_features`

unless those tables become populated, validated, and operational before implementation.

---

## 30.1 `unequal_roll_runs`

### Purpose
Stores one full MVP roll-based unequal valuation run.

### Suggested fields

- `unequal_roll_run_id`
- `parcel_id`
- `county_id`
- `tax_year`
- `run_status`
- `readiness_status`
- `support_status`
- `support_blocker_code`
- `model_version`
- `config_version`
- `source_coverage_status`
- `candidate_count_discovered`
- `candidate_count_eligible`
- `candidate_count_scored`
- `candidate_count_selected`
- `fallback_tier_used`
- `requested_roll_value`
- `requested_roll_value_rounded`
- `subject_appraised_value`
- `requested_reduction_amount`
- `requested_reduction_pct`
- `confidence_score`
- `confidence_label`
- `finalized_for_packet`
- `override_status`
- `summary_json`
- `created_at`
- `updated_at`
- `completed_at`

---

## 30.2 `unequal_roll_subject_snapshots`

### Purpose
Immutable run-specific subject snapshot.

### Suggested fields

- `unequal_roll_run_id`
- `parcel_id`
- `county_id`
- `tax_year`
- `address`
- `neighborhood_code`
- `subdivision_name`
- `property_type_code`
- `property_class_code`
- `appraised_value`
- `taxable_value`
- `district_market_value`
- `land_value`
- `improvement_value`
- `living_area_sf`
- `year_built`
- `effective_age`
- `lot_size_sf`
- `bedrooms`
- `full_baths`
- `half_baths`
- `effective_baths`
- `total_rooms`
- `stories`
- `garage_spaces`
- `garage_type`
- `pool_flag`
- `quality_code`
- `condition_code`
- `homestead_flag`
- `homestead_cap_flag`
- `circuit_breaker_flag`
- `exemption_total`
- `subject_appraised_psf`
- `valuation_bathroom_features_json`
- `snapshot_json`
- `source_provenance_json`
- `created_at`

---

## 30.3 `unequal_roll_candidates`

### Purpose
Stores all discovered and normalized candidate properties for the run.

### Suggested fields

- `unequal_roll_candidate_id`
- `unequal_roll_run_id`
- `candidate_parcel_id`
- `address`
- `neighborhood_code`
- `subdivision_name`
- `discovery_tier`
- `property_type_code`
- `property_class_code`
- `appraised_value`
- `taxable_value`
- `district_market_value`
- `land_value`
- `improvement_value`
- `living_area_sf`
- `year_built`
- `effective_age`
- `lot_size_sf`
- `bedrooms`
- `full_baths`
- `half_baths`
- `effective_baths`
- `total_rooms`
- `stories`
- `garage_spaces`
- `garage_type`
- `pool_flag`
- `quality_code`
- `condition_code`
- `raw_appraised_psf`
- `gla_diff_pct`
- `age_diff_abs`
- `lot_diff_pct`
- `bed_diff_abs`
- `effective_bath_diff_abs`
- `story_diff_abs`
- `same_neighborhood_flag`
- `same_subdivision_flag`
- `same_quality_flag`
- `same_condition_flag`
- `eligibility_status`
- `eligibility_fail_reason_code`
- `raw_similarity_score`
- `normalized_similarity_score`
- `outlier_flag`
- `outlier_reason_code`
- `selected_final_flag`
- `selected_rank`
- `adjusted_appraised_value`
- `total_signed_adjustment`
- `total_absolute_adjustment`
- `adjustment_pct_of_raw_value`
- `source_provenance_json`
- `created_at`

---

## 30.4 `unequal_roll_adjustments`

### Purpose
Stores the line-item adjustments for each selected comp.

### Suggested fields

- `unequal_roll_adjustment_id`
- `unequal_roll_run_id`
- `candidate_parcel_id`
- `adjustment_type`
- `source_method_code`
- `rate_or_basis_json`
- `subject_value_json`
- `candidate_value_json`
- `difference_value_json`
- `signed_adjustment_amount`
- `adjustment_reliability_flag`
- `material_flag`
- `notes`
- `created_at`

### Example `adjustment_type` values

- `gla`
- `age`
- `full_bath`
- `half_bath`
- `bedroom`
- `land`
- `garage`
- `pool`
- `story`
- `quality`
- `condition`
- `other_configured_feature`

---

## 30.5 `unequal_roll_overrides`

### Purpose
Stores all analyst overrides.

### Suggested fields

- `unequal_roll_override_id`
- `unequal_roll_run_id`
- `override_actor_user_id`
- `override_type`
- `override_reason_code`
- `field_name`
- `old_value_json`
- `new_value_json`
- `notes`
- `created_at`

---

## 30.6 Packet evidence persistence for MVP

Packet evidence may be rendered from:

- `unequal_roll_runs`
- `unequal_roll_subject_snapshots`
- finalized rows in `unequal_roll_candidates`
- `unequal_roll_adjustments`
- `unequal_roll_overrides`

For MVP, a separate `packet_unequal_roll_evidence_items` table is optional rather than required. Add it only if packet integration proves a persisted render-layer artifact is necessary.

---

## 31. Configuration Architecture

This MVP must be config-driven.

### Required config categories

- county readiness rules
- discovery tiers and radius limits
- preferred and fallback filters
- similarity score weights
- adjustment source precedence
- feature-level enable/disable flags
- comp burden thresholds
- final comp-count thresholds
- support / confidence penalties
- packet display / rounding rules
- manual-review triggers

### Recommended config layout

- `config/valuation/unequal_roll_mvp.yaml`
- `config/counties/<county>/unequal_roll_mvp.yaml`

### County-configurable items

At minimum:

- neighborhood field mappings
- class/condition field mappings
- subject and candidate source URLs/routes
- pool target limits
- filter tolerances
- land-share materiality threshold
- bath / garage / pool / story adjustment schedules
- age schedule source mode
- GLA rate mode
- packet wording tweaks if needed
- unsupported feature flags

### Important MVP rule

Do not hardcode county quirks in service logic if they can live in config.

---

## 32. Admin / Backend Requirements

The MVP is internal, so admin quality matters more than public polish.

### Admin must allow

- viewing latest and prior runs
- viewing county readiness/support status
- viewing subject snapshot
- viewing candidate discovery tiers
- viewing filtered-out candidates and reasons
- viewing similarity scores
- viewing selected comps
- viewing comp-level adjustments
- viewing sensitivity metrics
- viewing support/confidence explanation
- re-running the model
- excluding a comp
- including a comp in manual-review mode
- changing the final set
- overriding requested value
- finalizing for packet
- reopening finalized runs through explicit workflow

### Important admin rule

Every manual action must be audit-trailed.  
The original automated result must remain visible.

---

## 33. API / Service Layer Requirements

The MVP should have internal/admin-facing APIs only.

### Conceptual service surfaces

1. run valuation
2. fetch latest run
3. fetch run detail
4. fetch candidate grid
5. fetch selected comp grid
6. apply override
7. finalize for packet
8. fetch packet-ready evidence payload

### Example routes

- `POST /admin/api/unequal-roll-mvp/run`
- `GET /admin/api/unequal-roll-mvp/runs/{run_id}`
- `GET /admin/api/unequal-roll-mvp/runs/{run_id}/candidates`
- `GET /admin/api/unequal-roll-mvp/runs/{run_id}/selected-comps`
- `POST /admin/api/unequal-roll-mvp/runs/{run_id}/override`
- `POST /admin/api/unequal-roll-mvp/runs/{run_id}/finalize`

### Typed response requirements

Responses should include:

- support status
- confidence score
- candidate counts
- fallback tier used
- final requested roll value
- reduction amount
- selected comp count
- QA flags
- packet readiness status

---

## 34. Override Policy

Overrides are allowed because the MVP must be business-ready today, not academically pure.

### Allowed override types

- exclude candidate
- include candidate
- force manual-review geography expansion
- override adjustment line
- disable problematic adjustment channel
- change selected comp order
- set final requested value
- mark support status
- mark packet finalized

### Override rules

- every override must record actor, reason, timestamp, and notes
- automated result remains preserved
- override-heavy runs must lose confidence
- packet finalization after override must remain explicit

### Override philosophy

The MVP should not pretend full automation where county data is weak.  
Analyst intervention is allowed, but must be transparent and measurable.

---

## 35. Versioning and Reproducibility

Every run must be reproducible.

Persist:

- model version
- config version
- parser version
- subject snapshot
- candidate snapshot details
- selected comp set
- adjustment lines
- sensitivity metrics
- final outputs
- override history

### Reproducibility rule

If inputs, config, and parser versions have not changed, reruns should reproduce the same automated result.

---

## 36. QA Gates and Manual Review Triggers

This is where the MVP becomes production-ready instead of merely functional.

### Required QA gates before packet finalization

1. county readiness must be supported
2. subject snapshot must be complete enough
3. selected comp count must meet threshold
4. every selected comp must have source provenance
5. every selected comp must have a persisted adjustment grid
6. packet comps must match finalized selected comps
7. final requested value must equal median of persisted adjusted comp values
8. no prohibited market-value content may appear in packet template payload
9. support status must be `supported` or `supported_with_review`
10. override audit trail must be complete

### Manual review triggers

- selected comp count under 10
- selected comp count under 8 automatically
- adjusted-value median unstable
- high adjustment burden
- excessive fallback geography
- class/condition missing for many comps
- subject data correction required
- more than two material overrides
- county data parser uncertainty
- lot/land issues or premium-location mismatch
- bed/bath mismatch reliance too high

### Release blocker rule

Do not let the MVP auto-finalize a packet when manual-review triggers are unresolved.

---

## 37. Testing Requirements

### Unit tests

- filter thresholds by tier
- similarity component scoring
- scoring does not directly enter `A_request`
- normalization math
- feature availability rules
- adjustment formulas
- anti-double-counting checks
- median calculation
- leave-one-out calculation
- confidence scoring
- override persistence

### Integration tests

- fully supported same-neighborhood run
- supported-with-review fallback run
- sparse-neighborhood manual-review run
- unsupported county-year
- comp exclusion workflow
- override workflow
- finalize-for-packet workflow
- packet evidence generation
- Fort Bend additive bathroom status/confidence gating

### Data QA tests

- no missing critical fields in finalized outputs
- every selected comp has provenance
- every selected comp has at least one persisted basis row (even zero-adjustment comps must be explicit)
- final requested value matches median of selected adjusted values
- raw median `$ / SF × subject SF` is not used as the final requested value
- packet comp set equals finalized selected set
- prohibited market-value fields are suppressed from packet payload
- packet values match the finalized adjusted comp table exactly

### Methodology guardrail tests

- regression outputs cannot override the final value formula
- adjustment caps and manual-review triggers work
- scoring weights cannot override the median-of-adjusted-values conclusion

### Regression / calibration tests

- compare model outputs with internal historical review outcomes where available
- monitor analyst override frequency by county
- monitor comp-count distribution
- monitor adjustment-burden distribution
- monitor packet rejection / analyst return reasons

---

## 38. Observability Requirements

Track at least:

- run volume
- supported vs unsupported rate
- county readiness rates
- average candidate pool size
- average selected comp count
- average fallback tier used
- average confidence score
- override frequency
- median instability rate
- packet finalization rate
- top blocker codes

### Dashboards

Create admin dashboards for:

- county readiness
- support distribution
- comp-count distribution
- confidence distribution
- override hotspots
- parser/source issues
- packet finalization bottlenecks

---

## 39. Recommended Default Business Logic

### Default operating choices

- method: roll-based only
- protest posture: sole unequal appraisal
- discovery: neighborhood-first
- candidate pool target: 25–75
- final comp target: 12
- hard auto minimum: 8
- value conclusion: median of adjusted appraised values
- packet posture: no subject market value evidence
- tax impact: internal only
- confidence philosophy: conservative
- analyst review: allowed and auditable

### Default adjustment policy

- GLA: required
- age/effective age: required when material and rate exists
- full bath: required when material
- half bath: required when material
- bedrooms: filter/score always; cash adjustment only when enabled and justified
- quality/condition: same preferred, adjacent only with defendable adjustment
- lot size: usually filter/land reasonableness first; monetary adjustment only when warranted
- garage/pool/story: adjust or exclude if material

### Default packet rule

If filing solely on unequal appraisal, the packet should remain entirely within the roll-based framework.

---

## 40. Example End-to-End Walkthrough

### Subject

- County: Harris
- Tax year: 2026
- Appraised value: $400,000
- Living area: 2,100 SF
- Year built: 2005
- Beds/Baths: 4 / 2.5
- Stories: 2
- Garage: 2-car
- Pool: no
- Neighborhood code: N-410
- Quality: same cohort
- Condition: average

### Candidate discovery

- Stage A same-neighborhood discovery returns 41 candidates
- After hard filters Tier P: 18 remain
- After score ranking and conflict filtering: 13 remain
- After comp-burden review: 11 selected final comps

### Example adjustment on one comp

Comp raw facts:

- Appraised value: $382,000
- GLA: 2,000 SF
- Year built: 2008
- Baths: 2.0
- Half baths: 0
- Garage: 2
- Pool: no
- Land value: $95,000

Subject facts:

- GLA: 2,100 SF
- Year built: 2005
- Baths: 2.0
- Half baths: 1
- Land value: $100,000

Assume:

- `gla_rate = $110 / SF`
- `half_bath_rate = $6,500`
- `age_adjustment = +$3,000` because the comp is newer and is adjusted down to the older subject equivalent
- `land_adjustment = $5,000`

Then:

- GLA adjustment = `(2100 - 2000) * 110 = +11,000`
- Half-bath adjustment = `+6,500`
- Age adjustment = `+3,000`
- Land adjustment = `+5,000`

Adjusted value:

`382,000 + 11,000 + 6,500 + 3,000 + 5,000 = 407,500`

Repeat for all 11 comps.  
Suppose the ordered adjusted values are:

- 352,000
- 357,000
- 360,500
- 362,000
- 364,000
- 366,000
- 368,500
- 370,000
- 371,000
- 375,000
- 381,000

The median adjusted value is:

`366,000`

So:

- `requested_roll_value = 366,000`
- `requested_reduction = 400,000 - 366,000 = 34,000`

That is the MVP value conclusion.

---

## 41. Recommended Repo Artifact Plan

Create or update:

- `docs/architecture/unequal-roll-based-mvp-spec.md`
- `docs/architecture/unequal-roll-mvp-api-contracts.md`
- `docs/architecture/unequal-roll-mvp-schema-spec.md`
- `docs/architecture/unequal-roll-mvp-confidence-framework.md`
- `docs/architecture/unequal-roll-mvp-packet-integration.md`
- `docs/architecture/unequal-roll-mvp-admin-workflows.md`

Implementation artifacts should include:

- migrations
- service modules
- candidate assembly modules
- valuation pipeline modules
- admin APIs
- admin read models
- packet integration logic
- tests
- runbook updates

### Repo-fit implementation note

For MVP, implementation should prefer:

- canonical DB-backed subject/candidate assembly
- separate unequal-roll run persistence
- run-scoped immutable snapshots

For MVP, implementation should avoid making these launch blockers:

- `parcel_features`
- `market_features`
- `equity_features`
- request-time county scraping
- packet-only render tables unless packet integration truly requires them

---

## 42. Full-Length Codex Prompt — MVP Production Ready

```text
You are working inside the Dwellio repository.

Create a new branch from updated main for the unequal-roll-based MVP work and stop if the working tree is not clean.

Before making changes, read and follow these files in this authority order:

1. docs/source_of_truth/CANONICAL_CONTEXT.md
2. docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md
3. docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md
4. docs/source_of_truth/DWELLIO_BUILD_PLAN.md
5. docs/source_of_truth/AGENT_RULES.md
6. docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md
7. docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md

Also review:
- docs/runbooks/CANONICAL_PRECEDENCE.md
- docs/runbooks/ARCHITECTURE_STATE.md
- docs/runbooks/ARCHITECTURE_MAP.md
- docs/architecture/implementation-spec.md
- docs/architecture/api-contracts.md
- docs/architecture/comp-engine.md
- docs/architecture/valuation-savings-recommendation-engine.md
- docs/architecture/testing-observability-security.md
- docs/architecture/schema-reference.md

Treat docs/source_of_truth/* as highest authority.
Treat stable repo schema and contracts as higher authority than summaries.
Do not break canonical public contracts.

Objective:
Implement Dwellio’s MVP unequal-appraisal engine for 2026 Texas SFR protests using only the roll-based method.

Hard scope constraints:
- Texas only
- SFR only
- roll-based unequal appraisal only
- sole unequal-appraisal posture
- no subject market value evidence in packet logic
- no ratio method
- no MLS or market comp dependency in valuation logic
- no public-facing output

Repo-fit constraints:
- keep this engine separate from InstantQuoteService and public quote contracts
- use current canonical parcel-year data as the primary runtime substrate
- do not require parcel_features / market_features / equity_features for MVP launch
- do not make request-time county scraping or raw parsing part of the valuation path

Core legal-method constraint:
The final requested value must be the median of adjusted appraised values of the selected comparable properties.

Methodology guardrails:
- similarity scoring is for comp selection only
- scoring weights are not valuation weights
- regression is internal-only for MVP calibration/QA unless separately promoted into transparent adjustment schedules
- Dwellio may use $/SF to screen, compare, and explain comps, but the final MVP roll-based unequal value must be the median of adjusted appraised values

Do not:
- compute final value from the similarity score
- compute final value from regression output
- compute final value from weighted median of peer assessed psf
- use taxable value as the unequal comparison field
- mix subject market value into the sole unequal packet
- silently override automated outputs
- auto-support runs with fewer than 8 comps unless an explicit manual-review exception exists

Implementation scope:

A) Schema / persistence
Implement or extend run tables aligned with repo conventions.

Conceptually persist:
- unequal_roll_runs
- unequal_roll_subject_snapshots
- unequal_roll_candidates
- unequal_roll_adjustments
- unequal_roll_overrides

For MVP:
- store final selected-comp state on unequal_roll_candidates
- store final run summary outputs on unequal_roll_runs
- add a separate packet evidence table only if packet integration truly requires persisted render artifacts

Persist at minimum:
- parcel_id / county_id / tax_year
- run status
- readiness status
- support status
- model/config/parser versions
- candidate counts
- selected comp count
- fallback tier used
- subject appraised value
- requested roll value
- requested reduction
- confidence score/label
- finalized_for_packet flag
- timestamps

B) County readiness and source provenance
Implement county readiness gating.
Persist source provenance for subject and candidates:
- canonical source family or route identifier
- source tax year
- run snapshot timestamp
- parser/normalization version where available
- raw artifact hash or source file/version reference where available
- completeness status

Block unsupported county-years.

C) Subject snapshot
Build immutable subject snapshots with:
- appraised value
- taxable value
- district market value
- land value
- improvement value
- living area
- year built / effective age
- lot size
- beds
- full baths
- half baths
- total rooms where canonically available
- stories
- garage
- pool
- quality/class
- condition
- neighborhood/subdivision
- homestead / cap flags
- derived fields such as appraised psf, effective baths, land share
- Fort Bend additive valuation bathroom payload where allowed

D) Candidate discovery
Build canonical-DB-backed candidate discovery:
1. same county-year and SFR cohort
2. same neighborhood / subdivision
3. nearby fallback in same market area if needed
3. manual-review-only fallback if still sparse

Target raw pool size around 25-75 before scoring.
Persist discovery tier and provenance.

E) Hard eligibility filters
Implement config-driven preferred and fallback filters for:
- property type
- neighborhood / subdivision / market area
- living area
- year built / effective age
- lot size
- quality/class
- condition
- bedrooms
- full / half baths
- total rooms where canonically supported
- stories
- garage
- pool

Implement automatic rejection for obviously incompatible properties.

F) Similarity scoring
Implement deterministic similarity scoring only for ranking/selection.
Use configuration-backed weights for:
- neighborhood
- quality/class/condition
- GLA
- age
- beds/baths
- lot/land
- stories
- major features

Persist raw and normalized scores.
Do not let similarity scoring directly enter the final requested-value formula.

G) Adjustment engine
Implement line-item roll-based adjustments with persisted rows.

Support MVP adjustment channels:
- gla
- age/effective_age
- lot_size when county config supports explicit monetization
- land_value / site adjustment when material
- full_bath
- half_bath
- bedroom (optional and gated)
- garage
- pool
- story
- quality
- condition

County-aware notes:
- bedrooms are a first-class channel for both Harris and Fort Bend
- Harris total_rooms is separate from bedrooms and must not replace it
- Fort Bend additive bathroom-equivalent may be used only with explicit status/confidence gating

Implement source precedence:
1. canonical CAD roll fields and direct CAD contributory values
2. county cost/class/depreciation schedules
3. local matched-pair or regression-derived roll-only calibration
4. county-configured fallback
5. exclude if no defendable adjustment path exists

Implement anti-double-counting checks.

H) Comp burden and exclusion
For each selected comp compute:
- total signed adjustment
- total absolute adjustment
- adjustment pct of raw value
- material adjustment count

Apply review/exclusion thresholds from config.
Default:
- >20% adjustment burden = manual review
- >25% = exclude by default

I) Final comp selection
Select final comps after filtering/scoring/conflict review.
Default target:
- preferred 12
- acceptable 10-20
- hard auto minimum 8
- 6-7 only as manual-review exception with analyst approval

Persist selection ranks and inclusion reasons.

J) Valuation logic
For each selected comp:
A_adj_i = raw appraised value + sum of signed feature adjustments

Final requested value:
requested_roll_value = median(all selected adjusted values)

Persist:
- adjusted value per comp
- ordered adjusted values
- final median
- requested reduction amount / pct

K) Stability and QA metrics
Implement:
- leave-one-out median drift
- high/low removal median
- trimmed median
- adjusted-value MAD/IQR
- average and max adjustment burden
- average similarity score
- neighborhood purity
- raw psf divergence check

Trigger manual review when stability is weak.

L) Support/confidence scoring
Implement support statuses:
- supported
- supported_with_review
- manual_review_required
- unsupported

Implement conservative confidence score 0-100 using:
- comp count
- similarity
- neighborhood purity
- adjustment burden
- adjusted-value stability
- dispersion
- field completeness
- fallback depth
- feature-channel reliability, including additive Fort Bend bathroom reliability when used

M) Packet integration
Implement packet-ready payloads using only finalized selected comps.
Include:
- subject summary
- comp grid
- adjustment lines
- ordered adjusted values
- median calculation
- requested value
- concise roll-based unequal narrative

Packet may show $/SF as explanatory support only.
Do not present $/SF × subject SF as the final method.

Suppress all prohibited market-value content for sole unequal packet use.

N) Overrides
Allow audited overrides for:
- comp inclusion/exclusion
- adjustment edits
- selected set edits
- support status
- final requested value
- packet finalization

All overrides must preserve original automated outputs.

O) Admin APIs
Implement internal/admin APIs for:
- run
- fetch run
- fetch candidates
- fetch selected comps
- override
- finalize

P) Testing
Add unit, integration, and data QA tests for:
- filters
- scoring
- adjustments
- anti-double-counting
- median calculations
- support scoring
- override flows
- packet consistency
- market-value suppression
- scoring does not directly enter requested_roll_value
- raw median psf times subject sf is not used as the final requested value
- regression outputs cannot override the median-of-adjusted-values formula
- Fort Bend additive bathroom status/confidence gating

Q) Documentation
Create/update docs matching repo style.
Do not create duplicate competing docs if canonical equivalents already exist.

Execution sequence:
1. inspect canonical docs and repo patterns
2. design implementation plan
3. add/update schema
4. add/update config
5. implement readiness + acquisition + filtering + scoring
6. implement adjustment engine
7. implement selection + valuation + QA metrics
8. implement packet integration
9. implement admin APIs
10. implement tests
11. update docs
12. run lint/build/tests
13. summarize implementation and any canonical conflicts resolved
```

---

## 43. Suggested Pre-Flight Prompt for Branch Creation

```text
You are working inside the Dwellio repository.

Create a new branch from updated main for the unequal-roll-based MVP work.

Preflight:
1) git status --short --branch
2) git switch main
3) git pull --ff-only origin main
4) git switch -c stageXX_unequal_roll_mvp
5) git status --short --branch

Stop if the working tree is not clean.

Then read and follow these files in this authority order:
1. docs/source_of_truth/CANONICAL_CONTEXT.md
2. docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md
3. docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md
4. docs/source_of_truth/DWELLIO_BUILD_PLAN.md
5. docs/source_of_truth/AGENT_RULES.md
6. docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md
7. docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md

Also review:
- docs/runbooks/CANONICAL_PRECEDENCE.md
- docs/runbooks/ARCHITECTURE_STATE.md
- docs/runbooks/ARCHITECTURE_MAP.md
- docs/architecture/implementation-spec.md
- docs/architecture/api-contracts.md
- docs/architecture/comp-engine.md
- docs/architecture/valuation-savings-recommendation-engine.md
- docs/architecture/testing-observability-security.md
- docs/architecture/schema-reference.md

Do not start implementation until you have inspected the current repo’s valuation-run patterns, packet/evidence structures, admin read models, and county config patterns.
Then provide a concise implementation plan before coding.
```

---

## 44. Suggested Clean Commit / Push Prompt

```text
Review all changes related to the unequal-roll-based MVP implementation.

Before committing:
1) git status --short
2) verify changes are limited to this feature
3) run the project’s canonical lint/build/test commands
4) confirm no unrelated files are included
5) summarize what changed, including:
   - schema / migrations
   - config
   - source acquisition / parsers
   - filters / scoring / adjustment engine
   - packet integration
   - admin APIs
   - tests
   - docs

Create a clean commit with a precise message.

Suggested commit message:
feat: add roll-based unequal appraisal MVP for 2026 protest workflows

Then push the current feature branch to origin.
```

---

## 45. Source Notes for Maintenance

This MVP spec should be maintained against the following source categories:

1. Texas Tax Code provisions governing unequal appraisal and judicial review
2. SB 2063 / post-2026 procedure changes
3. Texas Comptroller ARB training and taxpayer assistance materials
4. county-specific public-data availability and parsing behavior
5. Dwellio internal outcome analytics and analyst feedback

When legal text, Comptroller training, or county field availability changes, update:

- readiness rules
- packet suppression rules
- filter tolerances
- adjustment source precedence
- confidence penalties
- county configs

This spec should be reviewed before each protest season.

---

## 46. Final Implementation Position

The correct Dwellio MVP for 2026 is:

- not the full unequal-appraisal framework
- not ratio-method first
- not market-comp dependent
- not a public quote tool
- not a weighted-median $/SF shortcut
- yes to neighborhood-first roll-only comparable selection
- yes to explicit line-item adjustments
- yes to median of adjusted appraised values
- yes to conservative support/confidence
- yes to strong QA gates
- yes to packet-safe market-value suppression
- yes to auditable manual review where needed

This is the leanest defensible production shape for Dwellio’s launch-stage unequal-appraisal product.
