# Unequal-Roll MVP Workflow Contract

## 1. Purpose

This document defines the MVP product workflow for unequal-roll evaluation and analyst packet generation.

The workflow is meant to keep the current product direction clear:

- evaluate a defensible `similarity_top_100` baseline,
- attempt a simple governed rerank,
- route each case into the correct analyst packet mode,
- produce reviewable evidence,
- and avoid automatic production value writing.

This is a durable MVP contract, not an irreversible legal decree. It should prevent accidental drift while still allowing evidence-backed revisions.

## 2. Architecture Name

`governed_similarity_baseline_with_simple_rerank`

## 3. Workflow Summary

For each subject property:

1. Always run `similarity_top_100`.
2. Always evaluate `simple_value_tier_rerank`.
3. Apply governed decision rules:
   - if rerank is safe and materially better, classify as `governed_rerank_ready`;
   - if rerank is not materially better but baseline supports a value reduction, classify as `baseline_support_only`;
   - if no material defensible reduction is found, classify as `no_reduction_no_action`;
   - if evidence is risky or mixed, route to `fallback_safety_blocked`, `hold_out`, or `analyst_review_only`.
4. Generate an analyst-reviewable packet.
5. Do not write production values automatically.

This is the default MVP evaluation and packet-routing workflow.

It is not:

- unconditional rerank defaulting,
- automatic production value writing,
- automatic protest filing,
- removal of analyst review,
- or a permanent freeze on future improvements.

## 4. Candidate Discovery And Eligibility

The MVP workflow is scoped to single-family residential (`SFR`) unequal-roll evaluation.

At a high level, the current workflow is designed around the supported county/tax-year data available in the canonical parcel stack. Validation and packet generation should state the requested tax year, and packet-visible comps should match that requested tax year unless a case is explicitly blocked or held out for tax-year evidence issues.

Candidate discovery starts from the same-neighborhood pool. Same-neighborhood support is preferred because it keeps the comparison set closer to the subject's local roll context and reduces the need for broad market or location assumptions.

Eligibility basics:

- subject must be an eligible SFR parcel;
- candidates should be in the same neighborhood pool for the baseline workflow;
- candidates must have `living_area_sf > 0`;
- candidates must have `appraised_value > 0`;
- wrong-tax-year comp evidence should block or route cases out of first-pilot packet use;
- discovered candidates can be scored and reviewed even if later excluded from final-value support.

`true_full_pool_requested` validation means the workflow requested the full same-neighborhood candidate universe for the subject, not a capped proxy. Bounded proxy modes may be useful for runtime diagnostics, but bounded proxy results should not be used for production-shaped conclusions.

The MVP baseline is `similarity_top_100`. If simple reranking does not create a safe, material improvement, the workflow falls back to baseline support when the baseline itself supports a model-backed reduction.

## 5. Similarity Scoring Methodology

Similarity scoring is used for candidate ranking and selection. It is not the final value formula.

The score is a governed composite used to order candidate comparables before adjustment and final-value logic. Major factors used or considered include:

- same neighborhood;
- subdivision or local area where available;
- living area similarity;
- year built or effective age;
- beds, baths, and stories;
- land size;
- value tier and value per SF;
- quality and condition where available;
- pool and other feature flags where available.

This document intentionally does not restate exact scoring weights. If exact scoring behavior is needed, use the implementation and tests as the source of truth. The product contract is that similarity scoring should remain explainable, local, and subordinate to final-value and governance checks.

## 6. Baseline Selection: `similarity_top_100`

`similarity_top_100` is the baseline unequal-roll support engine.

It ranks the same-neighborhood candidate pool by similarity and selects the top candidates for downstream adjustment and final-value logic. The baseline can produce value support even when the simple rerank does not add incremental benefit.

Baseline reductions are reported as baseline support. They are not rerank lift and should not be described as rerank-generated savings.

## 7. Simple Rerank Methodology

`simple_value_tier_rerank` is evaluated by default.

The simple rerank starts from similarity and is intentionally conservative. It emphasizes value-tier and value-per-SF outlier protection so lower-value capture does not come from obviously weak or out-of-tier comps.

The simple MVP rerank does not currently use:

- bedroom penalty;
- lower-value bonus;
- strong adjustment-burden ranking penalty;
- hard land mismatch;
- micro-location as default product logic.

The rerank is retained only when it is governed-safe and materially better than `similarity_top_100`. Raw rerank output is not the production default.

## 8. Adjustment Support Methodology

Adjustment support determines whether a candidate can be adjusted credibly enough to participate in final value evidence.

Candidates can be classified as:

- `adjustment_ready`;
- `adjustment_ready_with_review`;
- `adjustment_limited`;
- `adjustment_limited_with_review`;
- `excluded_from_adjustment_support`.

The review-bearing states exist because a comp can be usable but still require analyst attention. For example, a comp may have enough adjusted evidence to support a modeled value while carrying higher adjustment burden, mixed property characteristics, or other review context.

Some candidates can be discovered and scored but not included in final value. Discovery answers "is this a possible comparable?" Final-value inclusion answers "can this comp support a defensible adjusted roll value for this subject?"

## 9. Adjustment Monetization Logic

Adjustment values are part of the evidence package and final-value support. Packet reporting exposes them for analyst review but does not change the adjustment formulas.

Current monetized or scaffolded categories, where source data supports them:

- living area / GLA;
- age or effective age;
- full bath;
- half bath;
- stories;
- pool;
- quality;
- condition.

Non-monetized or guardrail-only at the MVP stage:

- bedroom difference;
- land/site difference unless a future land schedule is explicitly added and validated.

Adjustment fields:

- signed adjustment amount: direction and amount applied to a comp;
- total adjustment amount: signed sum of available line-item adjustments;
- total absolute adjustment burden: sum of absolute adjustment magnitudes;
- line item count: number of adjustment line items exposed for a comp.

Packet values should distinguish:

- `not_applicable`: the category is not used or not monetized for that comp/context;
- `unavailable_in_source_artifact`: the category could not be recovered from the source evidence.

Fort Bend bathroom fields use the validated fallback/source behavior already implemented in packet tooling. Blank Fort Bend bath fields in review-visible packet rows should be treated as a reporting/data issue, not silently accepted.

## 10. Governance And Routing: Decision Rules And Packet Modes

Governance ties together candidate evidence, adjustment quality, support status, final-status transitions, unsupported transitions, comp collapses, and materiality.

The packet modes below are the durable MVP routing vocabulary.

### `governed_rerank_ready`

Meaning: `simple_value_tier_rerank` produced a model-backed value reduction that is materially better than `similarity_top_100` and passed safety checks.

Used when:

- rerank is model-backed;
- rerank is materially better than baseline;
- no unsupported transition exists;
- no included-comp collapse exists;
- no final-status downgrade exists;
- comp evidence is recoverable and packet-visible.

Analyst action: eligible for first-order analyst review.

Can be used without human review: no. It is MVP pilot-ready, not production-autonomous.

Key caveat: this is a rerank win only after governance, not raw rerank output.

### `baseline_support_only`

Meaning: `similarity_top_100` supports a model-backed reduction, but `simple_value_tier_rerank` did not add material governed benefit.

Used when:

- baseline produces a material, supportable reduction;
- rerank is not materially better;
- no safety blocker requires hold-out;
- baseline comp evidence is available for packet review.

Analyst action: review as baseline unequal-roll support.

Can be used without human review: no.

Key caveat: these are not rerank wins and should not be described as rerank-generated savings.

### `spot_check_only`

Meaning: evidence appears close to pilot-ready but carries a narrow review concern that should be sampled before broad use.

Used when:

- model-backed benefit exists;
- safety blockers are absent;
- but evidence quality, segment posture, or comp-change pattern warrants spot-check review.

Analyst action: include in QA appendix or sample review queue.

Can be used without human review: no.

Key caveat: spot-check cases should not inflate first-pilot-ready counts.

### `analyst_review_only`

Meaning: there may be usable value support, but the case is not clean enough for first-pilot routing.

Used when:

- evidence is mixed;
- similarity or comp quality is marginal;
- a downgrade or other review-heavy signal requires analyst judgment;
- segment history is not clean enough for automated pilot routing.

Analyst action: route to an analyst-only queue.

Can be used without human review: no.

Key caveat: analyst-review cases are not automation-ready.

### `hold_out`

Meaning: case should be withheld from first pilot use until more evidence or a targeted fix exists.

Used when:

- a true downgrade exists and should not enter the first pilot;
- county or segment behavior is not stable enough;
- evidence is too mixed to classify as analyst-review-only.

Analyst action: hold out from pilot output, optionally inspect for research.

Can be used without human review: no.

Key caveat: hold-out is not necessarily harmful; it means not ready.

### `fallback_safety_blocked`

Meaning: the case is blocked by safety or evidence concerns.

Used when there are:

- true transitions to unsupported,
- included-comp collapses,
- harmful segment evidence,
- diagnostic/provisional-only apparent gains,
- missing or insufficient comp evidence,
- wrong-tax-year comp evidence,
- severe similarity deterioration,
- or other conditions that make the value hard to defend.

Analyst action: do not use without further diagnosis.

Can be used without human review: no.

Key caveat: some cases may later move with better evidence, but no current packet action should be taken.

### `no_reduction_no_action`

Meaning: neither governed rerank nor `similarity_top_100` produced a material defensible value reduction under current rules.

Used when:

- baseline reduction is zero, negative, or below materiality;
- rerank does not add material governed benefit;
- no safety-blocked routing is required.

Analyst action: no action for production-shaped packet output. For targeted requests, expose comp evidence so analysts can understand why no action was taken.

Can be used without human review: yes, for no-action routing only; no value recommendation is made.

Key caveat: no-action does not mean no comps were reviewed.

## 11. Final Value Methodology

The final value formula is:

`median_of_adjusted_appraised_values`

The model opinion value is the median of included comparable properties' adjusted appraised values.

The included comp set has already passed discovery, scoring, adjustment-support, and governance checks appropriate to its packet mode.

The model does not use:

- subject living area x median adjusted value/SF as the final value,
- a weighted value/SF shortcut,
- or raw value/SF as the final value formula.

Adjusted value/SF is shown in packets as a diagnostic reasonableness check. It helps analysts understand whether the selected comps make sense, but it is not the final-value calculation.

Packet methodology guardrails:

- `final_requested_value_formula = median_of_adjusted_appraised_values`
- `raw_psf_diagnostic_only_flag = true`
- `weighted_psf_shortcut_used_flag = false`

This aligns the packet presentation with the roll-based unequal appraisal standard: median appraised value of comparable properties, appropriately adjusted.

Example from 23 Serina Ln:

| Field | Value |
|---|---:|
| Model opinion value | `$520,209.30` |
| Final value formula | `median_of_adjusted_appraised_values` |
| Median adjusted value/SF | `$178.40` |
| Subject living area x median adjusted value/SF | `$511,472.80` |
| PSF cross-check difference | `$8,736.50` |

The PSF cross-check difference is diagnostic only. It does not replace the model opinion value.

## 12. Rerank Policy

`simple_value_tier_rerank` is evaluated by default for MVP packets.

The rerank output is retained only when it is:

- model-backed,
- materially better than `similarity_top_100`,
- free of unsupported transitions,
- free of included-comp collapse,
- free of final-status downgrades,
- and supported by reviewable comp evidence.

The rerank is not the unconditional final value default. If rerank does not add material governed benefit, baseline support remains valid when baseline evidence supports a reduction.

Future work should not add bedroom, land, frontage, depth, lower-value bonus, strong land mismatch, or adjustment-burden ranking penalties without evidence from analyst review or validation artifacts.

## 13. Baseline Support Policy

`similarity_top_100` remains the baseline unequal-roll support engine.

`baseline_support_only` means:

- baseline supports a model-backed value reduction,
- rerank was tested,
- rerank did not materially improve the baseline,
- and the packet should present baseline support, not rerank lift.

Baseline-support value is not incremental rerank value. It should be reported as similarity-baseline reduction support.

## 14. No-Action Policy

`no_reduction_no_action` does not mean no comps were reviewed.

It means no material defensible reduction was found under current MVP rules.

For targeted property packets, no-action cases should still expose:

- selected comp evidence,
- `No_Action_Review`,
- `No_Action_Opinion_Of_Value`,
- `No_Action_Subject_Comp_Grid`,
- and `No_Action_Comp_Details`.

This lets analysts understand why the workflow did not promote the case without changing the classification.

## 15. Safety And Governance Guardrails

The workflow must block or route cases away from action when there are:

- unsupported transitions,
- included-comp collapses,
- harmful segment evidence,
- diagnostic/provisional-only wins,
- insufficient evidence,
- retained downgrades requiring review,
- wrong-tax-year comp evidence,
- severe similarity deterioration,
- weak or non-material value benefit.

These guardrails protect defensibility. They should not be bypassed to inflate packet counts.

## 16. Analyst Packet Evidence

The MVP packet should expose the evidence an analyst needs to understand, approve, reject, or hold a case.

Packet-visible evidence includes:

- subject facts: address, account, county, neighborhood, appraised value, living area, value/SF, year built, land size, beds, baths, stories, subdivision, quality, and condition where available;
- comp facts: account, address, tax year, appraised value, living area, value/SF, adjusted value, adjusted value/SF, neighborhood, subdivision, land size, year built, beds, baths, stories, quality, condition, and hydration status where available;
- selected comps;
- membership: `overlap`, `smart_only`, or `rerank_only`;
- adjustment line items where recoverable;
- total adjustment amount and total absolute adjustment burden;
- opinion of value;
- PSF cross-check;
- final value formula and methodology guardrails;
- no-action evidence for targeted runs;
- analyst signoff fields.

For no-action targeted packets, the analyst-facing grids should use neutral `Selected Comp` headers while preserving membership as a row or field. This prevents the packet from implying that rerank created a no-action value.

## 17. Analyst Review Workflow

Recommended analyst review order:

1. `governed_rerank_ready`
2. `baseline_support_only`
3. `spot_check_only`
4. `analyst_review_only`
5. `hold_out`, `fallback_safety_blocked`, and `no_reduction_no_action` only for QA, diagnostics, or targeted explanation.

Expected analyst decisions:

- `approve`
- `approve_with_note`
- `reject_comp_quality`
- `reject_subject_data`
- `needs_more_review`
- `hold_out`

For comp review:

- review `rerank_only` comps first when a rerank case is present;
- review `smart_only` comps second;
- review `overlap` comps as context;
- for no-action grids, use the neutral `Selected Comp` columns and inspect the membership row.

Analysts should mark subject-level issue columns yes/no. If multiple comps have issues, list comp account numbers and reasons in notes.

## 18. Known MVP Limits And Production Readiness Boundaries

Current status:

- no-persist governed workflow,
- analyst packet generation,
- no automatic production final-value writing,
- no automatic protest filing,
- no DB writes as part of validation or reporting,
- no runtime default changes,
- no production scoring or final-value changes;
- no unconditional rerank default;
- no land monetization schedule yet;
- bedrooms are not monetized;
- analyst signoff is still required.

Production readiness requires analyst acceptance data from real packet review. Positive modeled value reduction alone is not enough.

## 19. Revision And Change Control

This contract can be revised when supported by evidence.

Valid reasons include:

- repeated analyst rejection patterns,
- recurring comp quality issues,
- county or segment-specific failures,
- data quality findings,
- validated threshold changes,
- validated model improvements,
- validated reporting or packet usability findings.

Future changes should document:

- reason for change,
- evidence or artifacts reviewed,
- expected impact,
- tests and checks run,
- guardrail confirmation,
- whether packet counts or value framing changed.

Changes to scoring, adjustment monetization, final-value methodology, materiality thresholds, or governance thresholds require explicit evidence and should not be bundled into unrelated packet-presentation work.

Changes should not silently:

- make raw rerank unconditional,
- hide baseline support,
- suppress no-action evidence in targeted review packets,
- change final value methodology,
- change production defaults,
- or add penalties without evidence.

## 20. Related Evidence

Current validation evidence summarized at the time of this contract:

| Metric | Value |
|---|---:|
| True full-pool packet coverage | `800` subjects |
| `governed_rerank_ready` | `88` |
| `baseline_support_only` | `223` |
| `no_reduction_no_action` | `382` |
| `fallback_safety_blocked` | `86` |
| Governed rerank value | `$329,178.01` |
| Baseline support value | `$7,253,994.15` |

Baseline support value is similarity-top-100 baseline reduction support. It is not incremental rerank lift.

## 21. Developer Guardrails

Future work should not drift into:

- adding more penalties without evidence,
- making raw rerank unconditional,
- changing the final value formula without explicit approval,
- hiding baseline support,
- labeling baseline support as rerank savings,
- suppressing no-action evidence in targeted review packets,
- removing analyst review from pilot workflows,
- writing production values from no-persist reporting artifacts.

The MVP workflow should remain simple, explainable, analyst-reviewable, and no-persist until production readiness is supported by analyst acceptance evidence.
