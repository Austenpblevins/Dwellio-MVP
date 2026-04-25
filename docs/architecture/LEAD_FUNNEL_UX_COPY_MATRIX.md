# Lead Funnel UX Copy Matrix

This document closes `S5-T2` for the current repo shape.

It defines the approved user-facing copy and CTA behavior for each Stage 5 funnel state.

Scope:

- parcel-page quote-to-lead experience
- soft-gated lead capture card
- success messaging after `POST /lead`

Out of scope:

- represented-customer onboarding
- agreements, billing, or filing language
- advanced equity valuation messaging

## Global rules

- Do not promise representation, filing, savings, or outcomes from lead capture alone.
- Do not describe instant quote as the protest-grade valuation engine.
- Keep fallback year disclosure explicit whenever `served_tax_year != requested_tax_year`.
- Unsupported business states should still allow honest email capture.
- `system_or_config_failure` must stay visually distinct from unsupported-demand states.

## Approved matrix

| Funnel state | Eyebrow | Title | Body copy | Primary CTA | Secondary CTA | Lead card title | Lead card description | Success message rule |
|---|---|---|---|---|---|---|---|---|
| `quote_ready` | `Instant protest signal` | `See the protest value Dwellio would start from.` | `This public quote is read from precomputed parcel-year data so the page stays fast, consistent, and explainable.` | `Email my protest snapshot` | `See how the value was supported` | `Email yourself the quote and next steps` | `Email is required so we can send the parcel-year snapshot. Phone and first name stay optional for this soft gate.` | Confirm that the parcel-year quote context was saved and Dwellio can follow up from the quote-safe record. |
| `missing_quote_ready_row` | `Quote refresh pending` | `Parcel facts are available, but the quote-ready row is not yet published.` | `Dwellio only serves precomputed public quotes. When a quote-safe row is missing, we keep the flow open so you can leave an email instead of hitting a dead end.` | `Email me when the quote is ready` | `Review parcel facts` | `Get notified when this parcel's quote is ready` | `We'll save your parcel-year context and send an update when a quote-safe read-model row is available.` | Confirm that the parcel-year context was saved and Dwellio will follow up when the quote-ready row is published. |
| `unsupported_property_type` | `Property type` | `This property is outside the current instant-quote scope.` | `The MVP public funnel is limited to single-family residential parcels. You can still leave your email for a manual follow-up as broader property support comes online.` | `Request manual follow-up` | `Review parcel facts` | `Stay in the loop for broader property support` | `Email is enough for now. We'll keep your parcel-year context and reach out when this property type is supported.` | Confirm that the contact request was saved and Dwellio will follow up when broader property support is available. |
| `unsupported_county` | `County rollout` | `This county is not in Dwellio's public quote scope yet.` | `Dwellio's MVP public flow currently supports Harris and Fort Bend single-family homes. You can still leave your email so we can notify you when coverage expands.` | `Get county updates` | `See current coverage` | `Join the county rollout list` | `Leave an email and we will let you know when this county is available for public quote requests.` | Confirm that the contact request was saved and Dwellio will reach out when the county opens. |
| `system_or_config_failure` | `Temporary issue` | `Dwellio couldn't load a trustworthy quote right now.` | `This looks like a configuration, readiness, or service issue rather than an unsupported property or county. Please try again shortly.` | `Try again` | `Return to search` | `Lead capture temporarily unavailable` | `Don't collect a lead from a failed system state unless a later explicit fallback flow is approved.` | No success state; keep the failure explicit and do not silently translate it into unsupported demand. |

## CTA behavior rules

### `quote_ready`

- Primary CTA expands the lead card.
- Secondary CTA scrolls or jumps to the explanation/value-support section on the parcel page.
- Detailed quote content remains visible by default.

### `missing_quote_ready_row`

- Primary CTA expands the lead card.
- Secondary CTA keeps the user on parcel facts; it must not imply a hidden quote exists.
- Quote-specific numeric language should stay absent.

### `unsupported_property_type`

- Primary CTA expands the lead card for manual follow-up interest.
- Secondary CTA keeps the parcel summary available.
- Copy must frame this as a current scope boundary, not a parcel defect.

### `unsupported_county`

- Primary CTA expands the lead card for county rollout interest.
- Secondary CTA points back to current coverage messaging, not to a fake quote path.
- Copy must keep Harris + Fort Bend + SFR scope explicit.

### `system_or_config_failure`

- Primary CTA retries the current page or request path.
- Secondary CTA returns the visitor to search or the last stable public page.
- Do not show a quote-style lead CTA in this state.

## Fallback disclosure rule

When a page serves fallback-year data:

- show `requested_tax_year` and `served_tax_year`
- explain that the displayed result is from the nearest supported prior year
- keep the CTA copy unchanged unless the business state itself changes

Fallback is a freshness/state disclosure, not its own funnel state.

## Current implementation mapping

The current web app already implements the approved copy for:

- `quote_ready`
- `missing_quote_ready_row`
- `unsupported_property_type`
- `unsupported_county`

Current repo evidence:

- `apps/web/app/_lib/lead-funnel.ts`
- `apps/web/app/_components/LeadCaptureCard.tsx`
- `apps/web/app/_lib/lead-capture-action.ts`

`system_or_config_failure` is approved here as a route/page-level UX rule, but it is not yet emitted through the current lead capture status contract.
