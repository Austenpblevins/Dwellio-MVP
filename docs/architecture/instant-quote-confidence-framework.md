# Instant Quote Confidence Framework

Internal confidence is deterministic and kept off the public API.

Starting score:
- `100`

Penalties:
- neighborhood-only fallback
- weak segment support
- noisy segment or neighborhood variance
- missing year built
- incomplete parcel summary readiness
- effective tax rate sourced from fallback component rollup
- material cap/freeze tax-limitation risk
- large subject-to-target PSF gap

Labels:
- `85-100` -> `high`
- `65-84` -> `medium`
- below `65` -> `low`

Public mapping:
- expose only `estimate_strength_label`
- suppress numeric range when confidence drops below the public-safe threshold

Tax-limitation handling:
- if tax protections are present but public output is still safe, show a constrained range with a plain-language note
- if protections make a public numeric range too uncertain, return `supported=false` with a refined-review CTA
- current implementation treats a homestead cap gap of `>= 3%` between `assessment_basis_value` and `capped_value` as a material cap-limitation signal
- homestead alone without a material cap gap does not constrain the public range
