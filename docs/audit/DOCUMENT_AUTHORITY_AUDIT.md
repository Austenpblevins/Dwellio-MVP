# Document Authority Audit

## Observed authority claims

- `docs/source_of_truth/CANONICAL_CONTEXT.md` describes a precedence order.
- `docs/source_of_truth/DWELLIO_MASTER_SPEC.md` and `AGENT_RULES.md` present
  the source-of-truth set as controlling implementation context.
- `docs/runbooks/CANONICAL_PRECEDENCE.md` describes canonical operational
  choices.
- `docs/architecture-state.md` calls itself the single authoritative
  implementation-status ledger, while directing design authority elsewhere.
- Multiple architecture and final-summary documents describe implementation
  using words such as “canonical,” “final,” and “reconciled.”

## Finding

The repository has useful precedence material, but the practical authority
boundary remains easy to misread: design intent, status reporting, historical
summaries, and code/migration truth are distributed across several document
families. The stale `architecture-state.md` migration reference demonstrates
the risk of treating any one narrative ledger as self-validating.

## Temporary rule during rebaseline

Until a human approves a durable hierarchy:

1. code and applied migration files are evidence of repository implementation;
2. `docs/source_of_truth/` and approved precedence documents guide intended
   design;
3. `docs/architecture-state.md` is a provisional implementation-status ledger;
4. final summaries, runbooks, and task artifacts are supporting evidence, not
   unilateral authority.

This rule does not replace existing governance; it makes the ambiguity explicit
for the rebaseline review.
