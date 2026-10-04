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

## Approved authority hierarchy — October 3, 2026

The project owner approved separating implementation reality from future
product/design intent:

1. Code and ordered migrations supply evidence of repository implementation.
2. `docs/architecture-state.md` records current implementation status. It
   remains provisional until reverified against that evidence.
3. Approved product/design documents, including individually confirmed
   `docs/source_of_truth/` material, describe intended design and future work.
4. Runbooks describe operational procedures; historical/final summaries and
   audit artifacts provide supporting evidence.

Migrations are schema implementation authority. The consolidated
`sql/dwellio_full_schema.sql` remains an unverified reference pending origin
and synchronization checks. Repository evidence does not establish deployed
database state.

This approval resolves the hierarchy decision, but does not itself rebaseline,
relabel, supersede, or archive other documents. Maintenance ownership and
refresh processes still need to be established where unspecified. See
[the full decision record](HUMAN_REVIEW_QUEUE.md#approved-decisions--october-3-2026).
