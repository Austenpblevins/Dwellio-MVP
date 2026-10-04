# Repository Stabilization Audit Summary

## Result

The local audit baseline is recorded on `repo-stabilization`, created directly
from `7cce26676c55ee1ff6fcecd4001c74e89eeb2264`. This change is documentation
and audit only.

## Verified inventory

- 32 mounted application handlers
- 60 service modules
- 17 registered jobs
- 48 operational scripts
- 77 migrations
- 19 frontend pages

## Major findings

1. The registered job CLI has duplicate argument registration and fails during
   parser construction at the audit baseline. A separate October 3 repair
   now passes 26 CLI tests and module help; no job implementation changed.
2. `docs/architecture-state.md` is stale relative to migration `0079`; it now
   carried a temporary `REBASELINING / NOT YET VERIFIED` warning at the audit
   baseline; the October 3 rebaseline below replaces that warning.
3. Documentation authority overlaps across status, source-of-truth, architecture,
   runbook, and summary documents.
4. Packet review foundations exist, but the refresh and final packet/PDF
   generation path is incomplete.
5. Instant-quote backend functionality exists; the current web API client uses
   normal quote endpoints, so the complete customer experience is unverified.
6. Customer/agreement/invoice schema support does not prove onboarding, e-sign,
   billing, payment, or filing workflows.
7. Unequal-roll work is substantial but has no corresponding route surface in
   the inspected API or web application. It remains classified
   `GOVERNED_NOT_PRODUCTION` pending a separately approved integration project.

## Human review outcome — October 3, 2026

The project owner approved all nine recommendations. The decision record is
in [HUMAN_REVIEW_QUEUE.md](HUMAN_REVIEW_QUEUE.md#approved-decisions--october-3-2026).
The first Human Review Gate is resolved. Approval does not repair the CLI,
verify the architecture ledger, establish production readiness, or authorize
cleanup.

## Next boundary

The implementation ledger was rebaselined on October 3, 2026 against repository
commit `a5ca4cc` through migration `0079`, using the approved authority hierarchy
and capability classifications. See [architecture-state.md](../architecture-state.md)
for evidence and verification limits. Static review and isolated parser
construction were performed; runtime, database, lint and frontend execution
remain outstanding. No application changes were made.

The separately approved CLI repair is complete: 26 regression tests pass
and module help succeeds. Job dispatch was mocked and no database job ran.
Correctness lint triage is also complete: all three undefined-name findings
were repaired, 47 focused tests passed, and the focused Ruff correctness check
passed. Full lint still has 149 reviewed maintenance/style findings; see
[DISCREPANCY_REPORT.md](DISCREPANCY_REPORT.md#correctness-related-lint-triage--october-3-2026).
Frontend test-runner setup is complete: 9 native Node tests, TypeScript checking,
ESLint and production build pass. No additional runner dependency was added.
Stale optional-attribution and CLI tuple expectations were aligned with existing
behavior; blank-email rejection coverage was added.

The full Python suite passes **754 tests** with the isolated Stage 21 database,
including temporary-schema migration checks. Migrations `0067`–`0080` were
applied to `stage21_dev` on port `55442` with owner authorization. No migrations
remain pending. Forward repair `0080` resolves the cross-schema constraint-name
collision revealed after applying `0079`, covering all 12 affected constraints
without changing historical SQL. A subsequent read-only seeded baseline slice
completed eight county/year cases (five manual-review-required, one supported-
with-review, two unsupported) plus a correctly blocked missing-account control.
Median/evidence and safe-value checks passed; one fallback case took 33.35 seconds.
The subsequent simple-rerank comparison is recorded below. Persisted end-to-end
workflows and representative acceptance/performance remain unverified.
See [DISCREPANCY_REPORT.md](DISCREPANCY_REPORT.md#replay-issue-resolved-by-forward-repair--october-3-2026)
for the verification history and repair evidence.

## Preservation and remaining boundaries

The owner authorized reviewing, committing and pushing the frontend setup,
verification changes and replay repair to the existing `repo-stabilization`
branch. No new branch is needed. These changes do not approve production rollout
or deployment to a shared database.

Retain evidence and require path-specific approval before archival or deletion.
Public instant-quote integration, final packet PDF generation and county filing
remain separately scoped future projects. Full Python lint retains the reviewed
maintenance/style backlog described above.


Path-specific cleanup proposals are recorded in `CLEANUP_CANDIDATES.md`: two
tracked Finder metadata files and one no-op unit test are proposed for deletion;
the root legacy architecture overview has an optional archival proposal.
Meaningful fixture tests, linked historical docs, migrations, adapters and
remediation/governance evidence are retained. No cleanup is performed before
owner approval; these verification/review updates remain local.


The owner subsequently approved the three minimal cleanup deletions: root/app
Finder metadata and the no-op unit placeholder test. Those paths are deleted
locally; optional legacy-document archival remains pending. No behavioral test
coverage was removed, and no new branch, commit or push was made.

## Seeded simple-rerank comparison and analyst handoff — 2026-10-03

Compared eight nonrandom 2026 subjects (four per county, one per selected
neighborhood) using full neighborhood candidate pools and three selections:
current-order cap, similarity top 100, and simple value-tier rerank. All 24
replays completed read-only against isolated Stage 21 data. Requested and served
years matched; included-comp detail counts matched reported counts.

Governance classified seven cases `not_eligible_low_benefit` and one
`blocked_case` for severe similarity deterioration. The blocked Harris account
`0390890000035` had a raw additional roll-value reduction of $16,034.27;
governance correctly retained the similarity baseline. All eight cases fell
back, leaving zero governed additional reduction in this diagnostic sample.
These amounts are appraised roll-value reductions, not estimated tax savings.

An analyst workbook and companion JSON contain eight blank review decisions
and 317 included-comp evidence rows across baseline and rerank. The handoff
emphasizes comp quality, subject data and adjustment support. The sample does
not establish representative performance, analyst acceptance, persisted
pipeline behavior, or production readiness. Unequal roll remains
`GOVERNED_NOT_PRODUCTION`; actual analyst review is the next gate.

## Stabilization closeout

The owner authorized committing and pushing the verification notes and three
approved cleanup deletions to the existing `repo-stabilization` branch. The
post-cleanup Python suite passed all 753 tests against the isolated Stage 21
configuration; `git diff --check` passed. Earlier local-only statements above
describe the state at each verification stage. No new branch was created.
Analyst acceptance and production promotion remain separate follow-up gates.
