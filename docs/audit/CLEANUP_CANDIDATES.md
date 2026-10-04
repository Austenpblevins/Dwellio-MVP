# Cleanup Candidates

This is a path-specific approval proposal, not authorization to delete, move
or archive files. The owner approved documentation hierarchy and repair work;
the owner subsequently approved the first three deletions listed below.
The optional archival remains pending.

## Closed prerequisites

The architecture ledger is rebaselined through `0080`. The CLI repair,
correctness-related lint triage, frontend test runner and schema replay repair
are complete. Unequal-roll remains `GOVERNED_NOT_PRODUCTION`, packet stubs are
retained, and migrations are schema implementation authority. The consolidated
SQL remains an unverified reference. Evidence must be retained and indexed.

## Proposed paths — October 3, 2026

| Exact repository path | Proposed action | Evidence and impact | Approval status |
| --- | --- | --- | --- |
| `.DS_Store` | Remove from version control and repository. | Tracked macOS Finder metadata. `.gitignore` already excludes `.DS_Store`; no code dependency. | Approved and deleted |
| `app/.DS_Store` | Remove from version control and repository. | Same tracked Finder metadata; no executable content. | Approved and deleted |
| `tests/unit/test_unit_placeholder.py` | Delete. | Entire file is `def test_placeholder(): assert True`; no behavior verification. Deletion reduces collected test count by one without removing a meaningful check. | Approved and deleted |
| `dwellio_architecture_diagram_and_index (2).md` | Move to `docs/archive/dwellio_architecture_diagram_and_index.md`; add historical/reference notice and link to current ledger. | Unlinked root architecture overview; search found no filename references. Preserve its content as historical design evidence. It presents a broader design pipeline and consolidated-schema bootstrap suggestions, rather than verified current integration status. | Pending; optional |

The owner approved the first three deletions on October 3, 2026; those exact
paths were deleted. The legacy architecture overview remains in place because
its optional archival was not explicitly selected.

## Retain in place

| Exact path or protected family | Recommendation | Reason |
| --- | --- | --- |
| `tests/integration/test_placeholder.py` | Retain; optionally rename in separately approved work. | Despite the name, it checks the Stage 15 README, workflow JSON and GIS fixture exist. |
| `docs/final_implementation_summary.md` | Retain. | Already labeled a historical Stage 16 snapshot and linked by `README.md`; it is not presented as the current status ledger. |
| `FINAL_MANIFEST.md` | Retain; consider a historical/reference notice in separate documentation maintenance. | Referenced by `docs/codex/DWELLIO_MASTER_CODEX_PROMPT.md`; its name alone does not justify removal. |
| `sql/dwellio_full_schema.sql` | Retain as unverified reference. | Generated provenance/synchronization is unresolved; migrations remain authoritative. |
| `app/db/migrations/*.sql` | Retain all. | Ordered schema history, including forward repair `0080`; never delete historical migrations for cleanup. |
| `app/county_adapters/harris/`, `app/county_adapters/fort_bend/` | Retain both. | Active county interfaces under shared ingestion framework. |
| `app/services/unequal_roll_*.py`, unequal-roll scripts/tests | Retain. | Governed tooling and experimental evidence, not dead code. |
| `app/services/packet_generator.py`, `app/jobs/job_packet_refresh.py` | Retain and keep stub classification. | Owner explicitly chose retained foundations; removal/completion is separately scoped. |
| Remediation, validation, rollout and audit artifacts | Retain and index. | Durable evidence protected by owner decision. |

Only the three approved paths were deleted. No archival, consolidation or rename
was performed. The meaningful integration fixture test remains intact; the full suite passed
753 tests after cleanup, down from 754 solely because the no-op test was removed.
The owner subsequently authorized preserving these changes by commit and push
to the existing `repo-stabilization` branch.
