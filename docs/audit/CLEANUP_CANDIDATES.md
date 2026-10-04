# Cleanup Candidates

This list is a review backlog, not authorization to delete, move, archive, or
rewrite anything.

| Candidate | Why it needs review | Required decision |
| --- | --- | --- |
| Duplicate job CLI arguments | The CLI parser fails before executing a job. | Approve a separate, tested repair scope. |
| Architecture-state refresh | Its migration metadata stops at `0056`, while the repository has `0079`. | Approve owner, evidence standard, and update method. |
| Overlapping authority documents | Multiple files describe themselves as canonical/final/status authority. | Approve one precedence hierarchy and maintenance policy. |
| Packet refresh/generator stubs | `job_packet_refresh` is TODO and `PacketGeneratorService` returns `not_implemented`. | Decide whether to complete, retain as an explicit stub, or remove under a separate change. |
| `sql/dwellio_full_schema.sql` | It may be a useful schema artifact, but its relationship to the migration sequence is not established by this audit. | Determine owner, refresh method, and authoritative status. |
| Evidence and remediation artifacts | Validation, rollout, and remediation evidence is spread through docs and scripts. | Define retention, indexing, and supersession rules before any archival. |

## Review outcome — October 3, 2026

The first Human Review Gate is resolved; see
[the approved decisions](HUMAN_REVIEW_QUEUE.md#approved-decisions--october-3-2026).
The narrow CLI repair is authorized as a separate tested change. The authority
hierarchy is approved, the implementation ledger needs rebaselining, packet
stubs are to be retained and labeled, and the consolidated schema remains an
unverified reference. Evidence is to be retained and indexed.

These decisions do not authorize deletion, movement, or archival of any path.
Path-specific cleanup approval remains required. No cleanup or code repair
is performed in this decision-recording change.
