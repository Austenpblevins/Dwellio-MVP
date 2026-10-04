# Human Review Queue

The project owner approved all nine recommendations on October 3, 2026.
The original questions below are retained for traceability. The decisions
recorded afterward resolve the first Human Review Gate; they do not establish
production readiness or complete the approved follow-up work.

1. **Documentation authority hierarchy:** Which document family controls design,
   implementation status, historical summaries, and operating instructions, and
   who is responsible for keeping each current?
2. **Unequal-roll production status:** Should all existing unequal-roll work
   remain `GOVERNED_NOT_PRODUCTION`, and which specific subset, if any, may be
   proposed for a separately approved integration project?
3. **Instant quote frontend integration:** Is the separate backend instant quote
   route intended for the current public web experience, and what UX, safety,
   data-readiness, and approval criteria must be met before wiring it in?
4. **Packet terms and refresh stub:** What does “packet” mean operationally,
   and should the packet-refresh job/generator be completed, retained as a
   clearly labeled foundation, or superseded?
5. **Job CLI repair authorization:** May a separate change repair the duplicate
   argument registration, with parser-level tests and no job behavior changes?
6. **`sql/dwellio_full_schema.sql` status:** Is it generated from migrations,
   manually maintained, a reference snapshot, or obsolete? Who owns it and how
   should it be refreshed?
7. **Evidence retention:** Which remediation, validation, rollout, and audit
   artifacts must be retained, where should they be indexed, and what may ever
   be superseded?
8. **Customer workflow classifications:** Which customer account, agreement,
   e-sign, invoice, payment, and filing concepts are only schema foundations,
   which are planned, and which are approved product commitments?
9. **Repair priorities:** What is the priority and owner for the broken CLI,
   lint/correctness debt, and the frontend test-runner situation before broader
   feature work proceeds?

## Approved decisions — October 3, 2026

1. **Documentation authority:** Separate implemented repository reality from
   approved product/design intent. Use `docs/architecture-state.md` for current
   implementation status, approved product/design documents for future intent,
   and runbooks for operations. The ledger remains provisional until reverified
   against code and migrations; approval of the hierarchy is not verification
   of existing claims.
2. **Unequal roll:** Retain all existing work as `GOVERNED_NOT_PRODUCTION`.
   Any named subset proposed for production integration requires a separate
   approval. No experiments are declared historical by this decision.
3. **Instant quote:** Document the current capability as backend-only and
   end-to-end `PARTIAL`. Public frontend integration is deferred and must be
   planned separately; this does not authorize wiring it into the public UI.
4. **Packets:** Retain and clearly label the packet foundations and refresh/
   generator stubs. Final PDF generation and county filing remain separate
   future projects; no completion or removal is authorized here.
5. **Job CLI:** Authorize a narrow, separate repair of duplicate argument
   registration, with parser regression tests and no changes to job behavior.
6. **Schema authority:** Ordered migrations are authoritative evidence of
   repository schema implementation. Treat `sql/dwellio_full_schema.sql` as an
   unverified reference until its origin and synchronization are confirmed.
   Its generation method, refresh process, and maintenance owner remain to be
   established; this decision does not assert any deployed database state.
7. **Evidence retention:** Retain and index remediation, validation, rollout,
   and audit evidence. Archiving or deleting specific paths requires separate,
   path-specific approval. No evidence is approved for removal.
8. **Customer workflows:** Use `STUB` for schema foundations and `UNKNOWN`
   where no executable path is established for accounts, agreements, e-sign,
   invoices, payments, and filing. Record approved roadmap commitments
   separately; these labels do not create new product commitments.
9. **Repair priorities:** Address the CLI first, correctness-related lint
   second, and the frontend test runner third, as separate changes. The owner
   approved these repair recommendations; no repair is implemented by this
   decision-recording change. No durable maintenance owner was assigned.

## Follow-up boundary

Record and preserve these decisions, then rebaseline the implementation ledger
against repository evidence. Perform the approved repairs in separate changes.
The second, path-specific cleanup approval remains outstanding. Production
integration, final packet generation, filing, and public instant-quote UI work
remain separately scoped future projects.
