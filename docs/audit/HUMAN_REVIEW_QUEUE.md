# Human Review Queue

These are decisions for the project owner and appropriate operational/legal
stakeholders. This audit does not make them.

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
