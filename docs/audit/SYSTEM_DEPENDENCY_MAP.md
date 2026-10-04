# System Dependency Map

```text
County configuration and source files
        -> ingestion services and registered jobs
        -> 77 SQL migrations / canonical and derived data
        -> service modules and read models
        -> FastAPI public and admin routes
        -> Next.js public and protected admin pages
```

## Primary dependency observations

| System | Repository evidence | Audit conclusion |
| --- | --- | --- |
| Public property flow | Search, parcel, quote, and lead routes; web public API calls use `/search`, `/quote`, and `/lead` | A public property/lead path exists. |
| Instant quote | `GET /quote/instant/{county_id}/{tax_year}/{account_number}` and `app/services/instant_quote.py` | Backend capability exists, but the web client does not call the instant route. |
| Admin operations | Admin routes, service modules, and pages for readiness, source files, validation, jobs, leads, cases, and packets | Meaningful internal operator foundations exist. |
| Evidence packets | Case/packet tables and admin review pages | Review foundations exist; generation is incomplete. |
| Unequal roll | migrations `0067`–`0079`, services, jobs, and validation/evidence scripts | Substantial governed foundation; no corresponding public or admin route surface was found. |

## Dependency boundaries

- A table, migration, or service alone does not prove an end-to-end workflow.
- The packet refresh job is registered but contains a TODO; the packet generator
  returns `not_implemented`.
- The current web API helper calls normal quote endpoints, not the separate
  instant-quote endpoint. Customer-visible integration is therefore unverified.
- Database-touching verification was deliberately not run. In particular, this
  audit does not bypass the Stage 21 isolated-development-DB safeguards.
