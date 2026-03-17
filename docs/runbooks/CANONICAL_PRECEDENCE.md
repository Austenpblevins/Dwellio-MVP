# Canonical Precedence

Use this file to resolve instruction conflicts across the Dwellio repository and imported planning documents.

## Order of authority

1. `docs/source_of_truth/*`
2. Actual repository schema and implementation files
   - `app/db/migrations/*`
   - `app/db/views/*`
   - `app/models/*`
   - `app/services/*`
   - `app/jobs/*`
   - `app/api/*`
3. `docs/architecture/*`
4. `docs/codex/*`
5. Imported or legacy planning documents

## Required conflict rules

### Backend authority
Dwellio is **Python-first** for backend and ETL implementation.

- Backend/API authority: Python
- ETL authority: Python
- Database authority: PostgreSQL / Supabase
- Frontend: React / Next.js may be used as a frontend layer only

If any imported planning document suggests a TypeScript or Next.js backend, treat that as non-canonical unless it is explicitly adopted in `docs/source_of_truth/*`.

### Search authority
The canonical public search path is:
- `v_search_read_model`
- `GET /search?address={query}`

Do not create a second canonical search architecture unless formally approved.

### Quote authority
The canonical public quote path is:
- `v_quote_read_model`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`

Do not substitute a different public quote route shape in implementation or Codex prompt files.

### Schema authority
Conceptual or imported table names must map into the actual repo schema. Avoid creating duplicate canonical table families when an existing repo table already serves the same purpose.

### Migration authority
Only ordered migration files in `app/db/migrations/` are executable migrations.
The full schema convenience file belongs outside the migrations folder.
