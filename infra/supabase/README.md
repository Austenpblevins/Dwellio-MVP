# Supabase Notes

This folder is reserved for Supabase deployment and local runtime notes.

Stage 0 expectations:
- PostgreSQL remains canonical backing store
- PostGIS is enabled by migration `0001_extensions.sql`
- migration execution is controlled by ordered SQL in `app/db/migrations/`
