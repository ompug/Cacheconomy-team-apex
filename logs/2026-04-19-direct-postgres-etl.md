# Direct Postgres ETL Log

Date: 2026-04-19

## Summary

Replaced the large-table Supabase REST sync path with a direct Postgres ETL using `psycopg`.

The new pipeline now:

- connects with `SUPABASE_DB_URL`
- streams `companies_data` with a server-side cursor
- reuses `merge_duplicates.py` for deduplication
- loads cleaned rows into a staging table with Postgres `COPY`
- replaces `Cleaned_companies_data` atomically in a transaction

## Why This Change Was Needed

The prior REST-based fetch/insert flow hit PostgREST statement timeouts on the large source table and was not reliable for a full-table sync.

The direct Postgres ETL avoids those REST timeouts and completed successfully against the live dataset.

## Implementation Notes

- Added `psycopg` / `psycopg-binary` to `requirements.txt`
- Rewrote `sync_cleaned_pipeline.py` to use direct Postgres instead of REST fetch/insert
- Added session-level timeout handling with `SET statement_timeout = 0`
- Used a temporary staging table plus `TRUNCATE` + `INSERT ... SELECT` for replace-all behavior
- Updated `README.md` with `SUPABASE_DB_URL` and the direct Postgres workflow

## Issues Encountered

### 1. Connection string problems

- Direct connection initially failed because the environment did not have IPv6 reachability
- Switched to the Supavisor session pooler on port `5432`
- Pooler auth initially failed because the username format needed to be `postgres.<project-ref>`
- Password formatting also needed to be corrected in the URL

### 2. Destination table schema issue

The destination table `public."Cleaned_companies_data"` existed, but Postgres reported that it had zero columns.

That prevented the ETL from loading data into it. The table was recreated from the source schema so the ETL had a valid destination shape.

## Final ETL Result

- Source table: `public.companies_data`
- Destination table: `public."Cleaned_companies_data"`
- Source row count: `787,564`
- Merged output row count: `782,252`
- Destination row count after refresh: `782,252`

The cleaned data was successfully written to `public."Cleaned_companies_data"`.

## Duplicate Counts Explained

The current merge logic defines duplicates using exact matches on all three fields:

- `companyName`
- `streetAddress`
- `postalCode`

Using that rule, the ETL run found:

- `9,170` duplicate rows
- `3,858` duplicate groups
- `5,312` rows eliminated after merging groups down to one row each

This is smaller than the earlier rough `300,000+` duplicate figure discussed during troubleshooting.

That larger figure likely came from an earlier exploratory estimate or from a different duplicate definition, not from the final ETL rule currently implemented in `merge_duplicates.py`.

## Important Note About Table Visibility

The destination table name is mixed case:

- `public."Cleaned_companies_data"`

This is different from lowercase `public.cleaned_companies_data`.

If the Supabase UI appears empty, make sure the exact mixed-case table is selected in the `public` schema.
