# Safe Data Cleaning with Supabase and Pandas

This plan outlines a safe, step-by-step approach for cleaning business data stored in Supabase using Python and pandas, with a focus on data integrity and minimizing risk to the production database.

## Steps
1. Identify Supabase table names and schema (columns, types).
2. Export data from Supabase to pandas DataFrame for analysis and cleaning.
3. Perform data cleaning in pandas (remove duplicates, fix inconsistencies, validate formats, etc.).
4. Validate cleaned data thoroughly before any database write-back.
5. (Optional) Write cleaned data back to Supabase, using safe update/insert practices:
   - Always use WHERE clauses for updates/deletes
   - Test queries with SELECT first
   - Use transactions if possible
6. Maintain backups and version control for all scripts and data exports.

## Relevant files
- `.env` — Store Supabase credentials securely
- `test_supabase.py` — Example for connecting and querying Supabase
- Python scripts live in the project root (or another non-copilot code directory)
- `copilot/` is reserved for markdown planning and notes only

## Verification
1. Confirm data export matches Supabase source before cleaning
2. Validate cleaned data with summary statistics and spot checks
3. Test update queries with SELECT before running
4. Ensure .env is not committed to version control

## Decisions
- Use environment variables for credentials
- Only write back to Supabase after thorough validation
- Keep destructive operations (update/delete) to a minimum and always with WHERE clauses

## Further Considerations
1. Consider using a staging table or database for testing updates
2. Automate regular backups before bulk operations
