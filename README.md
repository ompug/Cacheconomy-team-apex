# Cacheconomy – Team Apex

## Overview
Cacheconomy is a data-driven initiative focused on uncovering and organizing hidden economic value within local communities. This project aims to transform fragmented and outdated business data into a structured, reliable dataset that better represents the true landscape of small businesses.

Team Apex contributes by working on data analysis, organization, and validation to support the creation of a high-quality business index.

---

## Objectives
- Improve the accuracy of small business data  
- Identify and reduce inconsistencies or outdated records  
- Support the creation of a unified and reliable dataset  
- Contribute to insights that help better understand local economic activity  

---

## Our Role
Team Apex focuses on:
- Reviewing and organizing large datasets  
- Identifying patterns and inconsistencies in data  
- Supporting data validation and quality improvement  
- Collaborating as a team to ensure efficient progress  

---

## Tools & Technologies
- Python  
- Pandas  
- SQL  
- Supabase  

---

## Skills Applied
- Data Analysis  
- Problem Solving  
- Team Collaboration  
- Pattern Recognition  
- Data Validation  

---

## Project Context
This work is part of a university group internship course in collaboration with Advocations. The project emphasizes hands-on learning and real-world application of data skills in a collaborative environment.

---

## Team Apex
- Team-based collaboration focused on delivering meaningful data insights  
- Emphasis on organization, consistency, and continuous improvement  

---

## Notes
This repository contains work related to the Cacheconomy project. Some datasets and details may be restricted due to project scope and data privacy considerations.

---

## Environment Variables (.env)

Create a `.env` file in the project root with the following variables:

```
SUPABASE_URL=your_supabase_project_url
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_secret
SUPABASE_DB_URL=postgresql://USER:PASSWORD@HOST:5432/postgres
SUPABASE_TABLE=your_table_name
SUPABASE_SOURCE_TABLE=companies_data
SUPABASE_DEST_TABLE=Cleaned_companies_data
SUPABASE_PAGE_SIZE=1000
SUPABASE_MAX_PAGES=3
```

- `SUPABASE_URL`: Your Supabase project URL (used by the small REST helper scripts).
- `SUPABASE_SERVICE_ROLE_KEY`: Service role secret key (for admin/server-side scripts only).
- `SUPABASE_DB_URL`: Direct Postgres connection string used by the cleaning pipeline. Use either:
  - the **direct connection** on port `5432`, or
  - the **Supavisor session pooler** on port `5432` (recommended if your network does not support IPv6).
  Format: `postgresql://postgres.<project-ref>:<password>@<host>:5432/postgres`. Get this string from the Supabase dashboard under *Project Settings → Database → Connection string*.
- `SUPABASE_TABLE`: The table to query for the original single-table scripts.
- `SUPABASE_SOURCE_TABLE`: Source table for the Supabase-to-Supabase cleaning pipeline.
- `SUPABASE_DEST_TABLE`: Destination table for cleaned output. This table is replaced on each pipeline run.
- `SUPABASE_PAGE_SIZE`: Number of rows to fetch per page for the legacy REST helpers.
- `SUPABASE_MAX_PAGES`: Maximum number of pages to fetch in one run for the legacy REST helpers.

## Supabase Cleaning Pipeline

Use the cleaning pipeline script to read raw data from one Supabase table, merge duplicates in memory, and write the cleaned result to another table:

```bash
python sync_cleaned_pipeline.py
```

This pipeline runs as a **direct Postgres ETL** instead of going through the Supabase REST API, so it can handle very large source tables without PostgREST statement timeouts.

What it does, end to end:

1. Connects to Postgres using `SUPABASE_DB_URL` and disables `statement_timeout` for the ETL session only.
2. Streams every row from `SUPABASE_SOURCE_TABLE` using a server-side cursor (no offset paging, no REST API).
3. Runs the duplicate-merging logic from `merge_duplicates.py` in pandas.
4. Bulk-loads cleaned rows into a temporary staging table with Postgres `COPY`.
5. Atomically refreshes `SUPABASE_DEST_TABLE` inside a single transaction: `TRUNCATE` then `INSERT ... SELECT` from staging. The destination is never left half-written.

Notes and requirements:

- The destination table is fully replaced on every successful run.
- Metadata columns (`id`, `created_at`, `updated_at`) are not copied so Supabase can regenerate them.
- Only columns that exist in **both** the merged data and the destination table are loaded; merged columns missing from the destination are skipped (and printed in the run summary).
- Run this from a machine that can reach the Supabase database (direct connection requires IPv6; otherwise use the Supavisor session pooler on port 5432).
- `psycopg` (installed via `requirements.txt` as `psycopg[binary]`) is required.
- Because the script uses a privileged connection and replaces the cleaned table on each run, only use it in trusted server-side or local development contexts.

**Never commit your .env file or secret keys to version control.**

## Recent Work Log

A second-pass aggressive multi-pass dedupe workflow was added and loaded into `v2_Cleaned_companies_data` for comparison against the original `Cleaned_companies_data` output. The v2 run caught many more candidate duplicates, but evaluation showed it is likely too aggressive for final production use, so treat it as an analysis/comparison result unless the matching rules are tightened further.