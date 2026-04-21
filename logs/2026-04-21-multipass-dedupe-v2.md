# Multi-Pass Dedupe V2 Log

Date: 2026-04-21

## Summary

Implemented a second-pass duplicate-cleaning approach that is much more aggressive than the original exact-match workflow.

This run writes to:

- `public."v2_Cleaned_companies_data"`

so it can be compared side by side with the original first-pass output in:

- `public."Cleaned_companies_data"`

## What Changed

The old matcher only merged rows when these three fields matched exactly:

- `companyName`
- `streetAddress`
- `postalCode`

The new matcher in `merge_duplicates.py` now:

- normalizes name, address, ZIP, website/domain, phone, and stable IDs
- uses multiple deterministic matching passes
- unions overlapping matches into connected duplicate groups
- preserves merge behavior by filling missing values from related rows instead of just dropping duplicates

The analysis script `identify_duplicates.py` was also updated so it uses the same production logic for reporting.

## Final Matching Passes Used

The tuned aggressive matcher used these passes:

- `duns_exact`
- `companyID_exact`
- `placeId_exact`
- `googleId_exact`
- `SOSID_exact`
- `domain_address_zip`
- `name_facebook`
- `name_linkedin`
- `name_twitter`
- `name_instagram`
- `name_youtube`
- `phone_address_zip`
- `name_domain`
- `name_phone`
- `name_domain_zip`
- `name_phone_zip`
- `name_address_zip`
- `name_address`
- `name_address_number_zip`

Some originally broader passes were removed or tightened after spot checks showed they over-merged unrelated businesses.

## Results

- Source table row count: `787,564`
- Original first-pass cleaned table row count: `782,252`
- V2 multi-pass cleaned table row count: `623,321`

This means the second attempt eliminated:

- `164,243` rows relative to the raw source

Compared with the first attempt, the V2 matcher removed:

- `158,931` more rows than the original exact-match-based cleaned table

## Duplicate Counts Observed

The final tuned multi-pass analysis found:

- `263,216` duplicate-involved rows
- `98,973` connected duplicate groups

That is much closer to the expected duplicate scale for a dataset assembled from multiple source feeds.

## Per-Pass Breakdown

- `duns_exact`: `170` rows across `85` groups
- `companyID_exact`: `122` rows across `61` groups
- `placeId_exact`: `530` rows across `260` groups
- `googleId_exact`: `530` rows across `260` groups
- `SOSID_exact`: `799` rows across `364` groups
- `domain_address_zip`: `75,180` rows across `32,015` groups
- `name_facebook`: `9,749` rows across `3,325` groups
- `name_linkedin`: `5,287` rows across `1,894` groups
- `name_twitter`: `7,066` rows across `2,145` groups
- `name_instagram`: `937` rows across `317` groups
- `name_youtube`: `576` rows across `179` groups
- `phone_address_zip`: `81,897` rows across `34,571` groups
- `name_domain`: `64,089` rows across `24,104` groups
- `name_phone`: `35,499` rows across `16,593` groups
- `name_domain_zip`: `40,914` rows across `17,897` groups
- `name_phone_zip`: `28,213` rows across `13,423` groups
- `name_address_zip`: `129,303` rows across `63,396` groups
- `name_address`: `130,679` rows across `64,059` groups
- `name_address_number_zip`: `159,238` rows across `77,357` groups

## Operational Issues Resolved

### 1. Destination table schema

`v2_Cleaned_companies_data` existed in Supabase, but Postgres reported it with zero columns.

To make the ETL load possible, the table was recreated from the `companies_data` schema.

### 2. Long-lived read connection during merge

The first ETL version kept a Postgres session open while Python spent a long time doing in-memory merging. That caused the server connection to die before the load step.

The ETL was updated to:

- use one connection for reading source rows
- close it before the heavy in-memory merge
- open a fresh write connection for the staging-table load and final replace

### 3. Stale idle transactions blocking `TRUNCATE`

An abandoned session briefly blocked `TRUNCATE` on `v2_Cleaned_companies_data`. That stale backend was terminated so the load could proceed.

## Notes

This second attempt is intentionally much more aggressive than the first one, so it likely catches many more real multi-source duplicates.

It also has a higher false-positive risk than the original exact-match approach. The next useful step would be targeted manual review of representative merged groups from the strongest non-ID passes to confirm the balance between recall and precision.
