import os
import sys
from datetime import date, datetime
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from supabase import Client

from data_loading import fetch_entire_table, get_client
from merge_duplicates import (
    EXCLUDE_FROM_MERGE,
    MATCH_FIELDS,
    PRIMARY_KEY,
    merge_all_duplicates,
)


INSERT_EXCLUDE_COLUMNS = set(EXCLUDE_FROM_MERGE)
INSERT_BATCH_SIZE = 500


def get_source_table() -> str:
    source_table = os.getenv("SUPABASE_SOURCE_TABLE") or os.getenv("SUPABASE_TABLE")
    if not source_table:
        raise ValueError(
            "Missing SUPABASE_SOURCE_TABLE or SUPABASE_TABLE in .env"
        )
    return source_table


def get_destination_table() -> str:
    destination_table = os.getenv("SUPABASE_DEST_TABLE")
    if not destination_table:
        raise ValueError("Missing SUPABASE_DEST_TABLE in .env")
    return destination_table


def normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        record = {
            key: normalize_value(value)
            for key, value in row.items()
            if key not in INSERT_EXCLUDE_COLUMNS
        }
        records.append(record)

    return records


def clear_destination_table(client: Client, table: str) -> None:
    print(f"Clearing destination table: {table}")
    client.table(table).delete().execute()


def insert_records(client: Client, table: str, records: List[Dict[str, Any]]) -> None:
    if not records:
        print("No cleaned rows to insert.")
        return

    total_records = len(records)
    print(f"Inserting {total_records:,} cleaned rows into: {table}")

    for start in range(0, total_records, INSERT_BATCH_SIZE):
        end = min(start + INSERT_BATCH_SIZE, total_records)
        batch = records[start:end]
        client.table(table).insert(batch).execute()
        print(
            f"Inserted rows {start + 1:,}-{end:,} "
            f"of {total_records:,}"
        )


def count_rows_by_paging(client: Client, table: str, page_size: int) -> int:
    total_rows = 0
    page = 0

    while True:
        start = page * page_size
        end = start + page_size - 1
        response = client.table(table).select("*").range(start, end).execute()
        rows = response.data or []

        if not rows:
            break

        total_rows += len(rows)

        if len(rows) < page_size:
            break

        page += 1

    return total_rows


def main() -> int:
    load_dotenv()

    try:
        page_size = int(os.getenv("SUPABASE_PAGE_SIZE", "1000"))
        if page_size <= 0:
            raise ValueError("SUPABASE_PAGE_SIZE must be a positive integer")

        source_table = get_source_table()
        destination_table = get_destination_table()

        if source_table == destination_table:
            raise ValueError(
                "SUPABASE_SOURCE_TABLE and SUPABASE_DEST_TABLE must be different"
            )

        client = get_client()

        print(f"Source table: {source_table}")
        print(f"Destination table: {destination_table}")
        print(f"Page size: {page_size}")
        print(f"Match fields: {', '.join(MATCH_FIELDS)}")
        print(f"Primary key: {PRIMARY_KEY}")

        source_df = fetch_entire_table(client, source_table, page_size)
        source_count = len(source_df)

        if source_count == 0:
            print("\nSource table is empty. Clearing destination table.")
            clear_destination_table(client, destination_table)
            destination_count = count_rows_by_paging(
                client, destination_table, page_size
            )
            print(f"Destination row count after sync: {destination_count:,}")
            print("Success: cleaned data synced to Supabase.")
            return 0

        print("\nFinding and merging duplicates...")
        merged_df, merge_reports = merge_all_duplicates(
            source_df,
            MATCH_FIELDS,
            PRIMARY_KEY,
            EXCLUDE_FROM_MERGE,
        )
        merged_count = len(merged_df)

        print(f"\nMerge complete: {source_count:,} -> {merged_count:,} rows")
        print(f"Eliminated {source_count - merged_count:,} duplicate rows")
        print(f"Duplicate groups found: {len(merge_reports):,}")

        records = dataframe_to_records(merged_df)
        clear_destination_table(client, destination_table)
        insert_records(client, destination_table, records)

        destination_count = count_rows_by_paging(client, destination_table, page_size)
        print(f"\nDestination row count after sync: {destination_count:,}")
        print("Success: cleaned data synced to Supabase.")
        return 0
    except Exception as exc:
        print(f"Sync failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
