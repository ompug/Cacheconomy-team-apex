import os
import sys
import csv
import time
import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client
from typing import Dict, List, Optional, Tuple


COMPANY_NAME_BUCKETS: List[Tuple[Optional[str], Optional[str], Optional[str]]] = [
    ("", None, None),
    *[
        (None, chr(codepoint), chr(codepoint + 1))
        for codepoint in range(ord(" "), ord("~"))
    ],
    (None, "~", None),
]


def get_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, key)


def fetch_all_data(client: Client, table: str, page_size: int, max_pages: int) -> pd.DataFrame:
    """Original function: fetches data into memory with page limit."""
    all_rows: List[Dict] = []

    print(f"Fetching data from table: {table}")
    for page in range(max_pages):
        start = page * page_size
        end = start + page_size - 1

        response = client.table(table).select("*", count="exact").range(start, end).execute()
        rows = response.data or []

        print(f"Page {page+1}: fetched {len(rows)} rows")
        all_rows.extend(rows)

        if len(rows) < page_size:
            print("Reached last partial page. Stopping early.")
            break

    df = pd.DataFrame(all_rows)
    print(f"Total rows fetched: {len(df)}")
    return df


def fetch_entire_table(client: Client, table: str, page_size: int) -> pd.DataFrame:
    """Fetch every row from a Supabase table into memory."""
    all_rows: List[Dict] = []

    print(f"Fetching entire table from: {table}")

    page = 0
    for exact_value, lower_bound, upper_bound in COMPANY_NAME_BUCKETS:
        bucket_label = describe_company_name_bucket(
            exact_value=exact_value,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )
        print(f"Scanning bucket: {bucket_label}")
        bucket_offset = 0
        while True:
            query = client.table(table).select("*")
            if exact_value is not None:
                query = query.eq("companyName", exact_value)
            else:
                if lower_bound is not None:
                    query = query.gte("companyName", lower_bound)
                if upper_bound is not None:
                    query = query.lt("companyName", upper_bound)

            response = execute_with_retries(
                lambda: query.range(bucket_offset, bucket_offset + page_size - 1).execute(),
                context=(
                    f"{bucket_label} offset {bucket_offset}-"
                    f"{bucket_offset + page_size - 1}"
                ),
            )
            rows = response.data or []

            if not rows:
                break

            page += 1
            all_rows.extend(rows)
            print(
                f"Page {page}: fetched {len(rows)} rows "
                f"(total: {len(all_rows):,})"
            )

            if len(rows) < page_size:
                break

            bucket_offset += page_size

    df = pd.DataFrame(all_rows)
    print(f"Total rows fetched: {len(df):,}")
    return df


def describe_company_name_bucket(
    exact_value: Optional[str],
    lower_bound: Optional[str],
    upper_bound: Optional[str],
) -> str:
    if exact_value is not None:
        return f"companyName == {repr(exact_value)}"
    return f"companyName in [{repr(lower_bound)}, {repr(upper_bound)})"


def execute_with_retries(operation, context: str, attempts: int = 3):
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt == attempts:
                break
            print(
                f"Retrying after error on {context} "
                f"(attempt {attempt}/{attempts}): {exc}"
            )
            time.sleep(attempt)

    raise last_error


def stream_all_data_to_csv(client: Client, table: str, page_size: int,
                           output_file: str) -> int:
    """
    Stream ALL data from Supabase directly to CSV file.
    Memory efficient - only holds one page in memory at a time.

    Returns: Total number of rows fetched
    """
    total_rows = 0
    page = 0
    writer: Optional[csv.DictWriter] = None
    file_handle = None
    total_count: Optional[int] = None

    print(f"Streaming all data from table: {table}")
    print(f"Output file: {output_file}")

    try:
        file_handle = open(output_file, 'w', newline='', encoding='utf-8')

        while True:
            start = page * page_size
            end = start + page_size - 1

            response = client.table(table).select("*", count="exact").range(start, end).execute()
            rows = response.data or []

            # Get total count on first page
            if page == 0 and response.count is not None:
                total_count = response.count
                print(f"Total rows in table: {total_count:,}")

            if not rows:
                print("No more data. Stopping.")
                break

            # Initialize CSV writer with headers from first batch
            if writer is None and rows:
                fieldnames = list(rows[0].keys())
                writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
                writer.writeheader()

            # Write rows to CSV
            writer.writerows(rows)
            total_rows += len(rows)

            # Progress update
            if total_count:
                pct = (total_rows / total_count) * 100
                print(f"Page {page+1}: fetched {len(rows)} rows "
                      f"({total_rows:,} / {total_count:,} = {pct:.1f}%)")
            else:
                print(f"Page {page+1}: fetched {len(rows)} rows (total: {total_rows:,})")

            if len(rows) < page_size:
                print("Reached last partial page. Done.")
                break

            page += 1

    finally:
        if file_handle:
            file_handle.close()

    print(f"\nTotal rows fetched: {total_rows:,}")
    return total_rows


def main() -> int:
    load_dotenv()

    table = os.getenv("SUPABASE_TABLE")
    page_size = int(os.getenv("SUPABASE_PAGE_SIZE", "1000"))
    max_pages = int(os.getenv("SUPABASE_MAX_PAGES", "3"))
    fetch_all = os.getenv("SUPABASE_FETCH_ALL", "false").lower() == "true"
    output_file = "fetched_data_sample.csv"

    if not table:
        print("Missing SUPABASE_TABLE in .env")
        return 1

    try:
        client = get_client()

        if fetch_all:
            # Stream ALL data to CSV (memory efficient)
            print("FETCH_ALL mode enabled - fetching entire table...")
            total = stream_all_data_to_csv(client, table, page_size, output_file)
            print(f"\nData saved to: {output_file}")
            print(f"Total rows: {total:,}")
        else:
            # Original behavior: limited pages, load into memory
            df = fetch_all_data(client, table, page_size, max_pages)
            print(df.head())
            print(df.shape)
            df.to_csv(output_file, index=False)

        return 0
    except Exception as exc:
        print(f"Failed: {exc}")
        return 1

if __name__ == "__main__":
    sys.exit(main())