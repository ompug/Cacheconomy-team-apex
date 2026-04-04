import os
import sys
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client


def get_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

    return create_client(url, key)


def run_paginated_check(client: Client, table: str, page_size: int, max_pages: int) -> int:
    total_rows_seen = 0

    print(f"Starting API pagination check on table: {table}")
    print(f"Page size: {page_size}, Max pages: {max_pages}")
    print("Using service role key (server-side only).")

    for page in range(max_pages):
        start = page * page_size
        end = start + page_size - 1

        response = (
            client.table(table)
            .select("*", count="exact")
            .range(start, end)
            .execute()
        )

        rows = response.data or []
        count_value: Optional[int] = response.count
        row_count = len(rows)
        total_rows_seen += row_count

        print(
            f"Page {page + 1}: fetched {row_count} rows "
            f"(range {start}-{end})"
        )

        if page == 0 and count_value is not None:
            print(f"Estimated total rows in table: {count_value}")

        if row_count == 0:
            print("No rows returned. Stopping early.")
            break

        if row_count < page_size:
            print("Reached final partial page. Stopping early.")
            break

    return total_rows_seen


def main() -> int:
    load_dotenv()

    table = os.getenv("SUPABASE_TABLE")
    page_size = int(os.getenv("SUPABASE_PAGE_SIZE", "1000"))
    max_pages = int(os.getenv("SUPABASE_MAX_PAGES", "3"))

    if not table:
        print("Missing SUPABASE_TABLE in .env")
        return 1

    if page_size <= 0 or max_pages <= 0:
        print("SUPABASE_PAGE_SIZE and SUPABASE_MAX_PAGES must be positive integers")
        return 1

    try:
        client = get_client()
        total = run_paginated_check(
            client=client,
            table=table,
            page_size=page_size,
            max_pages=max_pages,
        )
        print(f"Success: API calls worked. Total rows fetched in check: {total}")
        return 0
    except Exception as exc:
        print(f"API check failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
