import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client
from typing import List, Dict

def get_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, key)

def fetch_all_data(client: Client, table: str, page_size: int, max_pages: int) -> pd.DataFrame:
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

def main() -> int:
    load_dotenv()

    table = os.getenv("SUPABASE_TABLE")
    page_size = int(os.getenv("SUPABASE_PAGE_SIZE", "1000"))
    max_pages = int(os.getenv("SUPABASE_MAX_PAGES", "3"))

    if not table:
        print("Missing SUPABASE_TABLE in .env")
        return 1

    try:
        client = get_client()
        df = fetch_all_data(client, table, page_size, max_pages)

        # quick check
        print(df.head())
        print(df.shape)

        # optionally save to CSV
        df.to_csv("fetched_data_sample.csv", index=False)
        return 0
    except Exception as exc:
        print(f"Failed: {exc}")
        return 1

if __name__ == "__main__":
    sys.exit(main())