"""Direct Postgres ETL: companies_data -> clean -> Cleaned_companies_data.

This script bypasses the Supabase REST API and connects directly to Postgres
to avoid PostgREST statement timeouts on the large source table. It streams
the source rows with a server-side cursor, runs the existing duplicate-merge
logic in pandas, bulk-loads cleaned rows into a staging table with COPY, and
atomically refreshes the destination table inside a single transaction.
"""

import io
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg
from psycopg import sql
from dotenv import load_dotenv

from merge_duplicates import (
    EXCLUDE_FROM_MERGE,
    MATCH_FIELDS,
    PRIMARY_KEY,
    merge_all_duplicates,
)
from data_cleaning import standardize_dataframe


READ_CHUNK_SIZE = 50_000
COPY_NULL_SENTINEL = "__NULL__"
STAGING_TABLE_NAME = "_cleaned_companies_data_stage"


def get_db_url() -> str:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError(
            "Missing SUPABASE_DB_URL in .env. Set it to a direct Postgres "
            "connection string (port 5432, direct or Supavisor session mode)."
        )
    return db_url


def get_source_table() -> str:
    source_table = os.getenv("SUPABASE_SOURCE_TABLE") or os.getenv("SUPABASE_TABLE")
    if not source_table:
        raise ValueError("Missing SUPABASE_SOURCE_TABLE or SUPABASE_TABLE in .env")
    return source_table


def get_destination_table() -> str:
    destination_table = os.getenv("SUPABASE_DEST_TABLE")
    if not destination_table:
        raise ValueError("Missing SUPABASE_DEST_TABLE in .env")
    return destination_table


def open_connection(db_url: str) -> psycopg.Connection:
    connection = psycopg.connect(db_url, autocommit=False)
    with connection.cursor() as cursor:
        cursor.execute("SET statement_timeout = 0")
        cursor.execute("SET idle_in_transaction_session_timeout = 0")
    connection.commit()
    return connection


def ensure_destination_table(
    connection: psycopg.Connection,
    source_table: str,
    destination_table: str,
) -> List[str]:
    """Create the destination table from the source schema if it doesn't exist."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = %s",
            (destination_table,),
        )
        exists = cursor.fetchone() is not None

    if not exists:
        print(f"Destination table '{destination_table}' not found — creating it...")
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                    sql.Identifier(destination_table),
                    sql.Identifier(source_table),
                )
            )
        connection.commit()
        print(f"Created '{destination_table}' from '{source_table}' schema.")
    else:
        print(f"Destination table '{destination_table}' already exists.")

    return fetch_table_columns(connection, destination_table)


def fetch_table_columns(connection: psycopg.Connection, table_name: str) -> List[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT * FROM {} LIMIT 0").format(sql.Identifier(table_name))
        )
        if cursor.description is None:
            return []
        return [desc.name for desc in cursor.description]


def fetch_source_dataframe(
    connection: psycopg.Connection,
    source_table: str,
    chunk_size: int,
) -> pd.DataFrame:
    chunks: List[pd.DataFrame] = []
    total_rows = 0

    print(f"Streaming source table via direct Postgres: {source_table}")

    with connection.cursor(name="cleaned_pipeline_source_cursor") as cursor:
        cursor.itersize = chunk_size
        cursor.execute(
            sql.SQL("SELECT * FROM {}").format(sql.Identifier(source_table))
        )
        column_names = [desc.name for desc in cursor.description]

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break

            chunk_df = pd.DataFrame(rows, columns=column_names)
            chunks.append(chunk_df)
            total_rows += len(chunk_df)
            print(
                f"Fetched chunk of {len(chunk_df):,} rows "
                f"(total: {total_rows:,})"
            )

    connection.commit()

    if not chunks:
        return pd.DataFrame(columns=column_names)

    dataframe = pd.concat(chunks, ignore_index=True)
    print(f"Total source rows fetched: {len(dataframe):,}")
    return dataframe


def select_destination_columns(
    merged_columns: List[str], destination_columns: List[str]
) -> List[str]:
    excluded = set(EXCLUDE_FROM_MERGE)
    merged_set = set(merged_columns)
    return [
        column
        for column in destination_columns
        if column in merged_set and column not in excluded
    ]


def normalize_value_for_copy(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item") and not isinstance(value, str):
        try:
            return value.item()
        except Exception:
            return value
    return value


def dataframe_to_copy_buffer(
    dataframe: pd.DataFrame, columns: List[str]
) -> io.StringIO:
    buffer = io.StringIO()
    projected = dataframe[columns].copy()
    for column in columns:
        projected[column] = projected[column].map(normalize_value_for_copy)
    projected.to_csv(
        buffer,
        index=False,
        header=False,
        na_rep=COPY_NULL_SENTINEL,
        lineterminator="\n",
    )
    buffer.seek(0)
    return buffer


def run_replace_all_in_transaction(
    connection: psycopg.Connection,
    destination_table: str,
    columns: List[str],
    copy_buffer: io.StringIO,
    expected_row_count: int,
) -> Tuple[int, int]:
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    staging_table = STAGING_TABLE_NAME

    with connection.cursor() as cursor:
        destination_identifier = sql.Identifier(destination_table)
        staging_identifier = sql.Identifier(staging_table)
        column_identifiers = [sql.Identifier(column) for column in columns]
        column_list_sql = sql.SQL(", ").join(column_identifiers)

        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(staging_identifier)
        )
        cursor.execute(
            sql.SQL("CREATE TEMP TABLE {} AS SELECT {} FROM {} WITH NO DATA").format(
                staging_identifier,
                column_list_sql,
                destination_identifier,
            )
        )

        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL {})"
        ).format(
            staging_identifier,
            column_list_sql,
            sql.Literal(COPY_NULL_SENTINEL),
        )
        with cursor.copy(copy_sql) as copy:
            copy.write(copy_buffer.read())

        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(staging_identifier)
        )
        staged_count = cursor.fetchone()[0]

        if staged_count != expected_row_count:
            raise RuntimeError(
                f"Staged row count {staged_count:,} does not match "
                f"merged row count {expected_row_count:,}; aborting refresh."
            )

        print(f"Staged {staged_count:,} rows into {staging_table}.")

        cursor.execute(
            sql.SQL("TRUNCATE TABLE {}").format(destination_identifier)
        )
        cursor.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                destination_identifier,
                column_list_sql,
                column_list_sql,
                staging_identifier,
            )
        )
        inserted_count = cursor.rowcount

        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(destination_identifier)
        )
        destination_count = cursor.fetchone()[0]

    connection.commit()
    return inserted_count, destination_count


def main() -> int:
    load_dotenv()

    try:
        db_url = get_db_url()
        source_table = get_source_table()
        destination_table = get_destination_table()

        if source_table == destination_table:
            raise ValueError(
                "SUPABASE_SOURCE_TABLE and SUPABASE_DEST_TABLE must differ"
            )

        print(f"Source table:      {source_table}")
        print(f"Destination table: {destination_table}")
        print(f"Read chunk size:   {READ_CHUNK_SIZE:,}")
        print(f"Match fields:      {', '.join(MATCH_FIELDS)}")
        print(f"Primary key:       {PRIMARY_KEY}")

        connection = open_connection(db_url)

        try:
            source_df = fetch_source_dataframe(
                connection=connection,
                source_table=source_table,
                chunk_size=READ_CHUNK_SIZE,
            )
            source_count = len(source_df)

            print("\nStandardizing columns...")
            source_df = standardize_dataframe(source_df)
            print("Standardization complete.")

            destination_columns = ensure_destination_table(
                connection, source_table, destination_table
            )
            if not destination_columns:
                raise RuntimeError(
                    f"Destination table {destination_table} has no columns "
                    f"or was not found in schema 'public'."
                )

            if source_count == 0:
                print("Source table is empty. Truncating destination table.")
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("TRUNCATE TABLE {}").format(
                            sql.Identifier(destination_table)
                        )
                    )
                connection.commit()
                print("Success: destination cleared, no cleaned rows to write.")
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

            target_columns = select_destination_columns(
                merged_columns=list(merged_df.columns),
                destination_columns=destination_columns,
            )
            if not target_columns:
                raise RuntimeError(
                    "No overlapping columns between merged data and destination "
                    f"table {destination_table}."
                )

            missing_in_dest = sorted(
                column
                for column in merged_df.columns
                if column not in destination_columns
                and column not in EXCLUDE_FROM_MERGE
            )
            if missing_in_dest:
                print(
                    "Skipping merged columns not present in destination: "
                    f"{', '.join(missing_in_dest)}"
                )

            print(f"Loading {len(target_columns)} columns into destination.")

            copy_buffer = dataframe_to_copy_buffer(merged_df, target_columns)
            inserted_count, destination_count = run_replace_all_in_transaction(
                connection=connection,
                destination_table=destination_table,
                columns=target_columns,
                copy_buffer=copy_buffer,
                expected_row_count=merged_count,
            )

            print(f"Inserted into destination:  {inserted_count:,}")
            print(f"Destination row count now: {destination_count:,}")
            print("Success: cleaned data synced to Supabase via direct Postgres.")
            return 0
        finally:
            connection.close()
    except Exception as exc:
        print(f"Sync failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
