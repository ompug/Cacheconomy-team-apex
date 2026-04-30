import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, List

import pandas as pd
from psycopg import sql
from dotenv import load_dotenv

import api_pagination_check
import data_cleaning
import data_loading
import identify_duplicates
import merge_duplicates
import sync_cleaned_pipeline


DEFAULT_SOURCE_TABLE = "companies_data"
DEFAULT_DESTINATION_TABLE = "final_cleaned_companies"
DESTINATION_WRITE_BATCH_SIZE = 1_000


@dataclass(frozen=True)
class Stage:
    name: str
    entrypoint: Callable[[], int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full Cacheconomy pipeline and load the final result into "
            "Supabase."
        )
    )
    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help="Raw Supabase source table to export and clean.",
    )
    parser.add_argument(
        "--destination-table",
        default=DEFAULT_DESTINATION_TABLE,
        help="Destination Supabase table for the final cleaned output.",
    )
    return parser.parse_args()


def validate_environment() -> List[str]:
    required_vars = [
        "SUPABASE_URL",
        "SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_DB_URL",
    ]
    return [name for name in required_vars if not os.getenv(name)]


def apply_runtime_configuration(source_table: str, destination_table: str) -> Dict[str, str]:
    previous_values = {
        "SUPABASE_TABLE": os.getenv("SUPABASE_TABLE", ""),
        "SUPABASE_SOURCE_TABLE": os.getenv("SUPABASE_SOURCE_TABLE", ""),
        "SUPABASE_DEST_TABLE": os.getenv("SUPABASE_DEST_TABLE", ""),
        "SUPABASE_FETCH_ALL": os.getenv("SUPABASE_FETCH_ALL", ""),
    }

    os.environ["SUPABASE_TABLE"] = source_table
    os.environ["SUPABASE_SOURCE_TABLE"] = source_table
    os.environ["SUPABASE_DEST_TABLE"] = destination_table
    os.environ["SUPABASE_FETCH_ALL"] = "true"

    return previous_values


def restore_runtime_configuration(previous_values: Dict[str, str]) -> None:
    for key, value in previous_values.items():
        if value:
            os.environ[key] = value
        else:
            os.environ.pop(key, None)


def run_stage(stage: Stage) -> int:
    print(f"\n=== {stage.name} ===")
    started_at = time.time()
    exit_code = stage.entrypoint()
    elapsed = time.time() - started_at
    print(f"=== {stage.name} finished with exit code {exit_code} in {elapsed:.1f}s ===")
    return exit_code


def run_local_standardization() -> int:
    input_file = "fetched_data_sample.csv"
    output_file = "cleaned_data.csv"

    try:
        source = pd.read_csv(input_file, low_memory=False)
        cleaned = data_cleaning.standardize_dataframe(source)
        cleaned.to_csv(output_file, index=False)
        print(
            f"Standardization complete: {len(cleaned):,} rows written to {output_file}"
        )
        return 0
    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run data_loading.py first.")
        return 1
    except Exception as exc:
        print(f"Standardization failed: {exc}")
        return 1


def run_local_duplicate_merge() -> int:
    input_file = "fetched_data_sample.csv"
    output_file = "merged_data.csv"
    report_file = "merge_report.txt"

    try:
        df = pd.read_csv(input_file, low_memory=False)
        original_count = len(df)

        merged_df, merge_reports, match_stats = (
            merge_duplicates.merge_all_duplicates_with_stats(
                df,
                merge_duplicates.MATCH_FIELDS,
                merge_duplicates.PRIMARY_KEY,
                merge_duplicates.EXCLUDE_FROM_MERGE,
            )
        )
        merged_df.to_csv(output_file, index=False)

        report = merge_duplicates.generate_merge_report(
            original_count=original_count,
            merged_count=len(merged_df),
            merge_reports=merge_reports,
            match_fields=merge_duplicates.MATCH_FIELDS,
            match_stats=match_stats,
        )
        with open(report_file, "w", encoding="utf-8") as handle:
            handle.write(report)

        print(f"Merge complete: {original_count:,} -> {len(merged_df):,} rows")
        print(f"Eliminated {original_count - len(merged_df):,} duplicate rows")
        print(f"Merge report saved to: {report_file}")
        print(f"Merged data saved to: {output_file}")
        return 0
    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run data_loading.py first.")
        return 1
    except Exception as exc:
        print(f"Duplicate merge failed: {exc}")
        return 1


def run_local_duplicate_analysis() -> int:
    input_file = "fetched_data_sample.csv"
    duplicates_file = "duplicate_rows.csv"
    multi_pass_duplicates_file = "multipass_duplicate_rows.csv"
    summary_file = "duplicate_summary.txt"

    try:
        df = pd.read_csv(input_file, low_memory=False)
        exact_duplicates = identify_duplicates.find_exact_duplicates(
            df, exclude_cols=identify_duplicates.EXCLUDE_COLUMNS
        )
        multi_pass_duplicates, multi_pass_stats = (
            identify_duplicates.find_multipass_duplicates(df)
        )
        field_stats = identify_duplicates.analyze_field_duplicates(df)

        report = identify_duplicates.generate_summary_report(
            df,
            exact_duplicates,
            multi_pass_duplicates,
            multi_pass_stats,
            field_stats,
            exclude_cols=identify_duplicates.EXCLUDE_COLUMNS,
        )
        with open(summary_file, "w", encoding="utf-8") as handle:
            handle.write(report)

        if len(exact_duplicates) > 0:
            exact_duplicates.to_csv(duplicates_file, index=False)
        if len(multi_pass_duplicates) > 0:
            multi_pass_duplicates.to_csv(multi_pass_duplicates_file, index=False)

        print(f"Exact duplicate rows:       {len(exact_duplicates):,}")
        print(f"Multi-pass duplicate rows:  {len(multi_pass_duplicates):,}")
        print(
            f"Multi-pass duplicate groups: "
            f"{multi_pass_stats.get('group_count', 0):,}"
        )
        print(f"Summary saved to: {summary_file}")
        if len(exact_duplicates) > 0:
            print(f"Duplicate rows saved to: {duplicates_file}")
        else:
            print("No exact duplicate rows to export.")
        if len(multi_pass_duplicates) > 0:
            print(f"Multi-pass duplicate rows saved to: {multi_pass_duplicates_file}")
        else:
            print("No multi-pass duplicate rows to export.")
        return 0
    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run data_loading.py first.")
        return 1
    except Exception as exc:
        print(f"Duplicate analysis failed: {exc}")
        return 1


def run_final_supabase_load() -> int:
    input_file = "merged_data.csv"

    try:
        db_url = sync_cleaned_pipeline.get_db_url()
        source_table = sync_cleaned_pipeline.get_source_table()
        destination_table = sync_cleaned_pipeline.get_destination_table()
        connection = sync_cleaned_pipeline.open_connection(db_url)
    except Exception as exc:
        print(f"Final load setup failed: {exc}")
        return 1

    try:
        destination_columns = sync_cleaned_pipeline.ensure_destination_table(
            connection, source_table, destination_table
        )
        merged_columns = list(pd.read_csv(input_file, nrows=0).columns)
        target_columns = sync_cleaned_pipeline.select_destination_columns(
            merged_columns=merged_columns,
            destination_columns=destination_columns,
        )

        if not target_columns:
            raise RuntimeError(
                "No overlapping columns between merged_data.csv and destination table."
            )

        destination_identifier = sql.Identifier(destination_table)
        column_identifiers = [sql.Identifier(column) for column in target_columns]
        column_list_sql = sql.SQL(", ").join(column_identifiers)
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            destination_identifier,
            column_list_sql,
            sql.SQL(", ").join(sql.Placeholder() for _ in target_columns),
        )

        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(destination_identifier))
        connection.commit()
        print(f"Destination table '{destination_table}' truncated.")

        inserted_total = 0
        chunk_reader = pd.read_csv(
            input_file,
            chunksize=DESTINATION_WRITE_BATCH_SIZE,
            low_memory=False,
        )
        for chunk in chunk_reader:
            rows = [
                tuple(sync_cleaned_pipeline.normalize_value_for_db(value) for value in row)
                for row in chunk[target_columns].itertuples(index=False, name=None)
            ]
            with connection.cursor() as cursor:
                cursor.executemany(insert_sql, rows)
            connection.commit()
            inserted_total += len(rows)
            print(
                f"Inserted batch of {len(rows):,} rows into destination "
                f"(total inserted: {inserted_total:,})"
            )

        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(destination_identifier)
            )
            destination_count = cursor.fetchone()[0]
            if destination_count != inserted_total:
                raise RuntimeError(
                    f"Destination row count {destination_count:,} does not match "
                    f"inserted row count {inserted_total:,}."
                )

        print(f"Inserted into destination:  {inserted_total:,}")
        print(f"Destination row count now: {destination_count:,}")
        print("Success: merged CSV loaded into Supabase destination table.")
        return 0
    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run the merge stage first.")
        return 1
    except Exception as exc:
        print(f"Final load failed: {exc}")
        return 1
    finally:
        connection.close()


def main() -> int:
    args = parse_args()
    load_dotenv()

    missing_vars = validate_environment()
    if missing_vars:
        print(
            "Missing required environment variables: "
            + ", ".join(sorted(missing_vars))
        )
        return 1

    print(f"Raw source table:      {args.source_table}")
    print(f"Final output table:    {args.destination_table}")

    previous_values = apply_runtime_configuration(
        source_table=args.source_table,
        destination_table=args.destination_table,
    )

    stages = [
        Stage("API pagination check", api_pagination_check.main),
        Stage("Raw data export", data_loading.main),
        Stage("Local standardization", run_local_standardization),
        Stage("Local duplicate merge", run_local_duplicate_merge),
        Stage("Local duplicate analysis", run_local_duplicate_analysis),
        Stage("Final Supabase load", run_final_supabase_load),
    ]

    try:
        for stage in stages:
            exit_code = run_stage(stage)
            if exit_code != 0:
                print(f"Pipeline stopped after failure in stage: {stage.name}")
                return exit_code
    finally:
        restore_runtime_configuration(previous_values)

    print("\nPipeline completed successfully.")
    print("Artifacts:")
    print("  - fetched_data_sample.csv")
    print("  - cleaned_data.csv")
    print("  - merged_data.csv")
    print("  - merge_report.txt")
    print("  - duplicate_summary.txt")
    print("  - duplicate_rows.csv / multipass_duplicate_rows.csv (if duplicates found)")
    print(f"  - Supabase table refreshed: {args.destination_table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
