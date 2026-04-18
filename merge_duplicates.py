"""
Duplicate Merging Script for Supabase Business Data
Merges field-based duplicates by combining non-null values from duplicate records.
"""

import pandas as pd
import sys
from datetime import datetime
from typing import List, Tuple

# =============================================================================
# CONFIGURATION
# =============================================================================

# Fields to match on for finding duplicates
# Records matching on ALL these fields are considered duplicates
MATCH_FIELDS = ['companyName', 'streetAddress', 'postalCode']

# Primary key field (used to identify which record to keep as "primary")
PRIMARY_KEY = 'dunsNumber'

# Fields to exclude from comparison/merging (metadata fields, etc.)
EXCLUDE_FROM_MERGE = ['id', 'created_at', 'updated_at']

# =============================================================================
# CORE FUNCTIONS
# =============================================================================


def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV data into DataFrame."""
    print(f"Loading data from: {filepath}")
    df = pd.read_csv(filepath, low_memory=False)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def find_field_duplicates(df: pd.DataFrame, match_fields: List[str]) -> pd.DataFrame:
    """
    Find rows that are duplicates based on specific fields.

    Args:
        df: Input DataFrame
        match_fields: List of column names to match on

    Returns:
        DataFrame with duplicate rows and a 'duplicate_group' column
    """
    # Validate match fields exist
    missing_fields = [f for f in match_fields if f not in df.columns]
    if missing_fields:
        raise ValueError(f"Match fields not found in data: {missing_fields}")

    # Create a subset for comparison
    comparison_df = df[match_fields].copy()

    # Mark all duplicates (keep=False marks ALL occurrences)
    duplicate_mask = comparison_df.duplicated(keep=False)
    duplicates = df[duplicate_mask].copy()

    if len(duplicates) > 0:
        # Assign group IDs to duplicate sets
        duplicates['duplicate_group'] = duplicates.groupby(match_fields).ngroup()

    return duplicates


def merge_duplicate_group(group_df: pd.DataFrame, primary_key: str,
                          exclude_cols: List[str]) -> Tuple[pd.Series, dict]:
    """
    Merge a group of duplicate records by combining non-null values.

    Strategy: Start with the first record (or the one with fewest nulls),
    then fill in any null values from other records in the group.

    Args:
        group_df: DataFrame containing duplicate records to merge
        primary_key: Column name for the primary key
        exclude_cols: Columns to exclude from merging

    Returns:
        Tuple of (merged_row, merge_info_dict)
    """
    merge_info = {
        'records_merged': len(group_df),
        'primary_keys_involved': group_df[primary_key].tolist() if primary_key in group_df.columns else [],
        'fields_filled': []
    }

    # Sort by number of non-null values (most complete record first)
    # Reset index to ensure proper iloc access
    null_counts = group_df.isnull().sum(axis=1)
    sorted_indices = null_counts.sort_values().index
    sorted_df = group_df.loc[sorted_indices].reset_index(drop=True)

    # Start with the most complete record as base
    merged = sorted_df.iloc[0].copy()
    kept_primary_key = merged.get(primary_key) if primary_key in sorted_df.columns else None

    # Columns to process (exclude certain fields)
    cols_to_merge = [c for c in group_df.columns
                     if c not in exclude_cols and c != 'duplicate_group']

    # Fill in nulls from other records
    for idx in range(1, len(sorted_df)):
        other_row = sorted_df.iloc[idx]
        for col in cols_to_merge:
            if pd.isna(merged[col]) and pd.notna(other_row[col]):
                merged[col] = other_row[col]
                merge_info['fields_filled'].append({
                    'field': col,
                    'value': str(other_row[col])[:50],  # Truncate for report
                    'from_record': str(other_row.get(primary_key, 'unknown'))
                })

    merge_info['kept_primary_key'] = str(kept_primary_key)

    return merged, merge_info


def merge_all_duplicates(df: pd.DataFrame, match_fields: List[str],
                         primary_key: str, exclude_cols: List[str]) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Merge all duplicate records in the DataFrame.

    Args:
        df: Input DataFrame
        match_fields: Fields to match duplicates on
        primary_key: Primary key column
        exclude_cols: Columns to exclude from merging

    Returns:
        Tuple of (merged_df, merge_reports)
    """
    # Find duplicates
    duplicates = find_field_duplicates(df, match_fields)

    if len(duplicates) == 0:
        print("No duplicates found based on match fields.")
        return df, []

    num_groups = duplicates['duplicate_group'].nunique()
    print(f"Found {len(duplicates)} duplicate rows in {num_groups} groups")

    # Get non-duplicate rows (will be kept as-is)
    non_dup_mask = ~df.index.isin(duplicates.index)
    result_rows = [df.loc[non_dup_mask]]
    merge_reports = []

    # Process each duplicate group
    unique_groups = duplicates['duplicate_group'].unique()
    total_groups = len(unique_groups)

    for i, group_id in enumerate(unique_groups):
        group_df = duplicates[duplicates['duplicate_group'] == group_id]

        if len(group_df) == 0:
            continue

        try:
            merged_row, merge_info = merge_duplicate_group(
                group_df, primary_key, exclude_cols + ['duplicate_group']
            )
            merge_info['group_id'] = group_id
            merge_reports.append(merge_info)

            # Convert series to single-row DataFrame
            merged_row_df = pd.DataFrame([merged_row])
            result_rows.append(merged_row_df)
        except Exception as e:
            print(f"Error processing group {group_id}: {e}")
            print(f"Group size: {len(group_df)}")
            raise

        # Progress update every 10000 groups
        if (i + 1) % 10000 == 0:
            print(f"Processed {i+1:,} / {total_groups:,} groups ({(i+1)/total_groups*100:.1f}%)")

    # Combine all rows
    result_df = pd.concat(result_rows, ignore_index=True)

    # Remove the duplicate_group column if it exists
    if 'duplicate_group' in result_df.columns:
        result_df = result_df.drop(columns=['duplicate_group'])

    return result_df, merge_reports


def generate_merge_report(original_count: int, merged_count: int,
                          merge_reports: List[dict], match_fields: List[str]) -> str:
    """Generate a text report of the merge operation."""
    report = []
    report.append("=" * 70)
    report.append("DUPLICATE MERGE REPORT")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 70)
    report.append("")

    # Configuration
    report.append("CONFIGURATION")
    report.append("-" * 40)
    report.append(f"Match fields: {', '.join(match_fields)}")
    report.append("")

    # Summary statistics
    report.append("SUMMARY")
    report.append("-" * 40)
    report.append(f"Original row count:     {original_count:,}")
    report.append(f"Merged row count:       {merged_count:,}")
    report.append(f"Rows eliminated:        {original_count - merged_count:,}")
    report.append(f"Duplicate groups found: {len(merge_reports):,}")
    report.append("")

    if merge_reports:
        report.append("=" * 70)
        report.append("MERGE DETAILS")
        report.append("-" * 70)

        for merge_info in merge_reports:
            report.append(f"\nGroup {merge_info['group_id']}:")
            report.append(f"  Records merged: {merge_info['records_merged']}")
            report.append(f"  Kept primary key: {merge_info['kept_primary_key']}")
            report.append(f"  Other primary keys: {', '.join(str(pk) for pk in merge_info['primary_keys_involved'] if str(pk) != merge_info['kept_primary_key'])}")

            if merge_info['fields_filled']:
                report.append(f"  Fields filled from other records:")
                for fill in merge_info['fields_filled']:
                    report.append(f"    - {fill['field']}: '{fill['value']}' (from {fill['from_record']})")
            else:
                report.append("  No fields needed filling (primary record was complete)")

    report.append("")
    report.append("=" * 70)
    report.append("END OF REPORT")
    report.append("=" * 70)

    return "\n".join(report)


def main() -> int:
    input_file = "fetched_data_sample.csv"
    output_file = "merged_data.csv"
    report_file = "merge_report.txt"

    try:
        # Load data
        df = load_data(input_file)
        original_count = len(df)

        # Validate configuration
        print(f"\nMatch fields: {MATCH_FIELDS}")
        print(f"Primary key: {PRIMARY_KEY}")

        # Find and merge duplicates
        print("\nFinding and merging duplicates...")
        merged_df, merge_reports = merge_all_duplicates(
            df, MATCH_FIELDS, PRIMARY_KEY, EXCLUDE_FROM_MERGE
        )

        merged_count = len(merged_df)
        print(f"\nMerge complete: {original_count} -> {merged_count} rows")
        print(f"Eliminated {original_count - merged_count} duplicate rows")

        # Generate report
        report = generate_merge_report(
            original_count, merged_count, merge_reports, MATCH_FIELDS
        )

        with open(report_file, 'w') as f:
            f.write(report)
        print(f"\nMerge report saved to: {report_file}")

        # Save merged data
        merged_df.to_csv(output_file, index=False)
        print(f"Merged data saved to: {output_file}")

        # Print report to console
        print("\n" + report)

        return 0

    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run data_loading.py first.")
        return 1
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
