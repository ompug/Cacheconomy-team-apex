"""
Duplicate Identification Script for Supabase Business Data.

This script is now aligned with the production merge logic. It still reports
strict exact duplicates for reference, but its main summary is based on the
same aggressive multi-pass matcher used by the cleaning ETL.
"""

import pandas as pd
import sys
from datetime import datetime

from merge_duplicates import (
    MATCH_FIELDS,
    MATCH_STRATEGY,
    build_match_features,
    find_multipass_duplicates,
)

# Columns to exclude from duplicate comparison
# (these fields can legitimately have the same values across different records)
EXCLUDE_COLUMNS = ['postalCode']


def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV data into DataFrame."""
    print(f"Loading data from: {filepath}")
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def find_exact_duplicates(df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
    """Find rows that are exact duplicates (all fields match except excluded columns)."""
    # Filter out excluded columns before comparison
    if exclude_cols:
        comparison_df = df.drop(columns=exclude_cols, errors='ignore')
    else:
        comparison_df = df

    # Mark all duplicates (keep=False marks ALL occurrences, not just subsequent ones)
    duplicate_mask = comparison_df.duplicated(keep=False)
    duplicates = df[duplicate_mask].copy()

    if len(duplicates) > 0:
        # Assign group IDs to duplicate sets
        # Group by comparison columns to identify which rows are duplicates of each other
        duplicates['duplicate_group'] = duplicates.groupby(list(comparison_df.columns)).ngroup()

    return duplicates


def analyze_field_duplicates(df: pd.DataFrame) -> dict:
    """Analyze duplicate values in each field."""
    field_stats = {}

    for col in df.columns:
        total = len(df)
        non_null = df[col].notna().sum()
        unique_values = df[col].nunique()
        duplicate_values = df[col].duplicated().sum()

        field_stats[col] = {
            'total_rows': total,
            'non_null_count': non_null,
            'null_count': total - non_null,
            'unique_values': unique_values,
            'duplicate_value_count': duplicate_values,
            'duplicate_rate': round(duplicate_values / total * 100, 2) if total > 0 else 0
        }

    return field_stats


def generate_summary_report(
    df: pd.DataFrame,
    exact_duplicates: pd.DataFrame,
    multi_pass_duplicates: pd.DataFrame,
    multi_pass_stats: dict,
    field_stats: dict,
    exclude_cols: list = None,
) -> str:
    """Generate a text summary report."""
    report = []
    report.append("=" * 70)
    report.append("DUPLICATE IDENTIFICATION REPORT")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 70)
    report.append("")

    # Excluded columns info
    if exclude_cols:
        report.append("EXCLUDED COLUMNS")
        report.append("-" * 40)
        report.append(f"The following columns were excluded from duplicate comparison:")
        for col in exclude_cols:
            report.append(f"  - {col}")
        report.append("")

    # Overall statistics
    report.append("OVERALL STATISTICS")
    report.append("-" * 40)
    report.append(f"Total rows in dataset:        {len(df):,}")
    report.append(f"Total columns:                {len(df.columns)}")
    report.append(f"Exact duplicate rows:         {len(exact_duplicates):,}")
    report.append(f"Multi-pass duplicate rows:    {len(multi_pass_duplicates):,}")
    report.append(f"Multi-pass duplicate groups:  {multi_pass_stats.get('group_count', 0):,}")
    report.append(
        f"Estimated rows eliminated:    "
        f"{len(multi_pass_duplicates) - multi_pass_stats.get('group_count', 0):,}"
    )
    report.append(f"Match strategy:               {MATCH_STRATEGY}")

    if len(exact_duplicates) > 0:
        num_groups = exact_duplicates['duplicate_group'].nunique()
        report.append(f"Number of duplicate groups:   {num_groups:,}")
        report.append(f"Duplicate percentage:         {len(exact_duplicates) / len(df) * 100:.2f}%")
    else:
        report.append("No exact duplicates found (all rows are unique)")

    report.append("")
    report.append("=" * 70)
    report.append("MULTI-PASS BREAKDOWN")
    report.append("-" * 70)
    report.append(f"{'Pass':<24} {'Rows':>12} {'Groups':>12}")
    report.append("-" * 70)

    for pass_stat in multi_pass_stats.get("pass_stats", []):
        report.append(
            f"{pass_stat['name']:<24} {pass_stat['rows_matched']:>12,} "
            f"{pass_stat['raw_groups']:>12,}"
        )

    report.append("")
    report.append("=" * 70)
    report.append("PER-FIELD DUPLICATE ANALYSIS")
    report.append("(Shows how many duplicate values exist in each column)")
    report.append("-" * 70)
    report.append(f"{'Column Name':<35} {'Duplicates':>12} {'Rate':>8} {'Unique':>10}")
    report.append("-" * 70)

    # Sort by duplicate rate descending
    sorted_fields = sorted(field_stats.items(),
                          key=lambda x: x[1]['duplicate_rate'],
                          reverse=True)

    for col, stats in sorted_fields:
        report.append(
            f"{col:<35} {stats['duplicate_value_count']:>12,} "
            f"{stats['duplicate_rate']:>7.1f}% {stats['unique_values']:>10,}"
        )

    report.append("")
    report.append("=" * 70)
    report.append("COLUMN SCHEMA")
    report.append("-" * 70)

    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = field_stats[col]['null_count']
        report.append(f"{col:<35} {dtype:<15} (nulls: {null_count:,})")

    report.append("")
    report.append("=" * 70)
    report.append("END OF REPORT")
    report.append("=" * 70)

    return "\n".join(report)


def main() -> int:
    input_file = "fetched_data_sample.csv"
    duplicates_file = "duplicate_rows.csv"
    multi_pass_duplicates_file = "multipass_duplicate_rows.csv"
    summary_file = "duplicate_summary.txt"

    try:
        # Load data
        df = load_data(input_file)

        # Find exact duplicates
        print("\nFinding exact duplicates (all fields must match)...")
        if EXCLUDE_COLUMNS:
            print(f"Excluding columns from comparison: {', '.join(EXCLUDE_COLUMNS)}")
        exact_duplicates = find_exact_duplicates(df, exclude_cols=EXCLUDE_COLUMNS)
        print(f"Found {len(exact_duplicates)} duplicate rows")

        if len(exact_duplicates) > 0:
            num_groups = exact_duplicates['duplicate_group'].nunique()
            print(f"These form {num_groups} duplicate groups")

        print("\nFinding duplicates with the production multi-pass matcher...")
        multi_pass_duplicates, multi_pass_stats = find_multipass_duplicates(df)
        print(
            f"Found {len(multi_pass_duplicates)} multi-pass duplicate rows "
            f"in {multi_pass_stats.get('group_count', 0):,} groups"
        )
        print(f"Match strategy: {MATCH_STRATEGY}")

        # Analyze per-field duplicates
        print("\nAnalyzing per-field duplicate values...")
        field_stats = analyze_field_duplicates(df)

        # Generate and save summary report
        print("\nGenerating summary report...")
        report = generate_summary_report(
            df,
            exact_duplicates,
            multi_pass_duplicates,
            multi_pass_stats,
            field_stats,
            exclude_cols=EXCLUDE_COLUMNS,
        )

        with open(summary_file, 'w') as f:
            f.write(report)
        print(f"Summary saved to: {summary_file}")

        # Save duplicate rows to CSV
        if len(exact_duplicates) > 0:
            exact_duplicates.to_csv(duplicates_file, index=False)
            print(f"Duplicate rows saved to: {duplicates_file}")
        else:
            print("No duplicate rows to export.")

        if len(multi_pass_duplicates) > 0:
            multi_pass_duplicates.to_csv(multi_pass_duplicates_file, index=False)
            print(
                f"Multi-pass duplicate rows saved to: "
                f"{multi_pass_duplicates_file}"
            )
        else:
            print("No multi-pass duplicate rows to export.")

        # Print report to console
        print("\n" + report)

        return 0

    except FileNotFoundError:
        print(f"Error: {input_file} not found. Run data_loading.py first.")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
