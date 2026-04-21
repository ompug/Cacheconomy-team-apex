"""
Duplicate Merging Script for Supabase Business Data.

The original version only merged rows when `companyName`, `streetAddress`, and
`postalCode` matched exactly. This version keeps the same "merge surviving row
by filling from related rows" behavior, but upgrades duplicate detection to a
multi-pass normalized matcher that is better suited to a multi-source business
dataset.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Set, Tuple

import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

# Legacy fields kept for compatibility with older callers and reports.
MATCH_FIELDS = ["companyName", "streetAddress", "postalCode"]

# High-level description used by the ETL and reports.
MATCH_STRATEGY = "aggressive normalized multi-pass grouping"

# Primary key field (used to identify which record to keep as "primary")
PRIMARY_KEY = "dunsNumber"

# Fields to exclude from comparison/merging (metadata fields, etc.)
EXCLUDE_FROM_MERGE = ["id", "created_at", "updated_at"]

ENTITY_ID_COLUMNS = [
    "dunsNumber",
    "companyID",
    "placeId",
    "googleId",
    "SOSID",
    "recID",
]

SOCIAL_HANDLE_COLUMNS = [
    "facebook",
    "instagram",
    "linkedin",
    "twitter",
    "youtube",
]

NAME_COLUMNS = ["companyName", "companyNameSt"]
ADDRESS_COLUMNS = ["streetAddress"]
POSTAL_COLUMNS = ["postalCode"]
PHONE_COLUMNS = ["phoneNumber"]

COMPANY_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "l.l.c",
    "limited",
    "ltd",
    "pllc",
    "lp",
    "llp",
    "pc",
    "the",
}

ADDRESS_REPLACEMENTS = {
    r"\bst\b": "street",
    r"\brd\b": "road",
    r"\bave\b": "avenue",
    r"\bblvd\b": "boulevard",
    r"\bdr\b": "drive",
    r"\bln\b": "lane",
    r"\bct\b": "court",
    r"\bpl\b": "place",
    r"\bpkwy\b": "parkway",
    r"\bhwy\b": "highway",
    r"\bsuite\b": "ste",
    r"\bunit\b": "ste",
    r"\bapt\b": "ste",
    r"\bfl\b": "floor",
}


class UnionFind:
    """Simple union-find for connected duplicate grouping."""

    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return

        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root

        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1

# =============================================================================
# CORE FUNCTIONS
# =============================================================================


def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV data into DataFrame."""
    print(f"Loading data from: {filepath}")
    df = pd.read_csv(filepath, low_memory=False)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def _series_or_empty(df: pd.DataFrame, column_names: Iterable[str]) -> pd.Series:
    for column_name in column_names:
        if column_name in df.columns:
            return df[column_name]
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    if not text or text == "nan":
        return ""
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_name(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""

    parts = text.split()
    while parts and parts[-1] in COMPANY_SUFFIXES:
        parts.pop()
    return " ".join(parts).strip()


def _normalize_name_core(value: Any) -> str:
    text = _normalize_name(value)
    if not text:
        return ""

    tokens = [token for token in text.split() if token not in {"and"}]
    if not tokens:
        return ""
    return " ".join(tokens)


def _normalize_address(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""

    for pattern, replacement in ADDRESS_REPLACEMENTS.items():
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"\bste\s+[a-z0-9\-]+\b", "", text)
    text = re.sub(r"\bfloor\s+[a-z0-9\-]+\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _address_number(value: Any) -> str:
    text = _normalize_address(value)
    if not text:
        return ""
    match = re.match(r"(\d+)", text)
    return match.group(1) if match else ""


def _normalize_postal(value: Any) -> str:
    if pd.isna(value):
        return ""
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 5:
        return digits[:5]
    return digits


def _normalize_domain(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    text = re.sub(r"^https?\s+", "", text)
    text = re.sub(r"^https?://", "", str(value).strip().lower())
    text = re.sub(r"^www\d*\.", "", text)
    text = text.split("/", 1)[0]
    text = text.split("?", 1)[0]
    text = text.split("#", 1)[0]
    text = text.strip().strip(".")
    return text if "." in text else ""


def _normalize_phone(value: Any) -> str:
    if pd.isna(value):
        return ""
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) >= 10 else ""


def _normalize_identifier(value: Any) -> str:
    text = _normalize_text(value)
    return text.replace(" ", "")


def is_missing_value(value: Any) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def build_match_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build normalized fields used by the multi-pass matcher."""
    company_name = _series_or_empty(df, NAME_COLUMNS)
    street_address = _series_or_empty(df, ADDRESS_COLUMNS)
    postal_code = _series_or_empty(df, POSTAL_COLUMNS)
    website = _series_or_empty(df, ["website"])
    phone = _series_or_empty(df, PHONE_COLUMNS)

    features = pd.DataFrame(index=df.index)
    features["name_norm"] = company_name.map(_normalize_name)
    features["name_core"] = company_name.map(_normalize_name_core)
    features["address_norm"] = street_address.map(_normalize_address)
    features["address_number"] = street_address.map(_address_number)
    features["zip5"] = postal_code.map(_normalize_postal)
    features["website_domain"] = website.map(_normalize_domain)
    features["phone_norm"] = phone.map(_normalize_phone)

    for column in ENTITY_ID_COLUMNS:
        features[f"id::{column}"] = _series_or_empty(df, [column]).map(
            _normalize_identifier
        )

    for column in ["website"]:
        features[f"url::{column}"] = _series_or_empty(df, [column]).map(
            _normalize_domain
        )

    for column in SOCIAL_HANDLE_COLUMNS:
        features[f"social::{column}"] = _series_or_empty(df, [column]).map(
            _normalize_domain
        )

    features["name_token_count"] = features["name_core"].map(
        lambda value: len(value.split()) if value else 0
    )
    features["name_length"] = features["name_core"].map(len)

    return features


def get_match_pass_definitions(
    df: pd.DataFrame, features: pd.DataFrame
) -> List[Dict[str, Any]]:
    """Return duplicate-detection passes ordered from strongest to weakest."""
    pass_definitions: List[Dict[str, Any]] = [
        {
            "name": "duns_exact",
            "fields": ["id::dunsNumber"],
            "description": "Exact normalized DUNS match",
            "max_group_size": 15,
        }
    ]

    for column in ENTITY_ID_COLUMNS:
        feature_name = f"id::{column}"
        if column == PRIMARY_KEY or feature_name not in features.columns:
            continue
        if column in df.columns:
            pass_definitions.append(
                {
                    "name": f"{column}_exact",
                    "fields": [feature_name],
                    "description": f"Exact normalized {column} match",
                    "max_group_size": 15,
                }
            )

    if "website" in df.columns:
        pass_definitions.append(
            {
                "name": "domain_address_zip",
                "fields": ["website_domain", "address_number", "zip5"],
                "description": "Website domain with street number and ZIP5",
                "max_group_size": 12,
            }
        )

    for column in ["facebook", "linkedin", "twitter", "instagram", "youtube"]:
        if column in df.columns:
            pass_definitions.append(
                {
                    "name": f"name_{column}",
                    "fields": ["name_core", f"social::{column}"],
                    "description": f"Normalized name plus {column} handle",
                    "min_name_tokens": 2,
                    "min_name_length": 8,
                    "max_group_size": 20,
                }
            )

    pass_definitions.extend(
        [
            {
                "name": "phone_address_zip",
                "fields": ["phone_norm", "address_number", "zip5"],
                "description": "Phone with street number and ZIP5",
                "max_group_size": 12,
            },
            {
                "name": "name_domain",
                "fields": ["name_core", "website_domain"],
                "description": "Normalized name plus website domain",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 15,
            },
            {
                "name": "name_phone",
                "fields": ["name_core", "phone_norm"],
                "description": "Normalized name plus phone",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 15,
            },
            {
                "name": "name_domain_zip",
                "fields": ["name_core", "website_domain", "zip5"],
                "description": "Normalized name, website domain, and ZIP5",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 12,
            },
            {
                "name": "name_phone_zip",
                "fields": ["name_core", "phone_norm", "zip5"],
                "description": "Normalized name, phone, and ZIP5",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 12,
            },
            {
                "name": "name_address_zip",
                "fields": ["name_core", "address_norm", "zip5"],
                "description": "Normalized name, address, and ZIP5",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 12,
            },
            {
                "name": "name_address",
                "fields": ["name_core", "address_norm"],
                "description": "Normalized name and address",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 10,
            },
            {
                "name": "name_address_number_zip",
                "fields": ["name_core", "address_number", "zip5"],
                "description": "Normalized name with street number and ZIP5",
                "min_name_tokens": 2,
                "min_name_length": 8,
                "max_group_size": 12,
            },
        ]
    )

    return pass_definitions


def _key_frame(features: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
    return features[fields].copy()


def find_multipass_duplicates(
    df: pd.DataFrame, primary_key: str = PRIMARY_KEY
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Detect duplicates using connected groups built from multiple passes."""
    if len(df) == 0:
        return df.copy(), {"strategy": MATCH_STRATEGY, "pass_stats": []}

    features = build_match_features(df)
    pass_definitions = get_match_pass_definitions(df, features)
    union_find = UnionFind(len(df))
    row_passes: Dict[int, Set[str]] = defaultdict(set)
    pass_stats: List[Dict[str, Any]] = []

    for pass_definition in pass_definitions:
        key_frame = _key_frame(features, pass_definition["fields"])
        valid_mask = pd.Series(True, index=key_frame.index)
        for field in pass_definition["fields"]:
            valid_mask &= key_frame[field] != ""
        if "min_name_tokens" in pass_definition:
            valid_mask &= features["name_token_count"] >= pass_definition["min_name_tokens"]
        if "min_name_length" in pass_definition:
            valid_mask &= features["name_length"] >= pass_definition["min_name_length"]

        if not valid_mask.any():
            pass_stats.append(
                {
                    "name": pass_definition["name"],
                    "description": pass_definition["description"],
                    "raw_groups": 0,
                    "rows_matched": 0,
                    "skipped_large_groups": 0,
                }
            )
            continue

        valid_keys = key_frame.loc[valid_mask]
        duplicate_mask = valid_keys.duplicated(keep=False)
        duplicate_keys = valid_keys.loc[duplicate_mask]

        if duplicate_keys.empty:
            pass_stats.append(
                {
                    "name": pass_definition["name"],
                    "description": pass_definition["description"],
                    "raw_groups": 0,
                    "rows_matched": 0,
                    "skipped_large_groups": 0,
                }
            )
            continue

        raw_group_count = 0
        matched_rows: Set[int] = set()
        skipped_large_groups = 0
        grouped = duplicate_keys.groupby(pass_definition["fields"], sort=False).groups

        for positions in grouped.values():
            member_positions = list(positions)
            if len(member_positions) < 2:
                continue
            max_group_size = pass_definition.get("max_group_size")
            if max_group_size and len(member_positions) > max_group_size:
                skipped_large_groups += 1
                continue
            raw_group_count += 1
            leader = member_positions[0]
            for member in member_positions:
                matched_rows.add(member)
                row_passes[member].add(pass_definition["name"])
            for member in member_positions[1:]:
                union_find.union(leader, member)

        pass_stats.append(
            {
                "name": pass_definition["name"],
                "description": pass_definition["description"],
                "raw_groups": raw_group_count,
                "rows_matched": len(matched_rows),
                    "skipped_large_groups": skipped_large_groups,
            }
        )

    root_to_members: Dict[int, List[int]] = defaultdict(list)
    for position in range(len(df)):
        root_to_members[union_find.find(position)].append(position)

    duplicate_groups = [
        members for members in root_to_members.values() if len(members) > 1
    ]
    duplicate_groups.sort(key=lambda members: (len(members), members[0]), reverse=True)

    if not duplicate_groups:
        duplicates = df.iloc[0:0].copy()
        return duplicates, {
            "strategy": MATCH_STRATEGY,
            "pass_stats": pass_stats,
            "rows_involved": 0,
            "group_count": 0,
        }

    duplicate_positions = sorted(
        position for members in duplicate_groups for position in members
    )
    duplicates = df.iloc[duplicate_positions].copy()

    group_id_by_position: Dict[int, int] = {}
    group_passes: Dict[int, Set[str]] = {}
    for group_id, members in enumerate(duplicate_groups):
        group_id_by_position.update({position: group_id for position in members})
        group_passes[group_id] = set().union(
            *(row_passes.get(position, set()) for position in members)
        )

    duplicates["duplicate_group"] = [
        group_id_by_position[position] for position in duplicate_positions
    ]
    duplicates["matched_passes"] = [
        ",".join(sorted(group_passes[group_id_by_position[position]]))
        for position in duplicate_positions
    ]

    return duplicates, {
        "strategy": MATCH_STRATEGY,
        "pass_stats": pass_stats,
        "rows_involved": len(duplicate_positions),
        "group_count": len(duplicate_groups),
        "duplicate_groups": duplicate_groups,
    }


def merge_duplicate_group(
    group_df: pd.DataFrame, primary_key: str, exclude_cols: List[str]
) -> Tuple[pd.Series, dict]:
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
    match_passes = sorted(
        {
            pass_name
            for value in group_df.get("matched_passes", pd.Series([], dtype="object"))
            for pass_name in str(value).split(",")
            if pass_name
        }
    )
    merge_info = {
        "records_merged": len(group_df),
        "primary_keys_involved": group_df[primary_key].tolist()
        if primary_key in group_df.columns
        else [],
        "fields_filled": [],
        "match_passes": match_passes,
        "group_size": len(group_df),
    }

    # Sort by number of non-null values (most complete record first)
    # Reset index to ensure proper iloc access
    value_columns = [column for column in group_df.columns if column not in exclude_cols]
    null_counts = group_df[value_columns].apply(
        lambda column: column.map(is_missing_value)
    ).sum(axis=1)
    sorted_indices = null_counts.sort_values().index
    sorted_df = group_df.loc[sorted_indices].reset_index(drop=True)

    # Start with the most complete record as base
    merged = sorted_df.iloc[0].copy()
    kept_primary_key = merged.get(primary_key) if primary_key in sorted_df.columns else None

    # Columns to process (exclude certain fields)
    cols_to_merge = [
        c
        for c in group_df.columns
        if c not in exclude_cols and c not in {"duplicate_group", "matched_passes"}
    ]

    # Fill in nulls or blanks from other records.
    for idx in range(1, len(sorted_df)):
        other_row = sorted_df.iloc[idx]
        for col in cols_to_merge:
            if is_missing_value(merged[col]) and not is_missing_value(other_row[col]):
                merged[col] = other_row[col]
                merge_info["fields_filled"].append(
                    {
                        "field": col,
                        "value": str(other_row[col])[:50],
                        "from_record": str(other_row.get(primary_key, "unknown")),
                    }
                )

    merge_info["kept_primary_key"] = str(kept_primary_key)

    return merged, merge_info


def merge_all_duplicates_with_stats(
    df: pd.DataFrame,
    match_fields: List[str],
    primary_key: str,
    exclude_cols: List[str],
) -> Tuple[pd.DataFrame, List[dict], Dict[str, Any]]:
    """
    Merge all duplicate records in the DataFrame.

    Args:
        df: Input DataFrame
        match_fields: Legacy argument retained for compatibility
        primary_key: Primary key column
        exclude_cols: Columns to exclude from merging

    Returns:
        Tuple of (merged_df, merge_reports, match_stats)
    """
    duplicates, match_stats = find_multipass_duplicates(df, primary_key)

    if len(duplicates) == 0:
        print("No duplicates found based on the multi-pass matcher.")
        return df, [], match_stats

    num_groups = duplicates["duplicate_group"].nunique()
    print(f"Found {len(duplicates)} duplicate rows in {num_groups} groups")
    for pass_stat in match_stats["pass_stats"]:
        if pass_stat["raw_groups"] == 0:
            continue
        print(
            f"  - {pass_stat['name']}: {pass_stat['rows_matched']:,} rows in "
            f"{pass_stat['raw_groups']:,} raw groups"
        )

    # Get non-duplicate rows (will be kept as-is)
    non_dup_mask = ~df.index.isin(duplicates.index)
    result_rows = df.loc[non_dup_mask].to_dict(orient="records")
    merge_reports = []

    # Process each duplicate group
    unique_groups = duplicates["duplicate_group"].unique()
    total_groups = len(unique_groups)

    for i, group_id in enumerate(unique_groups):
        group_df = duplicates[duplicates["duplicate_group"] == group_id]

        if len(group_df) == 0:
            continue

        try:
            merged_row, merge_info = merge_duplicate_group(
                group_df, primary_key, exclude_cols + ["duplicate_group"]
            )
            merge_info["group_id"] = group_id
            merge_reports.append(merge_info)

            result_rows.append(merged_row.to_dict())
        except Exception as e:
            print(f"Error processing group {group_id}: {e}")
            print(f"Group size: {len(group_df)}")
            raise

        # Progress update every 10000 groups
        if (i + 1) % 10000 == 0:
            print(f"Processed {i+1:,} / {total_groups:,} groups ({(i+1)/total_groups*100:.1f}%)")

    # Combine rows once at the end to avoid concatenating hundreds of thousands
    # of tiny DataFrames during large Supabase runs.
    result_df = pd.DataFrame(result_rows)
    result_df = result_df.reindex(columns=df.columns)

    return result_df, merge_reports, match_stats


def merge_all_duplicates(
    df: pd.DataFrame,
    match_fields: List[str],
    primary_key: str,
    exclude_cols: List[str],
) -> Tuple[pd.DataFrame, List[dict]]:
    """Backward-compatible wrapper that omits the match stats."""
    result_df, merge_reports, _ = merge_all_duplicates_with_stats(
        df, match_fields, primary_key, exclude_cols
    )
    return result_df, merge_reports


def generate_merge_report(
    original_count: int,
    merged_count: int,
    merge_reports: List[dict],
    match_fields: List[str],
    match_stats: Dict[str, Any] | None = None,
) -> str:
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
    report.append(f"Legacy match fields: {', '.join(match_fields)}")
    report.append(f"Match strategy: {MATCH_STRATEGY}")
    report.append("")

    # Summary statistics
    report.append("SUMMARY")
    report.append("-" * 40)
    report.append(f"Original row count:     {original_count:,}")
    report.append(f"Merged row count:       {merged_count:,}")
    report.append(f"Rows eliminated:        {original_count - merged_count:,}")
    report.append(f"Duplicate groups found: {len(merge_reports):,}")
    report.append("")

    if match_stats:
        report.append("PASS BREAKDOWN")
        report.append("-" * 40)
        for pass_stat in match_stats.get("pass_stats", []):
            report.append(
                f"{pass_stat['name']:<24} groups={pass_stat['raw_groups']:>8,} "
                f"rows={pass_stat['rows_matched']:>8,}"
            )
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
            report.append(
                f"  Match passes: {', '.join(merge_info.get('match_passes', [])) or 'n/a'}"
            )

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
        print(f"Match strategy: {MATCH_STRATEGY}")

        # Find and merge duplicates
        print("\nFinding and merging duplicates...")
        merged_df, merge_reports, match_stats = merge_all_duplicates_with_stats(
            df, MATCH_FIELDS, PRIMARY_KEY, EXCLUDE_FROM_MERGE
        )

        merged_count = len(merged_df)
        print(f"\nMerge complete: {original_count} -> {merged_count} rows")
        print(f"Eliminated {original_count - merged_count} duplicate rows")

        # Generate report
        report = generate_merge_report(
            original_count, merged_count, merge_reports, MATCH_FIELDS, match_stats
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
