import pandas as pd
import re

# Raw strings that mean "no value" in the source data
_NULL_SENTINELS = ["", "N/A", "n/a", "NA", "N/a", "unknown", "Unknown",
                   "None", "none", "null", "NULL", "nan", "NaN", "<NA>"]


def _safe_str(series: pd.Series, *str_ops) -> pd.Series:
    """Apply str ops only to non-null cells, preserving pd.NA."""
    mask_null = series.isna()
    result = series.astype(object).where(mask_null, series.astype(str))
    for op in str_ops:
        result = op(result.str)
    return result.where(~mask_null, pd.NA)


def _format_phone(phone) -> object:
    if pd.isna(phone):
        return pd.NA
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return pd.NA


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ── Normalize known null sentinels first, before any str conversion ──────
    df.replace(_NULL_SENTINELS, pd.NA, inplace=True)

    # ── Company / name fields → Title Case ───────────────────────────────────
    name_cols = [
        "companyName", "companyNameSt",
        "immediateParentName", "domesticParentName", "globalName",
        "industryType", "businessType",
    ]
    for col in name_cols:
        if col in df.columns:
            df[col] = _safe_str(df[col], lambda s: s.strip(), lambda s: s.title())

    # ── Website + social fields → lowercase ──────────────────────────────────
    social_cols = [
        "website", "websiteSt",
        "facebook", "instagram", "linkedin", "twitter", "youtube",
        "websiteredirectedto",
    ]
    for col in social_cols:
        if col in df.columns:
            df[col] = _safe_str(df[col], lambda s: s.strip(), lambda s: s.lower())

    # ── Phone number → (XXX) XXX-XXXX ────────────────────────────────────────
    if "phoneNumber" in df.columns:
        df["phoneNumber"] = df["phoneNumber"].apply(_format_phone)

    # ── Street address → Title Case ───────────────────────────────────────────
    if "streetAddress" in df.columns:
        df["streetAddress"] = _safe_str(
            df["streetAddress"], lambda s: s.strip(), lambda s: s.title()
        )

    # ── Postal code → first 5-digit sequence, or NA ───────────────────────────
    if "postalCode" in df.columns:
        df["postalCode"] = (
            df["postalCode"]
            .astype(str)
            .str.extract(r"(\d{5})")[0]
        )

    # ── Numeric fields ─────────────────────────────────────────────────────────
    num_cols = [
        "latitude", "longitude", "yearFounded",
        "Employee All Sites", "Employee this Site",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── ID fields → strip whitespace, keep as string ──────────────────────────
    id_cols = [
        "EIN", "SOSID", "dunsNumber", "companyID", "placeId", "googleId",
        "recID", "IUSA Number",
        "immediateParentDuns", "domesticParentDuns", "globalDunsNo",
        "Parent IUSA Number", "Subsidiary IUSA Number",
    ]
    for col in id_cols:
        if col in df.columns:
            df[col] = _safe_str(df[col], lambda s: s.strip())

    # ── Description / free-text fields → strip only ───────────────────────────
    desc_cols = ["companyDescription", "websiteTitle", "websiteDescription"]
    for col in desc_cols:
        if col in df.columns:
            df[col] = _safe_str(df[col], lambda s: s.strip())

    # ── NAICS / data-source fields → strip only ───────────────────────────────
    strip_cols = ["primaryNAICS", "allNAICS", "dataRollup", "dataSource"]
    for col in strip_cols:
        if col in df.columns:
            df[col] = _safe_str(df[col], lambda s: s.strip())

    # ── Date fields ───────────────────────────────────────────────────────────
    if "companyBirthday" in df.columns:
        df["companyBirthday"] = pd.to_datetime(
            df["companyBirthday"], errors="coerce"
        )

    return df


if __name__ == "__main__":
    source = pd.read_csv("fetched_data_sample.csv", low_memory=False)
    cleaned = standardize_dataframe(source)
    cleaned.to_csv("cleaned_data.csv", index=False)
    print(f"Standardization complete: {len(cleaned):,} rows written to cleaned_data.csv")
