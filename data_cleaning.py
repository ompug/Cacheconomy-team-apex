import pandas as pd
import re

df = pd.read_csv("fetched_data_sample.csv", low_memory=False)

# -----------------------
# PLACEHOLDERS (TEXT ONLY)
# -----------------------
UNKNOWN = "Unknown"
UNKNOWN_ADDY = "unknown addy"
NO_PHONE = "no phone"
NO_WEBSITE = "no website"
NO_SOCIAL = "no social"
NO_ZIP = "no zip"
NO_DESC = "No description available"

# -----------------------
# COMPANY / NAME FIELDS
# -----------------------
name_cols = [
    "companyName", "companyNameSt",
    "immediateParentName", "domesticParentName", "globalName"
]

for col in name_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()

# -----------------------
# WEBSITE + SOCIALS
# -----------------------
social_cols = [
    "website", "websiteSt", "facebook",
    "instagram", "linkedin", "twitter",
    "youtube", "websiteredirectedto"
]

for col in social_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

# -----------------------
# PHONE NUMBER
# -----------------------
def format_phone(phone):
    if pd.isna(phone):
        return NO_PHONE
    
    digits = re.sub(r"\D", "", str(phone))
    
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    
    return NO_PHONE

if "phoneNumber" in df.columns:
    df["phoneNumber"] = df["phoneNumber"].apply(format_phone)

# -----------------------
# ADDRESS
# -----------------------
if "streetAddress" in df.columns:
    df["streetAddress"] = (
        df["streetAddress"]
        .astype(str)
        .str.strip()
        .str.title()
    )

# -----------------------
# ZIP CODE
# -----------------------
if "postalCode" in df.columns:
    df["postalCode"] = (
        df["postalCode"]
        .astype(str)
        .str.extract(r"(\d{5})")[0]
    )

# -----------------------
# NUMERIC FIELDS (KEEP NaN - DO NOT TOUCH)
# -----------------------
num_cols = [
    "latitude", "longitude", "yearFounded",
    "Employee All Sites", "Employee this Site"
]

for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# -----------------------
# IDS
# -----------------------
id_cols = [
    "EIN", "dunsNumber", "companyID",
    "placeId", "googleId", "SOSID",
    "recID", "IUSA Number"
]

for col in id_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# -----------------------
# DESCRIPTIONS
# -----------------------
desc_cols = ["companyDescription", "websiteDescription", "websiteTitle"]

for col in desc_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# -----------------------
# MISSING VALUE CLEANUP (TEXT ONLY)
# -----------------------
df.replace(
    ["", "N/A", "n/a", "unknown", "None", "null"],
    pd.NA,
    inplace=True
)

# -----------------------
# FILL TEXT COLUMNS ONLY
# -----------------------
text_cols = df.select_dtypes(include=["object"]).columns

for col in text_cols:
    df[col] = df[col].fillna(UNKNOWN)

# -----------------------
# SAVE OUTPUT
# -----------------------
df.to_csv("cleaned_data.csv", index=False)

print("Standardization complete (numeric-safe) ✅")