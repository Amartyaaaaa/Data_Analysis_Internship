import pandas as pd

print("=== DATA INGESTION ===")
df = pd.read_csv("organizations-100.csv")

print("Shape:", df.shape)
print("\nInfo:")
print(df.info())
print("\nPreview:")
print(df.head(3))

print("\n=== DEDUPLICATION ===")
print("Exact duplicates found:", df.duplicated().sum())
df = df.drop_duplicates()

print("\n=== COLUMN MANAGEMENT ===")

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

drop_cols = ["index", "notes", "unnecessary_id"]
df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

print("Columns after cleanup:", df.columns.tolist())

print("\n=== MISSING VALUE HANDLING ===")
print(df.isna().sum())


row_thresh = int(0.3 * len(df.columns))
df = df.dropna(thresh=row_thresh)


for col in df.columns:
    if df[col].isna().sum() > 0:
        if df[col].dtype == "object":
            df[col].fillna(df[col].mode()[0], inplace=True)
        else:
            df[col].fillna(df[col].median(), inplace=True)


print("\n=== DATA TYPE CORRECTION ===")

for col in df.columns:
    if "date" in col or "founded" in col or "established" in col:
        df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)

for col in df.columns:
    if df[col].dtype == "object":
        if df[col].str.replace(".", "", 1).str.isnumeric().any():
            df[col] = pd.to_numeric(df[col], errors="ignore")

print(df.dtypes)

print("\n=== FORMAT STANDARDIZATION ===")

text_cols = df.select_dtypes(include="object").columns
for col in text_cols:
    df[col] = df[col].astype(str).str.strip()

for col in ["organization_name", "city", "country"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.title()

for col in df.columns:
    if "email" in col or "website" in col:
        df[col] = df[col].astype(str).str.strip().str.lower()

print("\nCleaned Data Sample:")
print(df.head(3))

df.to_csv("organizations_cleaned.csv", index=False)
print("✅ File saved successfully as 'organizations_cleaned.csv' in current directory.")
