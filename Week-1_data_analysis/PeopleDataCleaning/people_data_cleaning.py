# data_cleaning_people.py

import pandas as pd

# 1️⃣ DATA INGESTION
print("=== DATA INGESTION ===")
df = pd.read_csv("people-100.csv")

print("Shape:", df.shape)
print("\nInfo:")
print(df.info())
print("\nPreview:")
print(df.head(3))

# 2️⃣ DEDUPLICATION
print("\n=== DEDUPLICATION ===")
print("Exact duplicates found:", df.duplicated().sum())
df = df.drop_duplicates()

# 3️⃣ COLUMN MANAGEMENT
print("\n=== COLUMN MANAGEMENT ===")
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Drop irrelevant columns if they exist
drop_cols = ["index", "notes", "id"]
df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

print("Columns after cleanup:", df.columns.tolist())

# 4️⃣ MISSING VALUE HANDLING
print("\n=== MISSING VALUE HANDLING ===")
print(df.isna().sum())

# Drop rows with too many missing values (>70%)
row_thresh = int(0.3 * len(df.columns))
df = df.dropna(thresh=row_thresh)

# Fill missing values with mode (most common)
for col in df.columns:
    if df[col].isna().sum() > 0:
        if df[col].dtype == "object":
            df[col].fillna(df[col].mode()[0], inplace=True)
        else:
            df[col].fillna(df[col].median(), inplace=True)

# 5️⃣ DATA TYPE CORRECTION
print("\n=== DATA TYPE CORRECTION ===")
# Convert possible date columns
for col in df.columns:
    if "date" in col or "dob" in col or "birth" in col:
        df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)

print(df.dtypes)

# 6️⃣ FORMAT STANDARDIZATION
print("\n=== FORMAT STANDARDIZATION ===")

# Normalize all text columns
text_cols = df.select_dtypes(include="object").columns
for col in text_cols:
    df[col] = df[col].astype(str).str.strip()

# Fix inconsistent job titles (capitalize each word)
if "job_title" in df.columns:
    df["job_title"] = df["job_title"].str.title()

# Clean malformed phone numbers — keep only digits
for col in df.columns:
    if "phone" in col:
        df[col] = df[col].astype(str).str.replace(r"\D", "", regex=True)
        df[col] = df[col].apply(lambda x: x if len(x) >= 7 else None)

# Final check
print("\nCleaned Data Sample:")
print(df.head(3))

# 7️⃣ SAVE CLEANED DATA
df.to_csv("people_cleaned.csv", index=False)
print("✅ File saved successfully as 'people_cleaned.csv' in current directory.")
