import pandas as pd
import os

# === 1. DATA INGESTION ===
print("=== DATA INGESTION ===")

# Use absolute path to ensure consistent behavior
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "customers-100.csv")
output_path = os.path.join(base_dir, "customers_cleaned.csv")

df = pd.read_csv(input_path)

print("Shape:", df.shape)
print("\nInfo:")
print(df.info())
print("\nPreview:")
print(df.head(3))

# === 2. DEDUPLICATION ===
print("\n=== DEDUPLICATION ===")

print("Exact duplicates found:", df.duplicated().sum())
df = df.drop_duplicates()

print("Duplicate Customer IDs found:", df["Customer Id"].duplicated().sum())
df = df.drop_duplicates(subset="Customer Id", keep="first")

# === 3. COLUMN MANAGEMENT ===
print("\n=== COLUMN MANAGEMENT ===")

df = df.drop(columns=["Index"], errors="ignore")

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

desired_order = [
    "customer_id", "first_name", "last_name", "email",
    "phone_1", "phone_2", "company", "city", "country",
    "subscription_date", "website"
]
df = df.reindex(columns=desired_order)

print("Columns after management:", df.columns.tolist())

# === 4. MISSING VALUE HANDLING ===
print("\n=== MISSING VALUE HANDLING ===")

print(df.isna().sum())

# Drop rows with more than 70% missing values
row_thresh = int(0.3 * len(df.columns))
df = df.dropna(thresh=row_thresh)

# Fill missing values with the most frequent (mode)
for col in df.columns:
    if df[col].isna().sum() > 0:
        df[col].fillna(df[col].mode()[0], inplace=True)

# === 5. DATA TYPE CORRECTION ===
print("\n=== DATA TYPE CORRECTION ===")

# Convert subscription_date to datetime
df["subscription_date"] = pd.to_datetime(df["subscription_date"], errors="coerce")

print(df.dtypes)

# === 6. FORMAT STANDARDIZATION ===
print("\n=== FORMAT STANDARDIZATION ===")

text_cols = ["first_name", "last_name", "city", "country"]
for col in text_cols:
    df[col] = df[col].astype(str).str.strip().str.title()

df["email"] = df["email"].astype(str).str.strip().str.lower()
df["website"] = df["website"].astype(str).str.strip().str.lower()

# === FINAL CHECK ===
print("\nCleaned Data Sample:")
print(df.head(3))

# === SAVE CLEANED DATASET ===
df.to_csv(output_path, index=False)
print(f"✅ File saved successfully as '{output_path}'")
