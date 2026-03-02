import pandas as pd
from sklearn.preprocessing import StandardScaler
import os

path = "C:\\Users\\hohai\\Desktop\\Data Proj\\CO4033-Mendeley_data\\data\\weather-vn-1.csv"
df = pd.read_csv(path)

# Display basic info
print("=== DATASET INFO ===")
print(df.info())
print("\n=== FIRST 5 ROWS ===")
print(df.head())
print(f"\nDataset shape: {df.shape}")

# Handle missing values
print("\n=== MISSING VALUES ===")
print(df.isnull().sum())
df = df.fillna(df.mean(numeric_only=True))  # Fill with mean for numeric columns

# Identify numeric columns
numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
print(f"\nNumeric columns: {numeric_cols}")

# Normalize numeric data
if numeric_cols:
    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    print(f"[OK] Normalized {len(numeric_cols)} numeric columns")

# Create output directory
output_dir = "../../data/processed"
os.makedirs(output_dir, exist_ok=True)

# Save processed data
df.to_csv(f"{output_dir}/processed_weather.csv", index=False)
print(f"\n[OK] Processed data saved to {output_dir}/processed_weather.csv")
print(f"[OK] Final shape: {df.shape}")