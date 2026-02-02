import pandas as pd
from sklearn.preprocessing import StandardScaler
import os 
path = "C:\\Users\\hohai\\Desktop\\Big Data\\CO4033-Mendeley_data\\data\\100_Batches_IndPenSim_V3.csv"
df = pd.read_csv(path)

# Select 33 columns
df = df.iloc[:, :33]

# Identify batches
batch_start_indices = df.index[df['Time (h)'] == 0.2]

def find_batch_end_indices(df, time_col='Time (h)', reset_value=0.2):
    reset_indices = df.index[df[time_col] == reset_value]
    batch_end_indices = reset_indices - 1
    batch_end_indices = batch_end_indices[batch_end_indices >= 0]
    batch_end_indices = pd.Index(list(batch_end_indices) + [df.index[-1]])
    return batch_end_indices

batch_end_indices = find_batch_end_indices(df)
df['batch_id'] = 0

for i, (start, end) in enumerate(zip(batch_start_indices, batch_end_indices), start=1):
    df.loc[start:end, 'batch_id'] = i

def assign_control(batch_id):
    if batch_id <= 30:
        return "recipe"
    elif batch_id <= 60:
        return "operator"
    elif batch_id <= 90:
        return "APC"
    else:
        return "fault"

df['control_type'] = df['batch_id'].apply(assign_control)

# Forward/backward fill for time series
# df = df.fillna(method='ffill').fillna(method='bfill')
df = df.groupby("batch_id").apply(lambda g: g.ffill().bfill()).reset_index(drop=True)

# Normalize numeric columns
sensor_cols = [
    col for col in df.columns 
    if col not in ['Time (h)', 'batch_id', 'control_type']
]
scaler = StandardScaler()
df[sensor_cols] = scaler.fit_transform(df[sensor_cols])

# Aggregation
batch_features = df.groupby(['batch_id', 'control_type'])[sensor_cols].agg(['mean', 'std', 'max'])
batch_features.columns = [f"{c[0]}_{c[1]}" for c in batch_features.columns]
batch_features = batch_features.reset_index()

# Create output directory
output_dir = "../../data/processed"
os.makedirs(output_dir, exist_ok=True)

# Save both files
df.to_csv(f"{output_dir}/processed_timeseries.csv", index=False)
# batch_features.to_csv(f"{output_dir}/processed_batch_features.csv", index=False)

print(f"[OK] Processed {df['batch_id'].nunique()} batches")
print(f"[OK] Time series data: {df.shape}")
print(f"[OK] Batch features: {batch_features.shape}")
print(f"\nControl type distribution:")
print(batch_features['control_type'].value_counts())