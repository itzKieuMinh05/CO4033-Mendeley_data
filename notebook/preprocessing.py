import os
import pandas as pd

path = "D:\\Desktop\\Project_BioPharma\\data\\raw\\100_Batches_IndPenSim_V3.csv"
df = pd.read_csv(path)

# df.head(20)
# print("Data shape:", df.shape)
# df = df.iloc[:, :33]


df = df.iloc[:, :33]
# print("\nAfter selecting first 33 columns:")
# print(df.shape)
# print(df.info())

batch_start_indices = df.index[df['Time (h)'] == 0.2]

print("\nBatch start indices:")
print(batch_start_indices[:10])
print("Total batches found:", len(batch_start_indices))

def find_batch_end_indices(df, time_col='Time (h)', reset_value=0.2):
    reset_indices = df.index[df[time_col] == reset_value]
    batch_end_indices = reset_indices - 1
    batch_end_indices = batch_end_indices[batch_end_indices >= 0]
    batch_end_indices = batch_end_indices.append(
        pd.Index([df.index[-1]])
    )
    return batch_end_indices

batch_end_indices = find_batch_end_indices(df)

print("\nBatch end indices:")
print(batch_end_indices[:10])

df['batch_id'] = 0

for i, (start, end) in enumerate(
    zip(batch_start_indices, batch_end_indices), start=1
):
    df.loc[start:end, 'batch_id'] = i
    
print("\nLast 10 rows after adding batch_id:")
print(df.tail(10))

print("\nNumber of batches:", df['batch_id'].nunique())


