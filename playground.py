import pandas as pd

# Create a DataFrame with a date column and a corresponding value column
df = pd.DataFrame({'Date': ['2023-06-05', '2023-06-10', '2023-06-12', '2023-06-17', '2023-06-19'],
                   'Value': [10, 20, 30, 40, 50]})

# Convert the 'Date' column to datetime type
df['Date'] = pd.to_datetime(df['Date'])

# Filter the DataFrame to include only rows with Saturday dates
saturdays_df = df[df['Date'].dt.dayofweek == 5]

# Reverse the filtered DataFrame to make the last Saturday the first row
reversed_df = saturdays_df.iloc[::-1]

# Calculate the cumulative sum on the filtered and reversed DataFrame
reversed_df['CumulativeSum'] = reversed_df['Value'].cumsum()

# Reverse the cumulative sum back to the original order
reversed_df = reversed_df.iloc[::-1]

# Merge the cumulative sum back into the original DataFrame
df = df.merge(reversed_df[['Date', 'CumulativeSum']], on='Date', how='left')

# Fill NaN values in the 'CumulativeSum' column with 0
df['CumulativeSum'] = df['CumulativeSum'].fillna(0)

# Print the updated DataFrame
print(df)
