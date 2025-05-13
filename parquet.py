import pandas as pd

# --- Pandas Display Options ---
# Set these before printing the DataFrame

# Display all columns (None means no limit)
pd.set_option('display.max_columns', None)

# Optional: Increase the display width (number of characters per line)
# if your terminal is wide enough and lines are still wrapping.
# pd.set_option('display.width', 1000) # Adjust as needed, or comment out

# Optional: Display more rows if needed (None for all rows, be careful with large DFs)
# pd.set_option('display.max_rows', None)

# Optional: Increase max_rows for df.info() if you have > 100 columns
# pd.set_option('display.max_info_rows', 1000)

df = pd.read_parquet('Output_Parquet_Directory/HLTH_CD_ANR.parquet')

# Print the schema-like information using df.info()
print("DataFrame Schema Information:")
df.info()

# --- Alternative: Print only Data Types ---
# If you *only* want the column names and their types:
# print("\nColumn Data Types:")
# print(df.dtypes)