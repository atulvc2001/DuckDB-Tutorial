# %%
import pandas as pd
import glob
import time
import duckdb
# %%
conn = duckdb.connect()
# %%
# pandas
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))
# %%
# DuckDB
cur_time = time.time()
conn.execute("""
    SELECT *
    FROM read_csv_auto('dataset/*.csv', header=True)
    LIMIT 10    
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
# %%
# Create a Virtual Table
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
""").df()
conn.register('df_view',df)
conn.execute("DESCRIBE df_view").df()
# %%
# Counting Rows
conn.execute("SELECT COUNT(*) FROM df_view").df()
# %%
# Drop nulls
df.isnull().sum()
df = df.dropna(how="all")

# Notice we use df and not df_view
# With DuckDB you can run SQL queries on top of Pandas dataframes
conn.execute("SELECT COUNT(*) FROM df").df()
# %%
conn.execute("""SELECT * FROM df WHERE "Order ID"='295665'""").df()

# %%
# It is not ideal to create a virtual table all the time, it's better to create a table directly if there isn't much complexity
conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		Price::DECIMAL AS price_each,
		strptime("Order Date", '%m/%d/%Y %H:%M')::DATE as order_date,
		"Purchase Address" AS purchase_address
	FROM df
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")
# %%
