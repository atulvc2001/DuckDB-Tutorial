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
    REPLACE("Price", ',', '')::DECIMAL AS price_each,
    "Order Date"::DATE AS order_date,
    "Purchase Address" AS purchase_address
FROM df
WHERE
    TRY_CAST("Order ID" AS INTEGER) IS NOT NULL;
""")
# %%
# Exclude data from select statement
conn.execute("""
	SELECT 
		* EXCLUDE (product, order_date, purchase_address)
	FROM sales
	""").df()
# %%
# The COlumns Expression
conn.execute("""
	SELECT 
        MIN(COLUMNS(* EXCLUDE(product, order_date, purchase_address)))
    FROM sales
""")
# %%
# Creating a view for aggregrating data
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		MONTH(order_date) as month,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price_each) AS revenue
	FROM sales
	GROUP BY ALL
""")
# %%
conn.execute("FROM aggregated_sales").df()
# %%
conn.execute("""
	SELECT 
        city,
        SUM(revenue) as total
    FROM aggregated_sales
    GROUP BY city
    ORDER BY total DESC
""").df()
# %%
# Parquet file
conn.execute("""
	COPY (FROM aggregated_sales) 
    TO 'aggregated_sales.parquet' (FORMAT 'parquet')
""")
# %%
# Reading Parquet file
conn.execute("FROM aggregated_sales.parquet").df()
# %%
