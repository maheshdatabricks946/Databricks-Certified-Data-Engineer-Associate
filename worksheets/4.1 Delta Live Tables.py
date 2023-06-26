# Databricks notebook source
### 4.1 Delta Live Tables

# COMMAND ----------

### Bronze Layer Tables

# COMMAND ----------

#### orders_raw

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets/

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
# MAGIC COMMENT "THE raw books orders, ingested from orders-raw"
# MAGIC AS SELECT * FROM cloud_files("${datasets_path}/orders-raw", "parquet", 
# MAGIC map("schema","order_id STRING, order_timestamp LONG, customer_id STRING, quantity LONG"))

# COMMAND ----------

#### customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC COMMENT "The customers lookup table, ingested from customers-json"
# MAGIC AS SELECT * FROM json.`${datasets_path}/customers-json`;

# COMMAND ----------

### SILVER LAYER TABLES

# COMMAND ----------

#### orders_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
# MAGIC   CONSTRAINT valid_order_number EXPECT (order_id is NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "The cleaned books orders with valid order_id"
# MAGIC AS 
# MAGIC SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books, 
# MAGIC c.profile:address:country as country
# MAGIC FROM STREAM(LIVE.orders_raw) o
# MAGIC LEFT JOIN LIVE.customers c
# MAGIC ON o.customer_id = c.customer_id

# COMMAND ----------

### GOLD TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
# MAGIC COMMENT "Daily number of books per customer in china"
# MAGIC AS
# MAGIC  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) AS order_date, sum(quantity) AS books_counts
# MAGIC  FROM LIVE.orders_cleaned
# MAGIC  WHERE country = "China"
# MAGIC  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.cn_daily_customer_books;

# COMMAND ----------


