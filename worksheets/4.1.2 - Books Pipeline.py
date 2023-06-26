# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Laye Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
# MAGIC COMMENT "The raw books data, ingested from CDC feed"
# MAGIC AS SELECT * FROM cloud_files("${datasets_path}/books-cdc","json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE books_silver;
# MAGIC
# MAGIC APPLY CHANGES INTO LIVE.books_silver
# MAGIC FROM STREAM(LIVE.books_bronze)
# MAGIC KEYS (book_id)
# MAGIC APPLY AS DELETE WHEN row_status = "DELETE"
# MAGIC SEQUENCE BY row_time
# MAGIC COLUMNS * EXCEPT (row_status, row_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE author_counts_state
# MAGIC COMMENT "Number of books per author"
# MAGIC AS SELECT author, count(*) AS books_count, current_timestamp() AS updated_time
# MAGIC FROM LIVE.books_silver
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE VIEW books_sales
# MAGIC AS SELECT b.title, o.quantity
# MAGIC FROM (
# MAGIC   SELECT *, explode(books) AS book
# MAGIC   FROM LIVE.orders_cleaned) o
# MAGIC   INNER JOIN LIVE.books_silver b
# MAGIC ON o.book.book_id = b.book_id;

# COMMAND ----------


