# Databricks notebook source
# MAGIC %md
# MAGIC ### 3.3 - Multi-Hop Architecture

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() AS arrival_time, input_file_name() AS source_file
# MAGIC   FROM orders_raw_temp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp;

# COMMAND ----------

# MAGIC %md
# MAGIC #### BRONZE TABLE

# COMMAND ----------

(
    spark.table("orders_tmp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
    .outputMode("append")
    .table("orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_bronze;

# COMMAND ----------

## loading new data in the book-store
load_new_data()

# COMMAND ----------

(
    spark.read
    .format("json")
    .load(f"{dataset_bookstore}/customers-json")
    .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup;

# COMMAND ----------

(
    spark.readStream
    .table("orders_bronze")
    .createOrReplaceTempView("orders_bronze_tmp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SILVER LEVEL 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name AS f_name, c.profile:last_name AS l_name,
# MAGIC   cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books 
# MAGIC FROM orders_bronze_tmp o 
# MAGIC INNER JOIN customers_lookup c 
# MAGIC ON o.customer_id = c.customer_id
# MAGIC WHERE quantity > 0 
# MAGIC )

# COMMAND ----------

(
    spark.table("orders_enriched_tmp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
    .outputMode("append")
    .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_silver;

# COMMAND ----------

## load new data to book-store 
load_new_data()

# COMMAND ----------

(
    spark.readStream
    .table("orders_silver")
    .createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) AS order_date, sum(quantity) AS books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

(
    spark.table("daily_customer_books_tmp")
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
    .trigger(availableNow=True)
    .table("daily_customer_books")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books;

# COMMAND ----------

## load new data in data-store
load_new_data(all= True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### stop active streams 

# COMMAND ----------

for s in spark.streams.active:
    s_id = s.id
    print("stopping stream: ",s_id)
    s.stop()
    s.awaitTermination()
    print("stopped stream: ",s_id)

# COMMAND ----------


