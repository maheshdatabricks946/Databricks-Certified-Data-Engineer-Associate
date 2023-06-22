# Databricks notebook source
# MAGIC %md
# MAGIC #### 2.4 - Higher Order Functions and SQL UDFs

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/customers-json/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers AS 
# MAGIC SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders AS 
# MAGIC SELECT * FROM parquet.`dbfs:/mnt/demo-datasets/bookstore/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books, FILTER (books, i-> i.quantity >= 2) AS multiple_copies
# MAGIC FROM orders; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, multiple_copies
# MAGIC FROM (SELECT order_id, 
# MAGIC FILTER (books, i -> i.quantity >= 2 ) AS multiple_copies 
# MAGIC FROM orders)
# MAGIC WHERE size(multiple_copies)>0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books,
# MAGIC TRANSFORM(books, b -> CAST(b.subtotal * 0.8 AS INT)) AS subtotal_after_discount 
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN concat("https://www.", split(email,"@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION get_url;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED get_url;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC           WHEN email LIKE "%.com" THEN "Commercial business"
# MAGIC           WHEN email LIKE "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email LIKE "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknown extension for domain:", split(email, "@")[1])
# MAGIC         END;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED site_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) as domain_category FROM customers;

# COMMAND ----------


